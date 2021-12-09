#include "trans.h"
#include "log.h"
#include "params.h"
#include <unistd.h>
#include <pthread.h>
#include <math.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>

#define PATH_MAX 240
#define BUFF_SIZE 8*1024

int is_write = 0;

typedef struct thread_data {
    int sd;
    uint64_t file_size;
    char *buff; //a huge buff in memory
    int fd;
    int offset;
} THREAD_DATA;

void *p_scatter(void *);
void *p_get(void *);

static void bb_fullpath(char fpath[PATH_MAX], const char *path) {
    strcpy(fpath, BB_DATA->rootdir);
    strncat(fpath, path, PATH_MAX); // ridiculously long paths will
    log_msg("    bb_fullpath:  rootdir = \"%s\", path = \"%s\", fpath = \"%s\"\n",
            BB_DATA->rootdir, path, fpath);
}

void get_meta_path(char meta_path[PATH_MAX], char *filename) {
    strcpy(meta_path, BB_DATA->metadir);
    strncat(meta_path, filename, PATH_MAX);
}

uint64_t get_split_size(uint64_t real_size) {
    return ceil(real_size / 3);
}

int recover(int sd, int fd, uint64_t file_size, int down_id, char* filename) {
    msg_log("Start to recover\n");
    char *file_data = (uint8_t *) mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);

    MSG message;
    MSG response;
    message.flag = 'R';
    message.payload_length = PATH_MAX;
    uint64_t pos[3];
    pos[3]=0;
    uint64 common_split_len = get_split_size(file_size);
    uint64_t recover_len=0;
    switch (down_id) {
        case 0:
            recover_len = common_split_len;
            pos[0]=common_split_len;
            pos[1]=common_split_len+common_split_len;
            break;
        case 1:
            recover_len = common_split_len;
            pos[0]=0;
            pos[1]=common_split_len+common_split_len;
            break;
        case 2:
            recover_len = file_size-common_split_len-common_split_len;
            pos[0]=0;
            pos[1]=common_split_len;
            break;
        default:
            log_msg("Fatal: down server id is: %d\n", down_id);
    }

    uint64_len recover_offset = down_id*common_split_len;

    sendm(BB_DATA->SD[3], &message, filename, PATH_MAX);
    recvm(BB_DATA->SD[3], &response, NULL, 0);
    if (response.flag == 'N') {
        //useless
        log_msg("Fatal: Remote file doesn't exist: Server %d: %s \n", down_id, filename);
        return -1;
    }
    if (response.flag == 'r') {
        uint64_t left_size = recover_len;
        int packet_size = BUFF_SIZE;
        uint64_t offset = recover_offset;
        char *receive_buff;
        char *recover_buff;
        receive_buff = (char *) malloc(BUFF_SIZE * sizeof(char));
        recover_buff = (char *) malloc(BUFF_SIZE * sizeof(char));
        int i=0;
        while (left_size > 0) {
            packet_size = left_size < BUFF_SIZE ? left_size : BUFF_SIZE;
            recvn(sd, receive_buff, packet_size);
            for(i=0;i<packet_size;i++) {
                recover_buff[i] = file_data[pos[0]++] ^ file_data[pos[1]++] ^ file_data[pos[2]++];
            }
            pwrite(fd, recover_buff, packet_size, offset);
            offset += packet_size;
            left_size -= packet_size;
        }
        free(receive_buff);
        free(recover_buff);
    }
    munmap(file_data, file_size);
}

int get_parity(char *source_data, char *parity_buff, uint64_t file_size) {
    uint64_t common_split_len = get_split_size(file_size);
    uint64_t pos = 0;

    for(pos = 0;pos<common_split_len;pos++){
        parity_buff[pos]=source_data[pos]^source_data[pos+common_split_len];
        parity_buff[pos]^=pos+common_split_len+common_split_len>file_size?0:source_data[pos+common_split_len+common_split_len];
    }
    return 0;
}


int myfs_write(char *filename) {
    char *local_path[PATH_MAX];
    bb_fullpath(local_path, filename);
    int fd = open(local_path, O_RDONLY);
    if (fd < 0) {
        msg_log("File doesn't exist: %s\n", local_path);
        return -1;
    }

    //get source data
    uint64_t real_size = lseek(fd, 0, SEEK_END);
    posix_fadvise(fd, 0, real_size, POSIX_FADV_WILLNEED);
    char *file_data = (uint8_t *) mmap(0, real_size, PROT_READ, MAP_SHARED, fd,
                                       0);
    int i = 0;

    char meta_path[PATH_MAX];
    char meta_size_buff[9];
    get_meta_path(meta_path, filename);

    //write a meta file here

    MSG message;
    message.flag = 'W';
    message.payload_length = PATH_MAX;
    pthread_t pid[4];
    THREAD_DATA ths[4];

    if (real_size > BB_DATA->threshold) {
        //large file
        uint64_t common_split_len = get_split_size(real_size);
        char parity_buff[common_split_len];
        get_parity(file_data, parity_buff, real_size);
        log_msg("Start remote write (large file mode) -> %s\n", filename);
        uint64_t offset = 0;
        for (i = 0; i < 4; i++) {
            ths[i].sd=BB_DATA->SD[i];
            if (i == 2) {
                message.file_length = real_size - common_split_len - common_split_len;
            } else {
                message.file_length = common_split_len;
            }
            //test & build trans connection
            MSG response;
            int ret = sendm(ths[i].sd, &message, filename, PATH_MAX);
            if (ret < 0) {
                log_msg("Impossible! Server %d is down\n", i);
                continue;
            }

            recvm(BB_DATA->SD[i], &response, NULL, 0);
            if (response.flag == 'N') {
                //useless
                log_msg("Remote file cannot create: server %d: %s \n", i, filename);
                return -1;
            } else if (response.flag == 'w') {
                ths[i].file_size = message.file_length;
                ths[i].sd = BB_DATA->SD[i];
                if (i == 3) {
                    ths[i].buff = parity_buff;
                    ths[i].offset=0;
                }
                else {
                    ths[i].buff = file_data;
                    ths[i].offset = offset;
                }
                pthread_create(&pid[i], NULL, p_scatter, (void *) &ths[i]);
            } else {
                log_msg("Impossible! Response flag is %c\n", response.flag);
                return -2;
            }
            offset += message.file_length;
        }
    } else {
        //small file
        log_msg("Start remote write (small file mode) -> %s\n", filename);
        message.file_length = real_size;
        int sd;
        for (i = 0; i < 4; i++) {
            sd = BB_DATA->SD[i];
            MSG response;
            int ret = sendm(sd, &message, filename, PATH_MAX);
            if (ret < 0) {
                log_msg("Impossible! Server %d is down\n", i);
                return -2;
            }
            recvm(sd, &response, NULL, 0);
            ths[i].sd = sd;
            ths[i].file_size = real_size;
            ths[i].buff = file_data;
            pthread_create(&pid[i], NULL, p_scatter, (void *) &ths[i]);
        }
        log_msg("Remote write complete -> %s\n", filename);
    }
    for(i=0;i<4;i++)
        pthread_join(pid[i],NULL);
    munmap(file_data, real_size);
    close(fd);

    fd = open(meta_path, O_WRONLY | O_CREAT);
    char* temptr;
    meta_size_buff = ultostr(real_size, temptr, 10);
    write(fd, meta_size_buff, 9);
    close(fd);
    remove(local_path);
    return 0;
}

int myfs_read(char *filename) {
    char *local_path[PATH_MAX];
    bb_fullpath(local_path, filename);
    int fd = open(local_path, O_RDONLY);

    if (fd > 0) {
        log_msg("%s exists locally\n", local_path);
        return 0;   //file exists locally
    }

    //get metadata locally
    char meta_path[PATH_MAX];
    uint64_t real_size;
    char meta_size_buff[9];
    get_meta_path(meta_path, filename);
    fd = open(meta_path, O_RDONLY);
    if (fd > 0) {
        read(fd, meta_size_buff, 9);
        char *temptr;
        real_size = strtoul(meta_size_buff, temptr, 10);
    } else {
        log_msg("Remote file doesn't exist: %s \n", filename);
        return -1;
    }
    close(fd);

    fd = open(local_path, O_WRONLY | O_CREAT);
    if (fd < 0) {
        log_msg("Fatal: Open or Create failed: %s\n", local_path);
    }
    int i = 0;
    pthread_t pid[4];
    THREAD_DATA ths[4];
    uint64_t offset=0;    //offset in each thread

    MSG message;
    message.flag = 'R';
    message.payload_length = PATH_MAX;

    int down_server_id = -1;
    uint64_t common_split_len = get_split_size(real_size);

    if (real_size > BB_DATA->threshold) {
        //large file
        log_msg("Start remote read (large file mode): -> %s\n", local_path);
        for (i = 0; i < 3; i++) {
            MSG response;
            ths[i].sd = BB_DATA->SD[i];
            int ret = sendm(ths[i].sd, &message, filename, PATH_MAX);
            if (ret < 0) {
                down_server_id = i;
                if(down_server_id>=0)
                    log_msg("Fatal: More than one server is down\n");
                log_msg("Server %d is down\n", i);
                if(down_server_id==0||down_server_id==1){
                    offset+=common_split_len;
                } else if(down_server_id==2){
                    offset+=real_size-common_split_len-common_split_len;
                } else{
                    msg_log("Fatal: Logistic error\n");
                }
                continue;
            }
            recvm(BB_DATA->SD[i], &response, NULL, 0);
            if (response.flag == 'N') {
                //useless
                log_msg("Fatal: Remote file doesn't exist: Server %d: %s \n", i, filename);
                return -1;
            }
            if (response.flag == 'r') {
                ths[i].file_size = response.file_length;
                ths[i].fd=fd;
                ths[i].offset=offset;
                pthread_create(&pid[i], NULL, p_get, (void *) &ths[i]);
            } else {
                log_msg("Impossible! Response flag is %c\n", response.flag);
                return -2;
            }
            offset+=response.file_length;
        }

        if (down_server_id >= 0) {
            if(down_server_id==2){
                int recover_len=real_size-common_split_len-common_split_len;
            }
            else{
                int recover_len=common_split_len;
            }
            recover(BB_DATA->sd[down_server_id], fd, real_size, down_server_id, filename);
        }
        for(i=0;i<3;i++)
            pthread_join(pid[i],NULL);
    } else {
        //small file
        log_msg("Start remote read (small file mode): -> %s\n", local_path);
        int ret = -1;
        i = 0;
        int sd = 0;
        MSG response;
        while (ret < 0) {
            sd = BB_DATA->SD[i];
            ret = sendm(sd, &message, filename, PATH_MAX);
            if (ret < 0) {
                log_msg("Server %d is down\n", i);
                i++;
                continue;
            }
            recvm(sd, &response, NULL, 0);
        }
        if (response.flag == 'N') {
            //useless
            log_msg("Remote file doesn't exist: Server %d: %s \n", i, filename);
            return -1;
        }
        if (response.flag != 'r') {
            log_msg("Impossible! Response flag is %c\n", response.flag);
            return -2;
        }

        uint64_t left_size = real_size;

        //delete here
        if (response.file_length != real_size)
            log_msg("WRONG in file size!!!\n");

        int packet_size = BUFF_SIZE;
        uint64_t offset = 0;

        char *receive_buff;
        receive_buff = (char *) malloc(BUFF_SIZE * sizeof(char));

        while (left_size > 0) {
            packet_size = left_size < BUFF_SIZE ? left_size : BUFF_SIZE;
            recvn(sd, receive_buff, packet_size);
            pwrite(fd, receive_buff, packet_size, offset);
            offset += packet_size;
            left_size -= packet_size;
        }
        log_msg("Remote read complete -> %s\n", local_path);
        free(receive_buff);
    }
    close(fd);
    return 0;
}

void *p_get(void *arg) {
    THREAD_DATA *th = (*THREAD_DATA) arg;
    uint64_t left_size = th->file_size;
    int packet_size = BUFF_SIZE;
    uint64_t offset = th->offset;
    int sd=th->sd;
    int fd=th->fd;

    char *receive_buff;
    receive_buff = (char *) malloc(BUFF_SIZE * sizeof(char));

    while (left_size > 0) {
        packet_size = left_size < BUFF_SIZE ? left_size : BUFF_SIZE;
        recvn(sd, receive_buff, packet_size);
        pwrite(fd, receive_buff, packet_size, offset);
        offset += packet_size;
        left_size -= packet_size;
    }
    free(receive_buff);
    pthread_exit(NULL);
}

void *p_scatter(void *arg) {
    THREAD_DATA *th = (*THREAD_DATA) arg;

    uint64_t left_size = th->file_size;
    int packet_size = BUFF_SIZE;
    uint64_t offset = th->offset;

    while (left_size > 0) {
        packet_size = left_size < BUFF_SIZE ? left_size : BUFF_SIZE;
        sendn(th->sd, th->buff + offset, packet_size);
        offset += packet_size;
        left_size -= packet_size;
    }

    pthread_exit(NULL);
}
