#include "trans.h"
#include "log.h"
#include "param.h"
#include <unistd.h>
#include <pthread.h>
#include <math.h>
#include <string.h>
#define PATH_MAX 240
#define BUFF_SIZE 8*1024

bool is_write = false;

typedef struct thread_data{
    int sd;
    int result;
    char filename[PATH_MAX];
    uint64_t file_size;
    int id;
} THREAD_DATA;

static void bb_fullpath(char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, BB_DATA->rootdir);
    strncat(fpath, path, PATH_MAX); // ridiculously long paths will
    log_msg("    bb_fullpath:  rootdir = \"%s\", path = \"%s\", fpath = \"%s\"\n",
            BB_DATA->rootdir, path, fpath);
}

void get_meta_path(char meta_path[PATH_MAX], char *filename){
    strcpy(meta_path, BB_DATA->metadir);
    strncat(meta_path, filename, PATH_MAX);
}

uint64_t get_split_size(uint64_t real_size){
    return ceil(real_size/3);
}

int split(int fd, char *meta_path){
    msg_log("Start to split file locally\n");
    char* file_data = (uint8_t*)mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);

}

int combine(){


    msg_log("Finish combination and remote read complete\n");
}

int recover(){
    msg_log("Start to recover\n");
}

int myfs_write(char *filename){
    char * local_path[PATH_MAX];
    bb_fullpath(local_path, path);
    int fd = open(local_path, O_RDONLY);
    if(fd < 0){
        msg_log("File doesn't exist: %s\n",local_path); 
        return -1;
    }
    uint64_t real_size = lseek(fd, 0, SEEK_END);
    posix_fadvise(fd, 0, real_size, POSIX_FADV_WILLNEED);

    int i=0;

    char meta_path[PATH_MAX];
    char meta_size_buff[9];
    get_meta_path(meta_path, filename);
    
    //write a meta file here

    MSG message;
    message.flag='W';
    message.payload_length=PATH_MAX;
    if(real_size > BB_DATA->threshold){
        //large file
        split();
        message.file_length=get_split_size(real_size);
        pthread_t pid[4];
        THREAD_DATA ths[4];
        log_msg("Start remote write (large file mode) -> %s\n", filename);
        for(i=0;i<4;i++){
            MSG response;
            int ret = sendn(ths[i].sd, &message, filename, PATH_MAX);
            if(ret<0){
                log_msg("Impossible! Server %d is down\n",i);
                continue;
            }
            recvn(BB_DATA->SD[i], &response, NULL, 0);
            if(response.flag=='N'){
                //useless
                log_msg("Remote file cannot create: server %d: %s \n", i, filename);
                return -1;
            }
            else if(response.flag == 'w'){
                strcpy(ths[i].filename, meta_path);
                ths[i].file_size = message.file_length;
                ths[i].id = i;
                ths[i].sd = BB_DATA->SD[i];
                ths[i].result = 0;
                pthread_create(&pid[i], NULL, p_client_read, (void*)&ths[i]);
            }
            else{
                log_msg("Impossible! Response flag is %c\n", response.flag);
                return -2;
            }
    }
    else{
        //small file
        log_msg("Start remote write (small file mode) -> %s\n", filename);
        message.file_length=real_size;

        char* file_data = (uint8_t*)mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);
        for(i=0;i<4;i++)
        {
            sd=BB_DATA->SD[i];
            MSG response;
            int ret = sendn(sd, &message, filename, PATH_MAX);
            if(ret<0){
                log_msg("Impossible! Server %d is down\n", i);
                return -2;
            }
            recvn(sd, &response, NULL, 0);
        
            uint64_t left_size = real_size;

            int packet_size = BUFF_SIZE;
            uint64_t offset = 0;

            while(left_size>0){
                packet_size = left_size < BUFF_SIZE ? left_size: BUFF_SIZE;
                sendn(sd,file_data+offset, packet_size);
                pwrite(fd, receive_buff, packet_size, offset);
                offset+=packet_size;
                left_size-=packet_size;
            }
            log_msg("Remote read complete -> %s\n", local_path);
        }
    }
    close(fd);
    return 0;
}


int myfs_read(char *filename){
    char * local_path[PATH_MAX];
    bb_fullpath(local_path, path);
    int fd = open(local_path, O_RDONLY);
    int down_server_id = -1;

    if(fd > 0)
    {
        log_msg("%s exists locally\n", local_path);
        return 0;   //file exists locally
    }
    int i=0;
    pthread_t pid[4];
    THREAD_DATA ths[4];

    char meta_path[PATH_MAX];
    uint64_t real_size;
    char meta_size_buff[9];
    get_meta_path(meta_path, filename);
    fd = open(meta_path, O_RDONLY);
    if(fd > 0){
        pread(fd, meta_size_buff, 9);
        char* temptr;
        real_size = strtoul(meta_size_buff, temptr, 10);
    }
    else
    {
        log_msg("Remote file doesn't exist: %s \n", filename);
        return -1;
    }
    close(fd);


    MSG message;
    message.flag='R';
    message.payload_length=PATH_MAX;

    if(real_size > BB_DATA->threshold){
        //large file
        log_msg("Start remote read (large file mode): -> %s\n", local_path);
        for(i=0;i<4;i++){
            MSG response;
            int ret = sendn(ths[i].sd, &message, filename, PATH_MAX);
            if(ret<0){
                down_server_id=i;
                log_msg("Server %d is down\n",i);
                continue;
            }
            recvn(BB_DATA->SD[i], &response, NULL, 0);
            if(response.flag=='N'){
                //useless
                log_msg("Remote file doesn't exist: Server %d: %s \n", i, filename);
                return -1;
            }
            if(response.flag == 'r'){
                strcpy(ths[i].filename, meta_path);
                ths[i].file_size = response.file_length;
                ths[i].id = i;
                ths[i].sd = BB_DATA->SD[i];
                ths[i].result = 0;
                pthread_create(&pid[i], NULL, p_client_save, (void*)&ths[i]);
            }
            else{
                log_msg("Impossible! Response flag is %c\n", response.flag);
                return -2;
            }
        }
        if(down_server_id>=0){
            recover();
        }
        combine();
    }
    else{
        //small file
        log_msg("Start remote read (small file mode): -> %s\n", local_path);
        int ret = -1;
        i = 0;
        int sd = 0;
        MSG response;
        while(ret<0)
        {
            sd=BB_DATA->SD[i];
            ret = sendn(sd, &message, filename, PATH_MAX);
            if(ret<0){
                log_msg("Server %d is down\n", i);
                i++;
                continue;
            }
            recvn(sd, &response, NULL, 0);
        }
        if(response.flag=='N'){
            //useless
            log_msg("Remote file doesn't exist: Server %d: %s \n", i, filename);
            return -1;
        }
        if(response.flag!='r'){
            log_msg("Impossible! Response flag is %c\n", response.flag);
            return -2;
        }

        uint64_t left_size = real_size;

        //delete here
        if(response.file_length!=real_size)
            log_msg("WRONG in file size!!!\n");

        int packet_size = BUFF_SIZE;
        uint64_t offset = 0;

        char* receive_buff;
        receive_buff=(char*)malloc(BUFF_SIZE*sizeof(char));

        int fd = open(local_path, O_WRONLY | O_CREAT);
        if(fd < 0){
            perror("Open or Create failed":);
        }
        while(left_size>0){
            packet_size = left_size < BUFF_SIZE ? left_size: BUFF_SIZE;
            recvn(sd, receive_buff, packet_size);
            pwrite(fd, receive_buff, packet_size, offset);
            offset+=packet_size;
            left_size-=packet_size;
        }
        log_msg("Remote read complete -> %s\n", local_path);
        close(fd);
        free(receive_buff);
    }
    return 0;
}

void* p_client_split(void *arg){
    THREAD_DATA *th = (*THREAD_DATA) arg;
    int fd = th->sd;
    char path[PATH_MAX];
    char id = th->id + 48;
    char suffix[3] = {'-',id,'\0'};
    strcpy(path, th->filename);
    strncat(path, suffix, PATH_MAX);

    char* file_data = (uint8_t*)mmap(0, th->file_size, PROT_READ, MAP_SHARED, fd, 
            (th->id)*(th->file_size));

    int split_fd = open(path, O_WRONLY | O_CREAT | O_TRUNC);
    if(split_fd < 0){
        msg_log("Open or create local temp splited file failed: %s\n", path);
    }
    
    pwrite(split_fd, file_data, th->file_size, 0);
    munmap(file_data);
    close(split_fd);
    pthread_exit(NULL);
}

void* p_client_save(void *arg){
    THREAD_DATA *th = (*THREAD_DATA) arg;
    uint64_t left_size = th->file_size;
    int packet_size = BUFF_SIZE;
    uint64_t offset = 0;
    
    char* receive_buff;
    receive_buff=(char*)malloc(BUFF_SIZE*sizeof(char));
    char path[PATH_MAX];
    char id = th->id + 48;
    char suffix[3] = {'-',id,'\0'};
    strcpy(path, th->filename;
    strncat(path, suffix, PATH_MAX);

    int fd = open(path, O_WRONLY | O_CREAT);
    if(fd < 0){
        msg_log("Open or create local temp splited file failed: %s\n", path);
    }
    while(left_size>0){
        packet_size = left_size < BUFF_SIZE ? left_size: BUFF_SIZE;
        recvn(sd, receive_buff, packet_size);
        pwrite(fd, receive_buff, packet_size, offset);
        offset+=packet_size;
        left_size-=packet_size;
    }
    close(fd);
    free(receive_buff);
    pthread_exit(NULL);
}

void* p_client_read(void *arg){
    THREAD_DATA *th = (*THREAD_DATA) arg;
    uint64_t left_size = th->file_size;
    int packet_size = BUFF_SIZE;
    uint64_t offset = 0;

    char path[PATH_MAX];
    char id = th->id + 48;
    char suffix[3] = {'-',id,'\0'};
    strcpy(path, th->filename);
    strncat(path, suffix, PATH_MAX);

    int fd = open(path, O_RDONLY);
    if(fd < 0){
        msg_log("Open splited file failed:%s\n",path);
    }
    char* file_data = (uint8_t*)mmap(0, th->file_size, PROT_READ, MAP_SHARED, fd, 0);
    while(left_size>0){
        packet_size = left_size < BUFF_SIZE ? left_size: BUFF_SIZE;
        sendn(sd, file_data+offset, packet_size);
        pwrite(fd, receive_buff, packet_size, offset);
        offset+=packet_size;
        left_size-=packet_size;
    }
    close(fd);
    munmap(file_data);
    pthread_exit(NULL);
}
