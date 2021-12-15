#include "trans.h"
#include "log.h"
#include "params.h"

#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <fuse.h>
#include <sys/statvfs.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <limits.h>
#include <sys/types.h>
#include <dirent.h>
#include <time.h>

//#define PATH_MAX 240
#define BUFF_SIZE 2 * 1024 * 1024

int is_write = 0;
int is_read = 0;

typedef struct thread_data
{
    int sd;
    uint64_t file_size;
    char *buff; //a huge buff in memory
    int fd;
    int offset;
} THREAD_DATA;

void *p_scatter(void *);
void *p_get(void *);
int myfs_ls(char *);

//util function, tranform uint64_t to uint8_t
char *ultostr(unsigned long num, unsigned base)
{
    static char string[64] = {'\0'};
    size_t max_chars = 64;
    char remainder;
    int sign = 0;
    if (base < 2 || base > 36)
    {
        return NULL;
    }
    for (max_chars--; max_chars > sign && num != 0; max_chars--)
    {
        remainder = (char)(num % base);
        if (remainder <= 9)
        {
            string[max_chars] = remainder + '0';
        }
        else
        {
            string[max_chars] = remainder - 10 + 'A';
        }
        num /= base;
    }
    if (max_chars > 0)
    {
        memset(string, '\0', max_chars + 1);
    }
    return string + max_chars + 1;
}

static void bb_fullpath(char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, BB_DATA->rootdir);
    strncat(fpath, path, PATH_MAX); // ridiculously long paths will
    log_msg("    bb_fullpath:  rootdir = \"%s\", path = \"%s\", fpath = \"%s\"\n",
            BB_DATA->rootdir, path, fpath);
}

void get_meta_path(char *meta_path, char *filename)
{
    strcpy(meta_path, BB_DATA->metadir);
    strncat(meta_path, filename, PATH_MAX);
}

uint64_t get_split_size(uint64_t real_size)
{
    //	return (uint64_t)ceil(real_size/3);
    int mod = real_size % 3;
    if (mod == 1)
    {
        uint64_t tmp_size = real_size + 2;
        return tmp_size / 3;
    }
    else if (mod == 2)
    {
        uint64_t tmp_size = real_size + 1;
        return tmp_size / 3;
    }
    else
        return real_size / 3;
}

int recover(int sd, uint64_t file_size, int down_id, char *filename)
{

    char *local_path[PATH_MAX];
    bb_fullpath(local_path, filename);
	int fd = open(local_path, O_RDWR, S_IRWXU | S_IRWXO);
	if(fd<0){
		 log_msg("File doesn't exist: %s \n", filename);
	}
    log_msg("Start to recover\n");
    char *file_data = (uint8_t *)mmap(0, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    MSG message;
    MSG response;
    message.flag = 'R';
    message.payload_length = PATH_MAX;
    uint64_t pos[2];
    uint64_t common_split_len = get_split_size(file_size);
    uint64_t recover_len = 0;
    switch (down_id)
    {
    case 0:
        recover_len = common_split_len;
        pos[0] = common_split_len;
        pos[1] = common_split_len + common_split_len;
        break;
    case 1:
        recover_len = common_split_len;
        pos[0] = 0;
        pos[1] = common_split_len + common_split_len;
        break;
    case 2:
        recover_len = file_size - common_split_len - common_split_len;
        pos[0] = 0;
        pos[1] = common_split_len;
        break;
    default:
        log_msg("Fatal: down server id is: %d\n", down_id);
    }

    uint64_t recover_offset = down_id * common_split_len;

    sendm(BB_DATA->SD[3], &message, filename, PATH_MAX);
    recvm(BB_DATA->SD[3], &response, NULL, 0);
	
	log_msg("pos[0] is :%lu \npos[1] is :%lu \nrecover offset :%lu\n", pos[0], pos[1], recover_offset);
    if (response.flag == 'N')
    {
        //useless
        log_msg("Fatal: Remote file doesn't exist: Server %d: %s \n", down_id, filename);
        return -1;
    }
    if (response.flag == 'r')
    {
        uint64_t left_size = recover_len;
        uint64_t packet_size = BUFF_SIZE;
        uint64_t offset = recover_offset;
        char *receive_buff;
        char *recover_buff;
        receive_buff = (char *)malloc(BUFF_SIZE * sizeof(char));
        recover_buff = (char *)malloc(BUFF_SIZE * sizeof(char));
        uint64_t i = 0;
        while (left_size > 0)
        {
            packet_size = left_size < BUFF_SIZE ? left_size : BUFF_SIZE;
			log_msg("packet size is %lu\n", packet_size);

            recvn(sd, receive_buff, packet_size);
            for (i = 0; i < packet_size; i++)
            {
				if(pos[1]>=file_size){
					log_msg("fuck!!!%lu\n", pos[1]);
					recover_buff[i] = file_data[pos[0]] ^ 0 ;
					recover_buff[i]^= receive_buff[i];
				}
				else{
					recover_buff[i] = file_data[pos[0]] ^ file_data[pos[1]];
					recover_buff[i]^= receive_buff[i];
				}
				pos[0]+=1;
				pos[1]+=1;
			}
            //pwrite(fd, recover_buff, packet_size, offset);
			memcpy(file_data+offset, recover_buff, packet_size);
            offset += packet_size;
            left_size -= packet_size;
        }

		log_msg("pos[1] is %lu\n", pos[1]);
		log_msg("file_size is %lu\n", file_size);
        log_msg("done: %lu\n",offset-recover_offset);
		free(receive_buff);
        receive_buff = NULL;
        free(recover_buff);
        recover_buff = NULL;
    }
    munmap(file_data, file_size);
	log_msg("Recover complete!\n");
	close(fd);
}

int get_parity(char *source_data, char *parity_buff, uint64_t file_size)
{
    uint64_t common_split_len = get_split_size(file_size);
    uint64_t pos = 0;
    for (pos = 0; pos < common_split_len; pos++)
    {
        parity_buff[pos] = source_data[pos] ^ source_data[pos + common_split_len];
        parity_buff[pos] ^= pos + common_split_len + common_split_len >= file_size ? 0 : source_data[pos + common_split_len + common_split_len];
    }
    log_msg("Create parity. size = %lu\n", common_split_len);
    return 0;
}

int myfs_write(char *filename)
{
    char *local_path[PATH_MAX];
    bb_fullpath(local_path, filename);

    int fd = open(local_path, O_RDONLY);
    if (fd < 0)
    {
        log_msg("File doesn't exist: %s\n", local_path);
        return -1;
    }

    //get source data
    uint64_t real_size = lseek(fd, 0, SEEK_END);
    posix_fadvise(fd, 0, real_size, POSIX_FADV_WILLNEED);
    char *file_data = (uint8_t *)mmap(0, real_size, PROT_READ, MAP_SHARED, fd,
                                      0);
    int i = 0;

    char meta_path[PATH_MAX];
    get_meta_path(meta_path, filename);
    char *parity_buff;
    log_msg("\nStart to remote write: local %lu bytes-> %s", real_size, local_path);
	clock_t start_t;
    
	start_t = clock();

    MSG message;
    message.flag = 'W';
    message.payload_length = PATH_MAX;
    pthread_t pid[4];
    THREAD_DATA ths[4];

    int ret = 0;

    if (real_size > BB_DATA->threshold)
    {
        uint64_t common_split_len = get_split_size(real_size);

        log_msg(" <- large file mode\n");
        uint64_t offset = 0;
        parity_buff = (char *)malloc(sizeof(char) * common_split_len);
        memset(parity_buff, '\0', common_split_len);

        get_parity(file_data, parity_buff, real_size);

        for (i = 0; i < 4; i++)
        {
            ths[i].sd = BB_DATA->SD[i];
            if (i == 2)
            {
                message.file_length = real_size - common_split_len - common_split_len;
            }
            else
            {
                message.file_length = common_split_len;
            }
            //test & build trans connection
            MSG response;
            sendm(ths[i].sd, &message, filename, PATH_MAX);

            ret = recvm(BB_DATA->SD[i], &response, NULL, 0);
            if (ret < 0)
            {
                log_msg("Server %d is down\n", i);
                return -2;
                continue;
            }
            if (response.flag == 'N')
            {
                //useless
                log_msg("Remote file cannot create: server %d: %s \n", i, filename);
                return -2;
            }
            else if (response.flag == 'w')
            {
                ths[i].file_size = message.file_length;
                ths[i].sd = BB_DATA->SD[i];
                if (i == 3)
                {
                    ths[i].buff = parity_buff;
                    ths[i].offset = 0;
                }
                else
                {
                    ths[i].buff = file_data;
                    ths[i].offset = offset;
                }
                pthread_create(&pid[i], NULL, p_scatter, (void *)&ths[i]);
                if (i == 3)
                    log_msg("Server %d records %lu bytes parity data\n", i, message.file_length);
				else
					log_msg("Server %d records %lu bytes file data\n", i, message.file_length);
            }
            else
            {
                log_msg("Impossible! Response flag is %c\n", response.flag);
                return -2;
            }
            offset += message.file_length;
        }
    }
    else
    {
        //small file
        log_msg(" <- small file mode\n");
        message.file_length = real_size;
        int sd;
        for (i = 0; i < 4; i++)
        {
            sd = BB_DATA->SD[i];
            MSG response;
            sendm(sd, &message, filename, PATH_MAX);
            ret = recvm(sd, &response, NULL, 0);
            if (ret < 0)
            {
                log_msg("Impossible! Server %d is down\n", i);
                return -2;
            }
            ths[i].sd = sd;
            ths[i].file_size = real_size;
            ths[i].buff = file_data;
            pthread_create(&pid[i], NULL, p_scatter, (void *)&ths[i]);
        }
    }

    for (i = 0; i < 4; i++)
        pthread_join(pid[i], NULL);
    if (real_size > BB_DATA->threshold)
    {
        free(parity_buff);
        parity_buff = NULL;
    }
	clock_t end_t;
	end_t = clock();
	double diff_time = (double)(end_t-start_t) / CLOCKS_PER_SEC;
    log_msg("Remote write complete. Using %f seconds\n", diff_time);
    munmap(file_data, real_size);
    close(fd);

	start_t = clock();
    log_syscall("remove local file and create a fake file\n", remove(local_path), 0);

    char *meta_size_buff;
    meta_size_buff = (char *)malloc(10 * sizeof(char));
    strcpy(meta_size_buff, ultostr(real_size, 10));

    fd = open(local_path, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXO);
    pwrite(fd, "\0", 1, 0);
    close(fd);

    fd = open(meta_path, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXO);
    pwrite(fd, meta_size_buff, 10, 0);
    close(fd);
	
	end_t = clock();
	diff_time = (double)(end_t-start_t) / CLOCKS_PER_SEC;
    log_msg("Using %f seconds. Write metadata in :%s\n", diff_time, meta_path);
    free(meta_size_buff);
    meta_size_buff = NULL;
	start_t = clock();
    myfs_ls(BB_DATA->metadir);
	end_t = clock();
	diff_time = (double)(end_t-start_t) / CLOCKS_PER_SEC;
	log_msg("List using %f seconds\n", diff_time);
    return 0;
}

int myfs_read(char *filename)
{
    char *local_path[PATH_MAX];
    bb_fullpath(local_path, filename);

    //get metadata locally
    char meta_path[PATH_MAX];
    uint64_t real_size;
    char meta_size_buff[10];
    get_meta_path(meta_path, filename);
    int fd = open(meta_path, O_RDONLY);
    if (fd > 0)
    {
        read(fd, meta_size_buff, 10);
        char *temptr;
        real_size = strtoul(meta_size_buff, temptr, 10);
    }
    else
    {
        log_msg("Remote file doesn't exist: %s \n", filename);
        return -1;
    }
    close(fd);

    log_msg("\nStart to remote read: %s->local :%lu bytes \n", local_path, real_size);

    fd = open(local_path, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXO);
    if (fd < 0)
    {
        log_msg("Fatal: Open or Create failed: %s\n", local_path);
    }
    int i = 0;
    pthread_t pid[4];
    THREAD_DATA ths[4];
    uint64_t offset = 0; //offset in each thread

    MSG message;
    message.flag = 'R';
    message.payload_length = PATH_MAX;

    int down_server_id = -1;
    uint64_t common_split_len = get_split_size(real_size);

    int ret = 0;
    if (real_size > BB_DATA->threshold)
    {
        //large file
        log_msg(" <- large file mode\n");
        for (i = 0; i < 3; i++)
        {
            MSG response;
            ths[i].sd = BB_DATA->SD[i];
            ret = sendm(ths[i].sd, &message, filename, PATH_MAX);
            /*log_msg("ret = %d in send to server %d\n", ret, i);
            if (ret < 0)
            {
                down_server_id = i;
                if (down_server_id >= 0)
                    log_msg("Fatal: More than one server is down\n");
                log_msg("Server %d is down\n", i);
                if (down_server_id == 0 || down_server_id == 1)
                {
                    offset += common_split_len;
                }
                else if (down_server_id == 2)
                {
                    offset += real_size - common_split_len - common_split_len;
                }
                else
                {
                    log_msg("Fatal: Logistic error\n");
                }
                continue;
            }*/
            ret = recvm(BB_DATA->SD[i], &response, NULL, 0);
            log_msg("ret = %d in receive from server %d\n", ret, i);
            if (ret < 0)
            {
                if (down_server_id >= 0)
                    log_msg("Fatal: More than one server is down\n");
                down_server_id = i;
                log_msg("Server %d is down\n", i);
                if (down_server_id == 0 || down_server_id == 1)
                {
                    offset += common_split_len;
                }
                else if (down_server_id == 2)
                {
                    offset += real_size - common_split_len - common_split_len;
                }
                else
                {
                    log_msg("Fatal: Logistic error\n");
                }
                continue;
            }
            if (response.flag == 'N')
            {
                //useless
                log_msg("Fatal: Remote file doesn't exist: Server %d: %s \n", i, filename);
                return -1;
            }
            if (response.flag == 'r')
            {
                ths[i].file_size = response.file_length;
                ths[i].fd = fd;
                ths[i].offset = offset;
                pthread_create(&pid[i], NULL, p_get, (void *)&ths[i]);
            }
            else
            {
                log_msg("Impossible! Server %d responses flag is %c\n", i, response.flag);
                return -2;
            }
            offset += response.file_length;
        }

        for (i = 0; i < 3; i++)
        {
            if (i == down_server_id)
                continue;
            pthread_join(pid[i], NULL);
        }
        if (down_server_id >= 0)
        {
            if (down_server_id == 2)
            {
                int recover_len = real_size - common_split_len - common_split_len;
            }
            else
            {
                int recover_len = common_split_len;
            }
			close(fd);
            recover(BB_DATA->SD[down_server_id], real_size, down_server_id, filename);
        }
		else
			close(fd);
    }
    else
    {
        //small file
        log_msg("Start remote read (small file mode): -> %s\n", local_path);
        int ret = -1;
        i = 0;
        int sd = 0;
        MSG response;
        while (ret < 0)
        {
            sd = BB_DATA->SD[i];
            ret = sendm(sd, &message, filename, PATH_MAX);
            /*log_msg("ret = %d in send to server %d\n", ret, i);
            if (ret < 0)
            {
                log_msg("Server %d is down\n", i);
                i++;
                continue;
            }*/
            ret = recvm(sd, &response, NULL, 0);
            log_msg("ret = %d in receive from server %d\n", ret, i);

            if (ret < 0)
            {
                log_msg("Server %d is down\n", i);
                i++;
                continue;
            }
        }
        if (response.flag == 'N')
        {
            //useless
            log_msg("Remote file doesn't exist: Server %d: %s \n", i, filename);
            return -1;
        }
        if (response.flag != 'r')
        {
            log_msg("Impossible! Response flag is %c\n", response.flag);
            return -2;
        }

        uint64_t left_size = real_size;

        //delete here
        if (response.file_length != real_size)
            log_msg("WRONG in file size!!!\n");

        uint64_t packet_size = BUFF_SIZE;
        uint64_t offset = 0;

        char *receive_buff;
        receive_buff = (char *)malloc(BUFF_SIZE * sizeof(char));

        while (left_size > 0)
        {
            packet_size = left_size < BUFF_SIZE ? left_size : BUFF_SIZE;
            recvn(sd, receive_buff, packet_size);
            pwrite(fd, receive_buff, packet_size, offset);
            offset += packet_size;
            left_size -= packet_size;
        }
        log_msg("Remote read complete -> %s\n", local_path);
        free(receive_buff);
        receive_buff = NULL;
		close(fd);
    }
    return 0;
}

int myfs_ls(char *dir_path)
{
    DIR *dir;
    struct dirent *di;
    uint64_t file_size;
    int fd;
    char meta_path[PATH_MAX];
	char meta_size_buff[10];
	
	log_msg("===============list all files in MYFS=============\n");

    if ((dir = opendir(dir_path)) == NULL)
    {
        log_msg("%s cannot open\n", dir_path);
    }
    else
    {
        while ((di = readdir(dir)) != NULL)
        {
            if (strcmp(di->d_name, ".") == 0 || strcmp(di->d_name, "..") == 0)
            {
                continue;
            }
			memset(meta_path, '\0', PATH_MAX);
			memset(meta_size_buff, '\0', 10);
            strcpy(meta_path, dir_path);
			strcat(meta_path, "/");
			strcat(meta_path, di->d_name);
            fd = open(meta_path, O_RDONLY);
            if (fd > 0)
            {
                read(fd, meta_size_buff, 10);
                char *temptr;
                file_size = strtoul(meta_size_buff, temptr, 10);
            }
            close(fd);
            log_msg("%s\t\t\t%lubytes\n", di->d_name, file_size);
		
	//		log_msg("%s\n", di->d_name);
		}
        closedir(dir);
    }
	log_msg("===================================================\n");
}

void *p_get(void *arg)
{
    THREAD_DATA *th = (THREAD_DATA *)arg;
    uint64_t left_size = th->file_size;
    uint64_t packet_size = BUFF_SIZE;
    uint64_t offset = th->offset;
    int sd = th->sd;
    int fd = th->fd;

    char *receive_buff;
    receive_buff = (char *)malloc(BUFF_SIZE * sizeof(char));

    while (left_size > 0)
    {
        packet_size = left_size < BUFF_SIZE ? left_size : BUFF_SIZE;
        recvn(sd, receive_buff, packet_size);
        pwrite(fd, receive_buff, packet_size, offset);
        offset += packet_size;
        left_size -= packet_size;
    }
    free(receive_buff);
    receive_buff = NULL;
    pthread_exit(NULL);
}

void *p_scatter(void *arg)
{
    THREAD_DATA *th = (THREAD_DATA *)arg;

    uint64_t left_size = th->file_size;
    uint64_t packet_size = BUFF_SIZE;
    uint64_t offset = th->offset;

    while (left_size > 0)
    {
        packet_size = left_size < BUFF_SIZE ? left_size : BUFF_SIZE;
        sendn(th->sd, th->buff + offset, packet_size);
        offset += packet_size;
        left_size -= packet_size;
    }

    pthread_exit(NULL);
}
