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

//#define PATH_MAX 240
#define BUFF_SIZE 8 * 1024
#define GLOBAL_BUFF_SIZE 12 * 1024 * 1024

char *global_buff[GLOBAL_BUFF_SIZE];
uint64_t global_offset = 0;
int is_write = 0;
int is_read = 0;
uint64_t global_size = 0; //inform the writing file size

typedef struct thread_data
{
    int sd;
    uint64_t file_size; //this is size of one remote write; but full file size in read
    char *buff; //a huge buff in memory, useless
    int fd;
    int offset;
} THREAD_DATA;

void *p_scatter(void *);
void *p_get(void *);

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
    uint64_t mod = real_size % 3;
    if (mod == 1)
    {
        uint64_t tmp_size = real_size + 2;
        return tmp_size / 3;
    }
    else if (mod == 2)
    {
        uint64_t tmp_size = real_size + 1;
        return (real_size + 1) / 3;
    }
    else
        return real_size / 3;
}

/**
 * @brief  recover the read data
 * @note   only used in remote read
 * @param  sd: 
 * @param  fd: 
 * @param  file_size: 
 * @param  down_id: 
 * @param  filename: 
 * @retval 
 */
int recover(int sd, int fd, uint64_t file_size, int down_id, char *filename)
{
    log_msg("Start to recover\n");
    char *file_data = (uint8_t *)mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);

    MSG message;
    MSG response;
    message.flag = 'R';
    message.payload_length = PATH_MAX;
    uint64_t pos[3];
    pos[3] = 0;
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
    if (response.flag == 'N')
    {
        //useless
        log_msg("Fatal: Remote file doesn't exist: Server %d: %s \n", down_id, filename);
        return -1;
    }
    if (response.flag == 'r')
    {
        uint64_t left_size = recover_len;
        int packet_size = BUFF_SIZE;
        uint64_t offset = recover_offset;
        char *receive_buff;
        char *recover_buff;
        receive_buff = (char *)malloc(BUFF_SIZE * sizeof(char));
        recover_buff = (char *)malloc(BUFF_SIZE * sizeof(char));
        int i = 0;
        while (left_size > 0)
        {
            packet_size = left_size < BUFF_SIZE ? left_size : BUFF_SIZE;
            recvn(sd, receive_buff, packet_size);
            for (i = 0; i < packet_size; i++)
            {
                recover_buff[i] = file_data[pos[0]++] ^ file_data[pos[1]++] ^ file_data[pos[2]++];
            }
            pwrite(fd, recover_buff, packet_size, offset);
            offset += packet_size;
            left_size -= packet_size;
        }
        free(receive_buff);
        receive_buff = NULL;
        free(recover_buff);
        recover_buff = NULL;
    }
    munmap(file_data, file_size);
}

/**
 * @brief  calculate parity with XOR
 * @note   
 * @param  parity_buff: 
 * @retval 
 */
int get_parity(char *parity_buff)
{
    uint64_t common_split_len = get_split_size(data_length);
    uint64_t pos = 0;

    for (pos = 0; pos < common_split_len; pos++)
    {
        parity_buff[pos] = source_data[pos] ^ source_data[pos + common_split_len];
        parity_buff[pos] ^= pos + common_split_len + common_split_len > file_size ? 0 : source_data[pos + common_split_len + common_split_len];
    }
    log_msg("Create parity. size = %ld\n", common_split_len);
    return 0;
}

int init_write(char *filename)
{
    log_msg("Init write operation -> %s\n", filename);
    MSG message;
    message.flag = 'W';
    message.payload_length = PATH_MAX;
    int i = 0;
    for (i = 0; i < 4; i++)
    {
        int64_t ret = sendm(BB_DATA->SD[i], message, filename, PATH_MAX);
        MSG response;
        ret = recvm(BB_DATA->SD[i], response, NULL, 0);

        if (response.flag == 'w')
            continue;
        else if (response.flag == 'N')
        {
            //useless
            log_msg("Remote file cannot create: server %d: %s \n", i, filename);
            return -1;
        }
        else
        {
            log_msg("Impossible! Response flag is %c\n", response.flag);
            return -2;
        }
    }
    return 0;
}

/**
 * @brief  remote write operation
 * @note   
 * @param  filename: name of file
 * @retval 
 */
//TODO remember to reset offset finally;
int myfs_write(char *filename)
{
    //get source data
    int i = 0;

    char meta_path[PATH_MAX];
    get_meta_path(meta_path, filename);

    log_msg("\nSend %lu bytes to remote-> %s", global_offset, filename);
    int tmpret;
    if (global_size == 0)
    {
        tmpret = init_write(filename);
        if (tmpret < 0)
            return -1;
    }

    pthread_t pid[4];
    THREAD_DATA ths[4];
    global_size += global_offset;
    if (global_size > BB_DATA->threshold)
    {
        uint64_t common_split_len = get_split_size(global_offset);
        uint64_t data_length;
        log_msg(" <- large file mode\n");

        uint64_t offset = 0;
        char* parity_buff[common_split_len];
        get_parity(parity_buff);

        for (i = 0; i < 4; i++)
        {
            ths[i].sd = BB_DATA->SD[i];
            if (i == 2)
            {
                data_length = global_offset - common_split_len - common_split_len;
            }
            else
            {
                data_length = common_split_len;
            }
            ths[i].file_size = data_length;
            if (i == 3)
            {
                ths[i].buff = parity_buff;
                ths[i].offset = 0;
            }
            else
            {
                ths[i].buff = global_buff;
                ths[i].offset = offset;
            }
            pthread_create(&pid[i], NULL, p_scatter, (void *)&ths[i]);

            offset += data_length;
        }
    }
    else
    {
        //small file
        log_msg(" <- small file mode\n");
        message.file_length = global_offset;
        int sd;
        for (i = 0; i < 4; i++)
        {
            ths[i].sd = BB_DATA->SD[i];
            ths[i].file_size = global_offset;
            ths[i].buff = global_buff;
            pthread_create(&pid[i], NULL, p_scatter, (void *)&ths[i]);
        }
    }

    for (i = 0; i < 4; i++)
        pthread_join(pid[i], NULL);

    log_msg("Remote write complete -> %s\n", filename);
}

/**
 * @brief  remote read operation
 * @note   
 * @param  filename: name of file
 * @retval 
 */
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

    log_msg("\nStart to remote read: %s->local :%d bytes \n", local_path, real_size);

    fd = open(local_path, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXO);
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

    if (real_size > BB_DATA->threshold)
    {
        //large file
        log_msg("Start remote read (large file mode): -> %s\n", local_path);
        for (i = 0; i < 3; i++)
        {
            MSG response;
            ths[i].sd = BB_DATA->SD[i];
            int64_t ret = sendm(ths[i].sd, &message, filename, PATH_MAX);
            log_msg("Sending %ld bytes to server %d\n", ret, i);
            ret = recvm(BB_DATA->SD[i], &response, NULL, 0);
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
            }
            log_msg("Receiving %ld bytes from server %d\n", ret, i);
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
            recover(BB_DATA->SD[down_server_id], fd, real_size, down_server_id, filename);
        }
    }
    else
    {
        //small file
        log_msg("Start remote read (small file mode): -> %s\n", local_path);
        int64_t ret = -1;
        i = 0;
        int sd = 0;
        MSG response;
        while (ret < 0)
        {
            sd = BB_DATA->SD[i];
            ret = sendm(sd, &message, filename, PATH_MAX);
            log_msg("Sending %ld bytes to server %d\n", ret, i);
            ret = recvm(sd, &response, NULL, 0);

            if (ret < 0)
            {
                log_msg("Server %d is down\n", i);
                i++;
                continue;
            }
            log_msg("Receiving %ld bytes from server %d\n", ret, i);
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

        int packet_size = BUFF_SIZE;
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
    }
    close(fd);
    return 0;
}

/**
 * @brief  Flush rest data in global_buff, which always not full. And tell servers to close writing
 * @note   This is always used in remote write because remote read will work on file directly
 * @param  filename: the name of file
 * @retval 0: success; -1: fail
 */
int myfs_flush(char *filename)
{
    char *meta_size_buff;
    meta_size_buff = (char *)malloc(10 * sizeof(char));
    strcpy(meta_size_buff, ultostr(real_size, 10));

    fd = open(local_path, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXO);
    pwrite(fd, "\0", 1, 0);
    close(fd);

    fd = open(meta_path, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXO);
    pwrite(fd, meta_size_buff, 10, 0);
    close(fd);

    log_msg("Write metadata in :%s\n", meta_path);
    free(meta_size_buff);
    meta_size_buff = NULL;
    return 0;
}

/**
 * @brief  the thread of remote read
 * @note   
 * @param  arg: see struct thread_data
 * @retval None
 */
void *p_get(void *arg)
{
    THREAD_DATA *th = (THREAD_DATA *)arg;
    uint64_t left_size = th->file_size;
    int packet_size = BUFF_SIZE;
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

/**
 * @brief  the thread of remote write
 * @note   
 * @param  arg: see struct thread_data
 * @retval None
 */
void *p_scatter(void *arg)
{
    THREAD_DATA *th = (THREAD_DATA *)arg;

    uint64_t left_size = th->file_size;
    int packet_size = BUFF_SIZE;
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
