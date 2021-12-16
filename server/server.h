#include <string.h>
#include <stdio.h>
#include "trans.h"
#include <sys/mman.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
#include <dirent.h>

#define BUFF_SIZE 1 * 1024 * 1024

int server_save(int sd, int fd, uint64_t file_size)
{
    char *receive_buff;
    receive_buff = (char *)malloc(file_size * sizeof(char));
    recvn(sd, receive_buff, file_size);
    pwrite(fd, receive_buff, file_size, 0);

    return 0;
}

int server_read(int sd, int fd, uint64_t file_size)
{
    char *file_data = (uint8_t *)mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);

    sendn(sd, file_data, file_size);

    return 0;
}

int dir_read(int sd, char *dir_path)
{
    DIR *dir;
    struct dirent *di;

    if ((dir = opendir(dir_path)) == NULL)
    {
        printf(stderr, "%s cannot open\n", dir_path);
    }
    else
    {
        while ((di = readdir(dir)) != NULL)
        {
            if (strcmp(di->d_name, ".") == 0 || strcmp(di->d_name, "..") == 0)
            {
                continue;
            }
            MSG msg;
            msg.flag = 'l';
            msg.file_length = 1;
            msg.payload_length = strlen(di->d_name);
            sendm(sd, &msg, di->d_name, msg.payload_length);
        }
        closedir(dir);
    }
}
