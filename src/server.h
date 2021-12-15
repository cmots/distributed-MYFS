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
    recvn(sd, receive_buff, packet_size);
    pwrite(fd, receive_buff, packet_size, offset);

    // char *receive_buff;
    // receive_buff = (char *)malloc(BUFF_SIZE * sizeof(char));
    // uint64_t left_size = file_size;
    // int packet_size = BUFF_SIZE;
    // uint64_t offset = 0;

    // while (left_size > 0)
    // {
    //     packet_size = left_size < BUFF_SIZE ? left_size : BUFF_SIZE;
    //     recvn(sd, receive_buff, packet_size);
    //     pwrite(fd, receive_buff, packet_size, offset);
    //     offset += packet_size;
    //     left_size -= packet_size;
    // }
    // free(receive_buff);
    return 0;
}

int server_read(int sd, int fd, uint64_t file_size)
{
    // uint64_t left_size = file_size;
    // int packet_size = BUFF_SIZE;
    // uint64_t offset = 0;

    char *file_data = (uint8_t *)mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);

    sendn(sd, file_data, file_size);

    // while (left_size > 0)
    // {
    //     packet_size = left_size < BUFF_SIZE ? left_size : BUFF_SIZE;
    //     sendn(sd, file_data + offset, packet_size);
    //     offset += packet_size;
    //     left_size -= packet_size;
    // }
    // munmap(file_data, file_size);
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
