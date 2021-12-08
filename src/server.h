#include <string.h>
#include <stdio.h>
#include "trans.h"
#include <sys/mman.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#define BUFF_SIZE 8*1024

int server_save(int sd, int fd, uint64_t file_size){
    char* receive_buff;
    receive_buff=(char*)malloc(BUFF_SIZE*sizeof(char));
    uint64_t left_size = file_size;
    int packet_size = BUFF_SIZE;
    uint64_t offset = 0;
    
    while(left_size>0){
        packet_size = left_size < BUFF_SIZE ? left_size: BUFF_SIZE;
        recvn(sd, receive_buff, packet_size);
        pwrite(fd, receive_buff, packet_size, offset);
        offset+=packet_size;
        left_size-=packet_size;
    }
    free(receive_buff);
    return 0;
}

int server_read(int sd, int fd, uint64_t file_size){
    uint64_t left_size = file_size;
    int packet_size = BUFF_SIZE;
    uint64_t offset = 0;

    char* file_data = (uint8_t*)mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);

    while(left_size>0){
        packet_size = left_size < BUFF_SIZE ? left_size: BUFF_SIZE;
        sendn(sd, file_data+offset, packet_size);
        offset+=packet_size;
        left_size-=packet_size;
    }
    munmap(file_data);
    return 0;
}

