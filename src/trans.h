#include <sys/socket.h>
#include <string.h>
#include <stdlib.h>

//this is only used in msg, i.e. create a connection. Data transmission doesn't use this
typedef struct msg_header {
    uint8_t flag;
    uint64_t file_length;   //file will be send later
    uint32_t payload_length;    //payload is filename
} MSG;


//length in functions are payload len
int sendn(int sd, char* buf, int length){
    int left_len = length;
    int copy_len = 0;
    while(left_len>0){
        copy_len = send(sd, buf + length - left_len, left_len, 0);
        if(copy_len < 0){
            printf("Send Error: %s (Errno: %d)\n", strerror(errno), errno);
            return -1;
        }
        else if(copy_len == 0)
            return 0;
        left_len-=copy_len;
    }
    return length;
}

int sendn(int sd, MSG* msg, char* payload, int length){
    char * buf;
    buf=(char*)malloc((length+sizeof(MSG))*sizeof(char));
    memcpy(buf, msg, sizeof(MSG));
    if(length!=0)
        memcpy(buf+sizeof(MSG), payload, length);
    int ret = sendn(sd, buf, length+sizeof(MSG));
    free(buf);
    return ret;
}

int recvn(int sd, char* buf, int length){
    int left_len = length;
    int copy_len = 0;
    while(left_len > 0){
        copy_len = recv(sd, buf + length - left_len, left_len, 0);
        if(copy_len < 0){
            printf("Send Error: %s (Errno: %d)\n", strerror(errno), errno);
            return -1;
        }
        else if(copy_len == 0)
            return 0;
        left_len-=copy_len;
    }
    return length;
}

int recvn(int sd, MSG* msg, char* payload, int length){
    char * buf;
    buf=(char*)malloc((length+sizeof(MSG))*sizeof(char));
    int ret = recvn(sd, buf, length+sizeof(MSG));
    memcpy(msg, buf, sizeof(MSG));
    memcpy(payload, buf+sizeof(MSG), length);
    free(buf);
    return ret;
}

char* get_filename(char* path){
    return strrchr(path, '\');
}
