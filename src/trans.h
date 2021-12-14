#include <sys/socket.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>

//socket returns ssize_t, signed 64-bit...

//this is only used in msg, i.e. create a connection. Data transmission doesn't use this
typedef struct msg_header {
    uint8_t flag;
    uint64_t file_length;   //file will be send later
    uint32_t payload_length;    //payload is filename
} MSG;


//length in functions are payload len
int64_t sendn(int sd, char* buf, int length){
    int left_len = length;
    int64_t copy_len = 0;
	int64_t done = 0;
    while(left_len>0){
        copy_len = send(sd, buf + length - left_len, left_len, 0);
        if(copy_len < 0){
            printf("Send Error: %s (Errno: %d)\n", strerror(errno), errno);
            return -1;
        }
        else if(copy_len == 0)
            return 0;
        left_len-=copy_len;
		done+=copy_len;
    }
    return done;
}

int64_t sendm(int sd, MSG* msg, char* payload, int length){
    char * buf;
    buf=(char*)malloc((length+sizeof(MSG))*sizeof(char));
    memcpy(buf, msg, sizeof(MSG));
    if(length!=0)
        memcpy(buf+sizeof(MSG), payload, length);
    int64_t ret = sendn(sd, buf, length+sizeof(MSG));
    free(buf);
	if(ret==0)
		ret = -1;
    return ret;
}

int64_t recvn(int sd, char* buf, int length){
    int left_len = length;
    int64_t copy_len = 0;
	int64_t done;
	while(left_len > 0){
        copy_len = recv(sd, buf + length - left_len, left_len, 0);
        if(copy_len < 0){
            printf("Send Error: %s (Errno: %d)\n", strerror(errno), errno);
            return -1;
        }
        else if(copy_len == 0)
            return 0;
        left_len-=copy_len;
		done+=copy_len;
    }
    return done;
}

int64_t recvm(int sd, MSG* msg, char* payload, int length){
    char * buf;
    buf=(char*)malloc((length+sizeof(MSG))*sizeof(char));
    int64_t ret = recvn(sd, buf, length+sizeof(MSG));
    memcpy(msg, buf, sizeof(MSG));
    memcpy(payload, buf+sizeof(MSG), length);
    free(buf);
	if(ret==0)
		ret = -1;
    return ret;
}

