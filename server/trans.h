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
int sendn(int sd, char* buf, int length){
	int64_t ret;
    while(length){
        ret = send(sd, buf, length, 0);
        if(ret < 0){
            //printf("Send Error: %s (Errno: %d)\n", strerror(errno), errno);
            return (int)ret;
        }
        else
		{
			buf += ret;
			length -= ret;
		}
    }
    return 0;
}

int sendm(int sd, MSG* msg, char* payload, int length){
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
	int64_t ret;
	while(length){
        ret = recv(sd, buf, length, 0);
        if(ret < 0){
            //printf("Send Error: %s (Errno: %d)\n", strerror(errno), errno);
            return (int)ret;
        }
        else
		{
			buf += ret;
			length -= ret;
		}
    }
    return 0;
}

int recvm(int sd, MSG* msg, char* payload, int length){
    char * buf;
    buf=(char*)malloc((length+sizeof(MSG))*sizeof(char));
    int64_t ret = recvn(sd, buf, length+sizeof(MSG));
    memcpy(msg, buf, sizeof(MSG));
    memcpy(payload, buf+sizeof(MSG), length);
    free(buf);
    return ret;
}

