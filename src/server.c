#include <stdio.h>
#include <sys/types.h>

#define PATH_MAX 260

int main(int argc, char **argv) {
    int arg_port=atoi(argv[1]);
    char root_dir[PATH_MAX];
    strcpy(root_dir, argv[2]);
    
    int sd = socket(AF_INET, SOCK_STREAM, 0);
    long val = 1;
    if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(long)) == -1) {
        printf("setsockopt error: %s (Errno: %d)\n", strerror(errno), errno);
        exit(0);
    }
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(arg_port);
    if (bind(sd, (struct sockaddr *) &server_addr, sizeof(server_addr)) < 0) {
        printf("bind error: %s (Error: %d)\n", strerror(errno), errno);
        exit(0);
    }
    if (listen(sd, 3) < 0) {
        printf("listen error: %s (Errno: %d)\n", strerror(errno), errno);
        exit(0);
    }


    struct sockaddr_in client_addr;
    int addr_len = sizeof(client_addr);
    int client_sd;
    client_sd = accept(sd, (struct sockaddr *) &client_addr, &addr_len);
    printf("Connection build. From port - %ld.\n", client_addr.sin_port);

    if (client_sd < 0) {
        printf("accept error: %s (Errno: %d)\n", strerror(errno), errno);
        exit(0);
    }
    
    MSG message;
    MSG response;
    char* filename;
    char* path;
    filename=(char*)malloc(sizeof(char)*PATH_MAX);
    path=(char*)malloc(sizeof(char)*PATH_MAX);

    while (1) {
        memset(filename,0,sizeof(char)*PATH_MAX);
        memset(path,0,sizeof(char)*PATH_MAX);

        recvn(sd, message, filename, PATH_MAX+sizeof(MSG));
        strcpy(path, root_dir);
        strcat(path, filename);

        if(message.flag=='R'){
            printf("Receive a read request.\n");
            fd = open(path, O_RDONLY);

            if (fd < 0) {
                response.flag='N';  //not found
                sendn(sd, response, NULL, 0);
                perror("Open failed: ");
            }
            else{
                response.flag='r';
                uint64_t file_size = lseek(fd, 0, SEEK_END);
                response.file_length=file_size;
                sendn(sd, response, NULL, 0);
                printf("Start remote read: %s \n", path);
                server_read(sd, fd, file_size);
                close(fd);
                printf("Complete remote read: %s", path);
            }
            fd=-1;
        }
        else if(message.flag=='W'){
            printf("Receive a write request.\n");
            fd = open(path, O_WRONLY | O_CREAT);

            if (fd < 0) {
                response.flag='N';  //not found
                sendn(sd, response, NULL, 0);
                perror("Open failed: ");
            }
            else{
                response.flag='w';
                sendn(sd, response, NULL, 0);
                printf("Start remote write: %s \n", path);
                server_save(sd, fd, message.file_length);
                close(fd);
                printf("Complete remote write: %s", path);
            }
        }
    }
}
