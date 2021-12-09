server: server.o 
	gcc -o server server.o

server.o: server.c server.h trans.h
	gcc -c server.c

clean:
	rm server server.o
