CC = g++
CFLAGS= -O3 -Wall -std=c++11 -pedantic
LIBS = -lpthread\

all: server client mysort

clean:
	rm -f *.o
	rm -f server
	rm -f client
	rm -f *~
	rm -f core
log:
	rm log*

mysort.o: mySort.cpp
	$(CC) $(CFLAGS) -g -c mySort.cpp

mysort:  mysort.o
	$(CC) $(CFLAGS) -g -o mysort mySort.o      

server.o: server.cpp
	$(CC) $(CFLAGS) -g -D_FILE_OFFSET_BITS=64 -c server.cpp ${LIBS}

server:  server.o
	$(CC) $(CFLAGS) -g -D_FILE_OFFSET_BITS=64  -o server server.o ${LIBS}

client.o: client.cpp
	$(CC) $(CFLAGS) -g -c client.cpp ${LIBS}

client:  client.o
	$(CC) $(CFLAGS) -g  -o client client.o ${LIBS}

