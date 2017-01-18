#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE
#define _XOPEN_SOURCE 600
#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <strings.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <climits>
#include <stdlib.h>
#include <pthread.h>

using namespace std;

#define N_MACHINES 8  //have to change every time
const unsigned port = 7531;
const long int N  = 1000000000 ; // 10^9
const unsigned MAXBUFLEN = 10000; //constant
pthread_mutex_t kmerge_mutex;
pthread_mutex_t senddata_mutex;
pthread_cond_t senddata_cv;
pthread_cond_t kmerge_cv;
pthread_barrier_t barr;
pthread_barrier_t sendbarr;
string fileToSort;

struct Message
{
    int Id ;
    int myrank;
    bool IsResponse;
    long int DATASIZE ;
};

class Connection
{
public :
    int sockfd;
    long int *data = new long int[2 * MAXBUFLEN * MAXBUFLEN];
    int BufferConsumed;
    int numreset;
    bool IsDataAvailable;
    long int *dptr ;
    Connection()
    {
        sockfd = -1;
        dptr = data;
        BufferConsumed = 0;
        numreset = 1;
        IsDataAvailable = false;
    }

    void RefillBuffer()
    {
        struct Message m;
        m.DATASIZE = MAXBUFLEN ;
        int n = write(sockfd,&m,sizeof(struct Message));
        if(n < 0)
        {
            printf("could not write packet\n");
            pthread_mutex_lock(&senddata_mutex);                                   
            printf("above shutdown write\n\n");
            sockfd = -1;
            pthread_mutex_unlock(&senddata_mutex);
        }
        BufferConsumed = 0;
    }

    void ResetPtr()
    {
        dptr = data;
    }

};

Connection client[N_MACHINES];


int readn (int sock, char *msgp, long int msgsize)
{
    long int rlen;
    long int temp;

    do
    {
        rlen = read(sock, msgp, msgsize);
    }
    while (rlen == -1 && errno == EINTR);

    if (rlen == 0)
    {
        /* connection closed */
        return 0;
    }


    while (rlen <msgsize)
    {
        do
        {
            temp = read(sock, (msgp+rlen), msgsize-rlen);
        }
        while (temp == -1 && errno == EINTR);
        rlen += temp;
    }

    if (rlen == 0)
    {
        /* connection closed */
        return 0;
    }

    if (rlen < msgsize)
    {
        perror(": read");
        _exit(1);
    }

    return !0;
}

int **Sockets;
int n_sockfd;


void* Distribute(void* arg) //have to check later
{

   int id  = *(int *)arg;
   // pthread_detach(pthread_self()); -----------------------> To check
     printf("Inside Distribute\n");
    int i =0;
    int* ptr = Sockets[id];
    long int Total = N_MACHINES * N / 10;  //have to change if machine changes
    long int buf[MAXBUFLEN]; 
    int fd = open(fileToSort.c_str(),O_LARGEFILE|O_RDONLY);
    printf("id %d\n",id);
    lseek64(fd, (N_MACHINES * N / 10 * id * sizeof(long int))  , SEEK_SET);// change even if machine changes
   while(true)
   {
            int n_read = read(fd,buf,MAXBUFLEN*sizeof(long int)) ;
            if(n_read == 0)perror("fread error\n");
            int n = write(ptr[i], buf, MAXBUFLEN * sizeof(long int));
            if(n == 0)perror("write failed");         
            Total -= MAXBUFLEN;
            if(Total == 0)break;
            //if(Total % 500000 ==0)
                //printf("Thread %d reach  %ld\n",id,Total);
            i++ ;
            if(i == N_MACHINES)i=0;            
   }
    for(int i =0 ; i< N_MACHINES ; i++)
    {
        close(ptr[i]);
    }

   return NULL;
}
/*void* SendDataToClients(void* arg)
{
    printf("Inside Send data waiting for signal\n");
    pthread_mutex_lock(&senddata_mutex);
    pthread_cond_wait(&senddata_cv, &senddata_mutex);
    pthread_mutex_unlock(&senddata_mutex);
    printf(" Send data after signal\n");
    pthread_t tid[10];
    int mydata[10];
    for(int i = 0 ; i < 10 ; i++ )
    {
      mydata[i] = i;
      pthread_create(&tid[i], NULL, &Distribute, (void*)mydata[i]);
    }
    
    return NULL;
}*/
int ThreadCount = 0;
void CollectClientSockets(int sockfd, int rank, int myid)
{
    
    pthread_mutex_lock(&senddata_mutex);
    Sockets[rank][myid] = sockfd;
    pthread_mutex_unlock(&senddata_mutex);
    printf("Inside collect c sock waiting for others\n");
    int rc = pthread_barrier_wait(&sendbarr);
    if(rc != 0 && rc != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        printf("Could not wait on barrier\n");
        exit(-1);
    }
    printf("Inside collect c sock after barr\n");
    pthread_mutex_lock(&senddata_mutex);
    //pthread_cond_signal(&senddata_cv);
    ThreadCount++;
              printf("Thread count %d\n",ThreadCount);
    if(ThreadCount == N_MACHINES * 10) //change here
    {
       for(int i = 0; i< 10; i++)
       {
          for(int j = 0; j < 10;j++)
           printf("%d   ",Sockets[i][j]);
           printf("\n");
       }
       pthread_t tid[10];
       int mydata[10];
        for(int i = 0 ; i < 10 ; i++ )
        {
          printf("Thread created\n");
          mydata[i] = i;
          pthread_create(&tid[i], NULL, &Distribute, (void*)&mydata[i]);
        }
    }
    pthread_mutex_unlock(&senddata_mutex);
    
}


void ReceiveDataFromClient(int sockfd, int myId) // collaborate with other threads this function is assciated with machines
{
    //long int buf[MAXBUFLEN];
    printf("in recvDataToClient start\n\n");
    client[myId].sockfd =  sockfd;

    long int *ptrbuf = client[myId].data;
    //request for more first time
    struct Message m;
    m.DATASIZE = 2 * MAXBUFLEN * MAXBUFLEN ;
    int n_write = write(sockfd,&m, sizeof(struct Message));
    if(n_write == 0)perror("write error\n");
    int n_read = readn(sockfd, (char *)ptrbuf, 2 * MAXBUFLEN * MAXBUFLEN * sizeof( long int));

    if(n_read == 0)perror("read error\n");
    //use pthread barrier
    int rc = pthread_barrier_wait(&barr);
    if(rc != 0 && rc != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        printf("Could not wait on barrier\n");
        exit(-1);
    }
    //trigger k merger
    pthread_mutex_lock(&kmerge_mutex);
    pthread_cond_signal(&kmerge_cv);
    pthread_mutex_unlock(&kmerge_mutex);

    long int *dest =  client[myId].data;
    long int Total = 2 * MAXBUFLEN * MAXBUFLEN;
    while (1)
    {
        int n_read = readn(sockfd, (char *)dest, MAXBUFLEN * sizeof(long int));
        if(n_read == 0) break;
        dest = dest + MAXBUFLEN;
        Total -= MAXBUFLEN;
        if(Total == 0)
        {
            dest = client[myId].data;
            Total = 2 * MAXBUFLEN * MAXBUFLEN;
            // signal to kmerger
             pthread_mutex_lock(&senddata_mutex);
              client[myId].IsDataAvailable = true;
             pthread_cond_signal(&senddata_cv);
             pthread_mutex_unlock(&senddata_mutex);
           
        }
        //add the chunck to appropriate place in the karr[myid]->{ x x x x}
    }
    pthread_mutex_lock(&senddata_mutex);
    printf("above shutdown read\n\n");
    close(sockfd);
    sockfd = -1;
    pthread_mutex_unlock(&senddata_mutex);
    printf("in recvDataToClient end\n\n");
}

void *process_connection(void *arg) //process connection should transfer data
{
    int sockfd  = *((int *)arg);
    free(arg);
    pthread_detach(pthread_self());

    
    struct Message m;
    size_t n_read = read(sockfd, &m, sizeof(struct Message));
    if(n_read == 0)perror("read failed");
    printf("sock %d , rank %d",sockfd,m.myrank);
    if(!m.IsResponse)
    {
        CollectClientSockets(sockfd,m.myrank,m.Id);
    }
    else
    {
        ReceiveDataFromClient(sockfd,m.Id);
    }
    return(NULL);
}

struct HeapNode
{
    long int element;
    long int i;
    long int j;
};

class MinHeap
{
    HeapNode *harr;
    int heap_size;
public:
    MinHeap(HeapNode a[], int size)
    {
        heap_size = size;
        harr = a;
        int i = (heap_size - 1)/2;
        while (i >= 0)
        {
            MinHeapify(i);
            i--;
        }
    }

    void MinHeapify(int i)
    {
        long int l = 2*i + 1;
        long int r = 2*i + 2;
        long int smallest = i;
        if (l < heap_size && harr[l].element < harr[i].element)
            smallest = l;
        if (r < heap_size && harr[r].element < harr[smallest].element)
            smallest = r;
        if (smallest != i)
        {
            HeapNode temp = harr[i];
            harr[i] = harr[smallest];
            harr[smallest] = temp;
            MinHeapify(smallest);
        }
    }

    HeapNode getMin()
    {
        return harr[0];
    }


    void replaceMin(HeapNode x)
    {
        harr[0] = x;
        MinHeapify(0);
    }
};


void *MergeKArrays(void *arg)
{
    FILE *fp = fopen("log_output","w");
    pthread_mutex_lock(&kmerge_mutex);
    pthread_cond_wait(&kmerge_cv, &kmerge_mutex);
    pthread_mutex_unlock(&kmerge_mutex);
    printf("in merge k arrays after cond wait\n");
    //Start kmerge
    HeapNode *harr = new HeapNode[N_MACHINES];
    for(int i=0; i< N_MACHINES ; i++)
    {
        harr[i].element = client[i].dptr[0]; //consumed
        harr[i].i = i;
        harr[i].j = 1;
        client[i].dptr++;
        client[i].BufferConsumed++;
    }
    MinHeap hp(harr, N_MACHINES);
    long long int end = N * N_MACHINES; //have to change "full final answer"
    long long int count = 0;
    for ( count = 0; count < end; count++)  //k clients each having n rows || IsActive and IsData IsInit veri
    {

        HeapNode root = hp.getMin();
       if(count % 10 == 0)//print to file % 10 finally
        {
          fprintf(fp,"%ld\n", root.element);        
        }
        //fprintf(fp,"%ld\n", root.element);   // create a small buffer and write
        //int id = root.i;
        if (root.j < (2 * MAXBUFLEN * MAXBUFLEN))
        {
            int id = root.i;
            root.element = client[id].dptr[0];  //consumed
            client[id].dptr++;
            root.j+= 1;
            client[id].BufferConsumed++;
            if(client[id].BufferConsumed == MAXBUFLEN )
            {
                if(client[id].numreset < 5) //no change if machine changes
                {
                  client[id].RefillBuffer();
                }
                client[id].BufferConsumed = 0;
            }
            
            if(root.j == (2 * MAXBUFLEN * MAXBUFLEN))
            {
                client[id].ResetPtr();
                
                // wait for the signal from recv data
                 pthread_mutex_lock(&senddata_mutex);                                  
                 if(client[id].sockfd!= -1)
                 {
                   if(client[id].numreset < 5)//no change if machine changes
                   {
                        if(!client[id].IsDataAvailable)
                          pthread_cond_wait(&senddata_cv, &senddata_mutex);
                       client[id].IsDataAvailable = false;
                   }
                 }
                 pthread_mutex_unlock(&senddata_mutex);
                 client[id].numreset++;
                printf("Data : %lld value:  %ld Numreset :  %d\n",count,root.element,client[id].numreset);
               
                 if(client[id].numreset != 6)
                 {
                   root.j = 0;
                 }
            }
        }
        else
        {
           
           printf("long max assigned\n");
           root.element =  LLONG_MAX;
            root.j = 0;
        }
        hp.replaceMin(root);
    }
    
        printf("%lld\n",count);
        printf("reached end\n");
          fflush(fp);
fclose(fp);
    exit(0);
    return(NULL);
}


int main(int argc , char **argv)
{
    int serv_sockfd, cli_sockfd, *sock_ptr;
    struct sockaddr_in serv_addr, cli_addr;
    socklen_t sock_len;
    pthread_t tid;
    //pid_t pid[10];
    if(argc < 2)
       {
         printf("usage ./mysort absolutefilepath\n");
         exit(0);
       }
    fileToSort = argv[1];
    Sockets = new int*[11];
    for(int i = 0 ; i < 11 ; i++)
      Sockets[i] = new int[11];
    cout << "port = " << port << endl;
    serv_sockfd = socket(AF_INET, SOCK_STREAM, 0);

    bzero((void*)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(port);

    bind(serv_sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr));

    if(pthread_barrier_init(&barr, NULL, N_MACHINES))
    {
        printf("Could not create a barrier\n");
        return -1;
    }
    
    if(pthread_barrier_init(&sendbarr, NULL, N_MACHINES * 10))
    {
        printf("Could not create a barrier\n");
        return -1;
    }
    pthread_attr_t attr;


    /* For portability, explicitly create threads in a joinable state */
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

   


    pthread_mutex_init(&kmerge_mutex, NULL);
    pthread_cond_init (&kmerge_cv, NULL);

    pthread_mutex_init(&senddata_mutex, NULL);
    pthread_cond_init (&senddata_cv, NULL);

    listen(serv_sockfd, 100);

    /*  string username = "cop5570g";
      string machines[] = {"C5570-2","C5570-3","C5570-4","C5570-5","C5570-6","C5570-7","C5570-8","C5570-9"};
      //    char** IP = {"192.168.28.125","192.168.28.126","192.168.28.127","192.168.28.128","192.168.28.131","192.168.28.132","192.168.28.133","192.168.28.134"};
      string serverIP = "192.168.28.124";
      int ret;
      for(int i = 0; i < N_MACHINES ; i++)
      {

          char temp[600];
          if((pid[i]=fork())==0)
          {
            sprintf(temp,"scp -q client.cpp %s@%s:./ ; ssh -q %s@%s 'g++ -o client -Wall -pedantic -std=c++11  client.cpp -lpthread' ; ssh -q %s@%s './client %s 7531 %d' ",username.c_str(),machines[i].c_str(),username.c_str(),machines[i].c_str(),username.c_str(),machines[i].c_str(),serverIP.c_str(),i);
            if((ret = execl("/bin/sh","sh","-c", temp ,(char *)0))==-1)
            perror("Error execl");
            exit(13);
          }
      }*/

//    pthread_create(&tid, NULL, &SendDataToClients, (void*)NULL);
    pthread_create(&tid, NULL, &MergeKArrays, (void*)NULL);
    for (; ;)
    {
        sock_len = sizeof(cli_addr);
        cli_sockfd = accept(serv_sockfd, (struct sockaddr *)&cli_addr, &sock_len);

        cout << "remote client IP == " << inet_ntoa(cli_addr.sin_addr);
        cout << ", port == " << ntohs(cli_addr.sin_port) << endl;

        sock_ptr = (int *)malloc(sizeof(int));
        *sock_ptr = cli_sockfd;
        pthread_create(&tid, &attr, &process_connection, (void*)sock_ptr);
    }
    pthread_attr_destroy(&attr);
    pthread_mutex_destroy(&kmerge_mutex);
    pthread_cond_destroy(&kmerge_cv);
    pthread_mutex_destroy(&senddata_mutex);
    pthread_cond_destroy(&senddata_cv);
    printf("Exiting\n");
}

