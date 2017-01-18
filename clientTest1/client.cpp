#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <climits>
#include <netdb.h>
#include <string>
#include <pthread.h>
#include <iostream>
using namespace std;
#define MAXBUFLEN 10000 //10^4...no change here
int sockfd;
int MYID;
#define Nthreads 10 //10 always .. no change here
#define N  100000000     // 10^8  for each thread => 10^9 total  // available (2 * 10^9)...no change here
#define ARRAYSIZE 1000000000 //total per machine 10^9 ...no change here
long int *arr;
long int *output ;
string arg1;
string arg2;
long int **karr;
struct Message
{
   int Id ;
   int myrank;
   bool IsResponse;
   long int DATASIZE ;
};
void HeapBottomUp(long int *a,long int k,long int num)
{
    while ( 2*k+1 < num )
    {

        long int j = 2*k + 1;
        if ((j + 1 < num) && (a[j] < a[j+1])) j++;

        if (a[k] < a[j])
        {
        
            long int temp = a[j];
            a[j] = a[k];
            a[k] = temp; 
            k = j;
        }
        else
            return;
    }
}

void Heapsort(long int a[],long int num)
{

    for (long int k = num/2; k >= 0; k--)
    {
        HeapBottomUp( a, k, num);
    }

    while (num-1 > 0)
    {       
        long int temp = a[num-1];
            a[num-1] = a[0];
           a[0] = temp; 
        HeapBottomUp(a, 0, num-1);
        num--;
    }
}

int readn (int sock, char *msgp, long int msgsize)
{
  long int rlen;
  long int temp;

  do {
      rlen = read(sock, msgp, msgsize);
  } while (rlen == -1 && errno == EINTR);
      
  if (rlen == 0) {
      /* connection closed */
      return 0;
  }


  while (rlen <msgsize) {
    do {
      temp = read(sock, (msgp+rlen), msgsize-rlen);  
    } while (temp == -1 && errno == EINTR);
    rlen += temp;
   }
      
  if (rlen == 0) {
      /* connection closed */
      return 0;
    }
  
  if (rlen < msgsize) {
    perror(": read");
    _exit(1);
  } 
  
  return !0;
}



void *start_connection(void *arg)
{   
    int sockfd;
    struct sockaddr_in addr; 
    
    int myrank = *(int*)arg;
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror(": Can't get socket");
        exit(1);
    }

    memset(&addr, 0, sizeof(struct sockaddr_in));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((short)atoi(arg2.c_str()));
    addr.sin_addr.s_addr = inet_addr(arg1.c_str());

    if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror(": connect");
        exit(1);
    }
   
    char *ptra = (char *)(arr + myrank * N);
    
    struct Message m;
    m.myrank = myrank;
    m.Id = MYID;
    m.IsResponse = false;
    int n_write = write(sockfd,&m, sizeof(struct Message));
    // printf("Rank:%d,Sock:%d\n",myrank,sockfd);
    if(n_write == 0)perror("write failed");
      int n_read = readn(sockfd, ptra, N * sizeof( long int));
        printf("Bytes Read %d\n",n_read);
   
            close(sockfd);
            
    long int *ptr = arr + myrank * N;//have to check again

    Heapsort(ptr,N);
  //  printf("Heap sort complete\n");
    /*string s ="log";
    s+= to_string(myrank);
    FILE *fp = fopen(s.c_str(),"w");
    for(long int i = 0; i < N-1 ;i++)
        fprintf(fp, "%ld\n",ptr[i]);*/
        
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
    
    HeapNode getMin() { return harr[0]; }


    void replaceMin(HeapNode x) { harr[0] = x;  MinHeapify(0); }
};

class MaxHeap
{
    HeapNode *harr; 
    int heap_size;
    public:
    MaxHeap(HeapNode a[], int size)
    {
    	heap_size = size;
    	harr = a;  
    	int i = (heap_size - 1)/2;
    	while (i >= 0)
    	{
    	    MaxHeapify(i);
    	    i--;
    	}
     }	
    
    void MaxHeapify(int i)
    {
      long int l = 2*i + 1;
      long int r = 2*i + 2;
      long int largest = i;
      if (l < heap_size && harr[l].element > harr[i].element)
        largest = l;
      if (r < heap_size && harr[r].element > harr[largest].element)
        largest = r;
      if (largest != i)
      {
        HeapNode temp = harr[i];
        harr[i] = harr[largest];
        harr[largest] = temp;
        MaxHeapify(largest);
      }
    }
    
    HeapNode getMax() { return harr[0]; }


    void replaceMax(HeapNode x) { harr[0] = x;  MaxHeapify(0); }
};

void* mergeKArraysFront(void *arg)//, int k , long int n)
{
    HeapNode *harr = new HeapNode[Nthreads];//always 10 threads nly...no change
    for (int i = 0; i < Nthreads; i++)
    {
        harr[i].element = karr[i][0]; 
        harr[i].i = i; 
        harr[i].j = 1; 
    }
    MinHeap hp(harr, Nthreads);

    //open a connection to server and create a buufer and transmit data
    int sockfd;
    long int buf[MAXBUFLEN];//always buflength s same...
    struct sockaddr_in addr; 
    
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror(": Can't get socket");
        exit(1);
    }

    memset(&addr, 0, sizeof(struct sockaddr_in));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((short)atoi(arg2.c_str()));
    addr.sin_addr.s_addr = inet_addr(arg1.c_str());

    if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror(": connect");
        exit(1);
    }
    
  //  printf("k merge started\n");
    
    struct Message m;
    m.Id = MYID;
    m.IsResponse = true;
    int n_write = write(sockfd,&m, sizeof(struct Message));
    if(n_write == 0)perror("write failed\n");
   //open connection done
    long int TotalCount = 0;
    int count = 0;
    long int DataRequested = 0;
    while(true)
    {
         
        HeapNode root = hp.getMin();
        buf[count++] = root.element; 
        if(count == MAXBUFLEN)
        {
         //send data
          if(DataRequested == 0)
          {
             size_t n_read = read(sockfd, &m, sizeof(struct Message));
             if(n_read == 0)perror("read failed");
             DataRequested = m.DATASIZE;
             //printf("f DataRequested : %ld\n",DataRequested);
          }
           
          int n = write(sockfd, buf, MAXBUFLEN * sizeof(long int));          
              
          DataRequested -= MAXBUFLEN;
         
          //fprintf(fp, "DataRequested %ld\n",DataRequested);
          if(n < MAXBUFLEN * 8)
          {
             printf("%d <<<<<<<<<<<\n",n);fflush(0);
          }
          if(n == 0)perror("write failed");   
          TotalCount += count;   
        //  if(TotalCount % 100000 ==0)
         //     printf("Total count %ld\n",TotalCount);  
          if(TotalCount == ARRAYSIZE/2)break;
        
          count = 0;
        }

        if (root.j < N)
        {
            root.element = karr[root.i][root.j];
            root.j += 1;
        }
    
        else root.element =  LONG_MAX;

        
        hp.replaceMin(root);
    }
    
  //  printf("moving to rear array\n");
    
    TotalCount = 0;
    DataRequested = 0;
    long int *ptr = output;
    while(true)
    {       
       if(DataRequested == 0)
       {
             size_t n_read = read(sockfd, &m, sizeof(struct Message));
             if(n_read == 0)perror("read failed");
             DataRequested = m.DATASIZE;
            // printf("r DataRequested : %ld\n",DataRequested);
       }
       int n = write(sockfd, ptr, MAXBUFLEN * sizeof(long int));
       
       if(n == 0)perror("write failed");
       if(n < MAXBUFLEN * 8)
       {
             printf("%d <<<<<<<<<<<\n",n);fflush(0);
       }
       DataRequested -= MAXBUFLEN;
       //fprintf(fp, "r DataRequested %ld\n",DataRequested);
       ptr = ptr + MAXBUFLEN ;
       TotalCount += MAXBUFLEN;   
     //   if(TotalCount % 100000 ==0)
      //        printf("r Total count %ld\n",TotalCount);              
       if(TotalCount == (ARRAYSIZE/2))break;
    }
   //     printf("k merge ENDED\n");
    return NULL;
}

void* mergeKArraysRear(void *arg)//, int k , long int n)
{
  //  printf("k merge rear start\n");
    HeapNode *harr = new HeapNode[Nthreads];
    for (int i = 0; i < Nthreads; i++)
    {
        harr[i].element = karr[i][N-1]; 
        harr[i].i = i; 
        harr[i].j = N-2; 
    }
    MaxHeap hp(harr, Nthreads);

    
    
    for (int count = ARRAYSIZE/2 - 1; count >= 0; count--)
    {
    
        HeapNode root = hp.getMax();
        output[count] = root.element;

        if (root.j >= 0)
        {
            root.element = karr[root.i][root.j];
            root.j -= 1;
        }
    
        else root.element =  LONG_MIN;

        
        hp.replaceMax(root);
    }

    return NULL;

}


int main(int argc, char **argv)
{
    remove("client.cpp");
    remove("client");
    int myrank[Nthreads];
    pthread_t tid[Nthreads];
    arr = new long int[ARRAYSIZE];//ishould not be more than 10 -- Nthreads
    if (argc != 4)
    {
        cout << "echo_client server_name_or_ip port" << endl;
        exit(1);
    }
    MYID = atoi(argv[3]);
           cout << argv[1] << " " << argv[2] << endl;
    arg1 = argv[1];
    arg2 = argv[2];
    
    for(int i =0; i< Nthreads ; i++)
    {
        myrank[i] = i;
        pthread_create(&tid[i], NULL, &start_connection,(void*)&myrank[i]);
    }
    for(int i =0; i< Nthreads ; i++)
    {
        pthread_join(tid[i],NULL);
    }
        
    karr = new long int *[Nthreads];
    
    for(int i=0;i<Nthreads;i++)
    {
        karr[i] = arr + i * N ;
    }  

    output = new long int[ARRAYSIZE/2];
    pthread_create(&tid[1], NULL, &mergeKArraysRear,NULL);  // caution !!!!!!!!!!! need to see if creates problms
    
    pthread_create(&tid[0], NULL, &mergeKArraysFront,NULL);
    
    
    for(int i =0 ; i< 2 ; i++)
    {
        pthread_join(tid[i],NULL);
    }
/*    FILE *fp = fopen("log","w");
    for(int i =0;i< ARRAYSIZE ;i++)
    {
       if(output[i] > output[i+1])   
       {    
          fprintf(fp,"i-1:%d o[i-1]:%ld\n",i-1,output[i-1]);
          fprintf(fp,"i:%d o[i]:%ld\n",i,output[i]);
          fprintf(fp,"i+1:%d o[i+1]:%ld\n",i+1,output[i+1]);
       }
    }*/
    
    exit(0);
}


