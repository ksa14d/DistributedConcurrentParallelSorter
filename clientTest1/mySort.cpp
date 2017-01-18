#include <iostream>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <climits>
#include <stdlib.h>
#include <sys/wait.h>

#define N_MACHINES 8
using namespace std;

int main(int argc , char **argv)
{
      pid_t pid[11];
      int i;
      string username = "cop5570g";
      string machines[] = {"C5570-1","C5570-2","C5570-3","C5570-4","C5570-5","C5570-6","C5570-7","C5570-9"};
      //    char** IP = {"192.168.28.125","192.168.28.126","192.168.28.127","192.168.28.128","192.168.28.131","192.168.28.132","192.168.28.133","192.168.28.134"};
      string serverIP = "192.168.28.133";
      char temp[600];
      int ret;
       if(argc < 2)
       {
         printf("usage ./mysort absolutefilepath\n");
         exit(0);
       }
       string fileToSort = argv[1];
	if((pid[i]=fork())==0)
	{
          sprintf(temp,"./server %s",fileToSort.c_str()); 
	  if((ret = execl("/bin/sh","sh","-c", temp ,(char *)0))==-1)
            perror("Error execl");
            exit(13);
	}
	else sleep(4);
      for(int i = 0; i < N_MACHINES ; i++)
      {

          
          if((pid[i]=fork())==0)
          {
            sprintf(temp,"scp -q client.cpp %s@%s:./ ; ssh -q %s@%s 'g++ -o client -Wall -pedantic -std=c++11  client.cpp -lpthread;' ; ssh -q %s@%s './client %s 7531 %d' ",username.c_str(),machines[i].c_str(),username.c_str(),machines[i].c_str(),username.c_str(),machines[i].c_str(),serverIP.c_str(),i);
            if((ret = execl("/bin/sh","sh","-c", temp ,(char *)0))==-1)
            perror("Error execl");
            exit(13);
          }
          else if(pid[i]>0)
          {
             sleep(2);
          }
          
      }
      
      for(int i = 0; i < N_MACHINES ; i++)
      {
           int status;
           wait(&status);
      }
      
      return 0;
    printf("Exiting\n");
}

