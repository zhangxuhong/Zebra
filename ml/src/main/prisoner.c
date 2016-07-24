#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>


/* Define globally accessible variables and a mutex */

#define NUMTHRDS 64
#define OFF 0
#define ON 1
   pthread_t Prisoner[NUMTHRDS];
   pthread_mutex_t mutexlight;
   pthread_mutex_t mutexstop;

int light;
int stop;

void *prisoner(void *arg)
{
   long tid;
   tid = (long)arg;
   
   if(tid ==0)
   {
     int count = 1;  
     for(;count < NUMTHRDS;)
     {
       pthread_mutex_lock (&mutexlight);
       if(light == ON) 
       {
        light = OFF;
        count ++;
       }
       pthread_mutex_unlock(&mutexlight);
   }
   
     // printf("leader #%ld! decleare: we have all visited the switch room at least once\n", tid);

     pthread_mutex_lock (&mutexstop);
     stop = 1;
     pthread_mutex_unlock(&mutexstop);
   }
   
   else
   {  
     int flag =1, free=0;
     while( free ==0 )
    {
      pthread_mutex_lock (&mutexlight);
      if(light == OFF && flag > 0) 
      {
        flag--;
        light = ON;
      //  printf(" P #%ld! in room and turn light ON\n", tid);
       }
      pthread_mutex_unlock(&mutexlight);
    
      pthread_mutex_lock (&mutexstop);
      if(stop == 1)
       free = 1;
      pthread_mutex_unlock(&mutexstop);
     }
   }

     pthread_exit(NULL);
}

int main (int argc, char *argv[])
{
   long i = 0;
   void *status;
   pthread_attr_t attr;;
    
   clock_t t_start, t_end;

   stop = 0;
   light = OFF;  
   
   pthread_mutex_init(&mutexlight, NULL);
   pthread_mutex_init(&mutexstop, NULL);
        
   /* Create threads to perform the game*/
   
  pthread_attr_init(&attr);

  t_start = clock();

  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

   for(; i<NUMTHRDS; i++)
      pthread_create(&Prisoner[i], NULL, prisoner, (void *)i);

   for (i=0;i<NUMTHRDS;i++)
      pthread_join(Prisoner[i],NULL);

   t_end = clock();
   
   printf("time: %fs\n", (double)(t_end - t_start)/(double)CLOCKS_PER_SEC); 

   pthread_attr_destroy(&attr);
   pthread_mutex_destroy(&mutexlight);
   pthread_mutex_destroy(&mutexstop);

   pthread_exit(NULL);
//finish
}
