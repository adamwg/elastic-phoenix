#ifndef DISTRIBUTED_H_
#define DISTRIBUTED_H_

#include <pthread.h>

#include "synch.h"

#define N_WORKERS 1

/* The Nahanni device */
#define SHM_DEV        "/dev/uio0"
/* The size of the shared memory, in bytes */
#define SHM_SIZE       (1024L * 1024L * 1024L)
/* Where we should map the memory */
#define SHM_LOC        (void *)(1024L * 1024L * 1024L * 1024L)

#define MASTER if(master_node)
#define WORKER if(!master_node)

typedef struct {
	pthread_spinlock_t lock;
	int count;
	int alldone;
	int exited;
} mr_barrier_t;

void *shm_base;

void shm_init();
void barrier_init(mr_barrier_t *bar);
void barrier(mr_barrier_t *bar);

int master_node;

#endif /* DISTRIBUTED_H_ */
