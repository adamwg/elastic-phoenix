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

#ifndef MASTER_APP
#ifndef WORKER_APP
#error "Enable one of MASTER_APP or WORKER_APP"
#endif /* !WORKER_APP */
#endif /* !MASTER_APP */

#ifdef MASTER_APP
#ifdef WORKER_APP
#error "Enable only one of MASTER_APP or WORKER_APP"
#endif /* WORKER_APP */
#define MASTER if(1)
#define WORKER if(0);
#endif /* MASTER_APP */

#ifdef WORKER_APP
#ifdef MASTER_APP
#error "Enable only one of MASTER_APP or WORKER_APP"
#endif /* MASTER_APP */
#define WORKER if(1)
#define MASTER if(0)
#endif /* WORKER_APP */

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

#endif /* DISTRIBUTED_H_ */
