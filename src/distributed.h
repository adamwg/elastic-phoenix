#ifndef DISTRIBUTED_H_
#define DISTRIBUTED_H_

#include <pthread.h>

#include "struct.h"
#include "synch.h"

/* The Nahanni device */
#define SHM_DEV        "/dev/uio0"
/* The size of the shared memory, in bytes */
#define SHM_SIZE       (4096L * 1024L * 1024L)
/* Where we should map the memory */
#define SHM_LOC        (void *)(1024L * 1024L * 1024L * 1024L)

#define MAX_WORKER_THREADS 32
#define L_NUM_THREADS 4

#define MASTER if(master_node)
#define WORKER if(!master_node)

typedef struct {
	pthread_spinlock_t lock;
	volatile int count;
	volatile int alldone;
	volatile int exited;
} mr_barrier_t;

/* Shared mr state */
typedef struct {
	/* The "Big Phoenix Lock", which joining workers will acquire while they set
	 * up */
	pthread_spinlock_t bpl;
	
	keyvals_arr_t **intermediate_vals;
	keyval_arr_t *final_vals;
	keyval_arr_t *merge_vals;
	void *result;
	int result_len;
	mr_barrier_t mr_barrier;
	int num_map_tasks;
	int num_reduce_tasks;

	int worker_counter;

	int current_stage;
} mr_shared_env_t;

mr_shared_env_t *mr_shared_env;
#define BARRIER() barrier(&mr_shared_env->mr_barrier)

void *shm_base;

void shm_init();
void barrier_init(mr_barrier_t *bar);
void barrier(mr_barrier_t *bar);

/* 1 if this is the master process */
int master_node;

#endif /* DISTRIBUTED_H_ */
