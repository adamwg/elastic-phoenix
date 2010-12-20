#ifndef DISTRIBUTED_H_
#define DISTRIBUTED_H_

#include <pthread.h>

#include "struct.h"
#include "synch.h"

/* Where we should map the memory */
#define SHM_LOC        (void *)(1024L * 1024L * 1024L * 1024L)
/* The maximum number of worker threads.  At four threads per worker process,
 * this is eight workers */
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

/* Shared state */
typedef struct {
	/* The "Big Phoenix Lock" prevents workers from joining the MapReduce at
	 * critical times, like when we're between stages. */
	pthread_spinlock_t bpl;
	
	keyvals_arr_t **intermediate_vals;
	keyval_arr_t *final_vals;
	keyval_arr_t *merge_vals;
	void *result;
	void *task_data;
	int result_len;
	mr_barrier_t mr_barrier;
	int num_map_tasks;
	int num_reduce_tasks;

	volatile int worker_counter;
	int current_stage;
} mr_shared_env_t;

mr_shared_env_t *mr_shared_env;

void *shm_base;

/* Initialize Nahanni shared memory.
 *
 * in_vm - true if we're in a virtual machine.
 * shm_device - path to the Nahanni device, or the name of a POSIX SHM object.
 * shm_size - size of the shared memory, in megabytes.
 */
void shm_init(bool in_vm, char *shm_device, size_t shm_size);
/* Initialize a barrier in memory. */
void barrier_init(mr_barrier_t *bar);
/* Block on a barrier until all the processes reach it */
void barrier(mr_barrier_t *bar);
/* We only actually use one barrier - the one in the shared environment */
#define BARRIER() barrier(&mr_shared_env->mr_barrier)

/* 1 if this is the master process */
int master_node;

#endif /* DISTRIBUTED_H_ */
