#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "atomic.h"
#include "distributed.h"
#include "stddefines.h"
#include "synch.h"

void shm_init() {
	int fd;

	/* Map the shmem region */
	fd = open(SHM_DEV, O_RDWR);
	CHECK_ERROR(fd < 0);
	shm_base = mmap(SHM_LOC, SHM_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 1*getpagesize());
	CHECK_ERROR(shm_base != SHM_LOC);
}

void barrier_init(mr_barrier_t *bar) {
	pthread_spin_init(&bar->lock, PTHREAD_PROCESS_SHARED);
	bar->count = 0;
	bar->alldone = 0;
	bar->exited = 0;
}

void barrier(mr_barrier_t *bar) {
	/* Lock */
	pthread_spin_lock(&mr_shared_env->bpl);
	pthread_spin_lock(&bar->lock);

	/* Increment the count */
	bar->count += 1;

	if(bar->count <= mr_shared_env->worker_counter * L_NUM_THREADS) {
		/* If not everyone is done, unlock and wait */
		pthread_spin_unlock(&bar->lock);
		while(!bar->alldone);
		/* When we're all done, atomically increment the extied counter */
		__sync_fetch_and_add(&bar->exited, 1);
		return;
	} else if(bar->count == mr_shared_env->worker_counter * L_NUM_THREADS + 1) {
		/* If we're the last one to get here, set the alldone flag */
		bar->alldone = 1;
		/* Wait for everyone else to exit */
		while(bar->exited < mr_shared_env->worker_counter * L_NUM_THREADS);

		/* Reset the barrier */
		bar->count = 0;
		bar->alldone = 0;
		bar->exited = 0;

		/* Unlock so the next barrier can work */
		pthread_spin_unlock(&bar->lock);
		pthread_spin_unlock(&mr_shared_env->bpl);
	}
}
