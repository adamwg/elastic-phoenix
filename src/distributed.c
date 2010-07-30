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
	pthread_spin_lock(&bar->lock);
	bar->count += 1;

	if(bar->count < N_WORKERS) {
		pthread_spin_unlock(&bar->lock);
		while(!bar->alldone);
		fetch_and_inc(&bar->exited);
		return;
	} else if(bar->count == N_WORKERS) {
		bar->count = 0;
		bar->alldone = 1;
		while(bar->exited < N_WORKERS - 1);
		pthread_spin_unlock(&bar->lock);
	}
}
