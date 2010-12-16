/* This is a brand new taskQ.c that implements queues in shared memory,
 * statically.
 *
 * -- awg
 */

#include <unistd.h>

#include "memory.h"
#include "taskQ.h"
#include "synch.h"

#include "distributed.h"

taskQ_t *the_tq = NULL;

taskQ_t *tq_init() {
	the_tq = shm_base;
	MASTER {
		static_lock_alloc((mr_lock_t)&the_tq->lock);
		the_tq->head = 0;
		the_tq->tail = 0;
	}

	return the_tq;
}

int tq_enqueue (taskQ_t* tq, task_t *task, int lgrp, int tid) {
	/* Acquire lock */
	lock_acquire((mr_lock_t)&tq->lock);

	/* Full */
	if((tq->head + 1) % N_TASKS == tq->tail) {
		lock_release((mr_lock_t)&tq->lock);
		return -1;
	}

	/* Copy in the data */
	mem_memcpy(&tq->tasks[tq->head], task, sizeof(task_t));
	tq->head = (tq->head + 1) % N_TASKS;

	/* Release lock */
	lock_release((mr_lock_t)&tq->lock);

	return 0;
}

int tq_enqueue_seq (taskQ_t* tq, task_t *task, int lgrp) {
	/* Full */
	if((tq->head + 1) % N_TASKS == tq->tail) {
		return -1;
	}

	/* Don't need to worry about locking in _seq */
	mem_memcpy(&tq->tasks[tq->head], task, sizeof(task_t));
	tq->head = (tq->head + 1) % N_TASKS;

	return 0;
}

int tq_dequeue (taskQ_t* tq, task_t *task, int lgrp, int tid) {
	/* Acquire lock */
	lock_acquire((mr_lock_t)&tq->lock);

	/* Empty */
	if(tq->head == tq->tail) {
		lock_release((mr_lock_t)&tq->lock);
		return 0;
	}

	/* Grab the data */
	mem_memcpy(task, &tq->tasks[tq->tail], sizeof(task_t));
	tq->tail = (tq->tail + 1) % N_TASKS;

	/* Release lock */
	lock_release((mr_lock_t)&tq->lock);

	return 1;
}

void tq_reset (taskQ_t* tq) {
	lock_acquire((mr_lock_t)&tq->lock);
	tq->head = tq->tail = 0;
	lock_release((mr_lock_t)&tq->lock);
}

void tq_finalize (taskQ_t* tq) { }
