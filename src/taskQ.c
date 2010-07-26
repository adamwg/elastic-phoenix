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

taskQ_t *tq_init(int num_threads) {
	the_tq = shm_base;
	static_lock_alloc(&the_tq->lock);
	the_tq->head = 0;
	the_tq->tail = 0;

	return the_tq;
}

int tq_enqueue (taskQ_t* tq, task_t *task, int lgrp, int tid) {
	/* Full */
	if(tq->head == tq->tail - 1) {
		return -1;
	}

	/* Acquire lock */
	lock_acquire(&tq->lock);

	/* Copy in the data */
	mem_memcpy(&tq->tasks[tq->head], task, sizeof(task_t));
	tq->head += 1;

	/* Release lock */
	lock_release(&tq->lock);

	return 0;
}

int tq_enqueue_seq (taskQ_t* tq, task_t *task, int lgrp) {
	/* Full */
	if(tq->head == tq->tail - 1) {
		return -1;
	}

	/* Don't need to worry about locking in _seq */
	mem_memcpy(&tq->tasks[tq->head], task, sizeof(task_t));
	tq->head += 1;

	return 0;
}

int tq_dequeue (taskQ_t* tq, task_t *task, int lgrp, int tid) {
	/* Empty */
	if(tq->head == tq->tail) {
		return 0;
	}

	/* Acquire lock */
	lock_acquire(&tq->lock);

	/* Grab the data */
	mem_memcpy(task, &tq->tasks[tq->tail], sizeof(task_t));

	/* Release lock */
	lock_release(&tq->lock);

	return 1;
}

void tq_reset (taskQ_t* tq, int num_threads) {
	lock_acquire(&tq->lock);
	tq->head = tq->tail = 0;
	lock_release(&tq->lock);
}

void tq_finalize (taskQ_t* tq) { }
