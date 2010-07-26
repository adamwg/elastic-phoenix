#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include "synch.h"

#ifdef MR_LOCK_SPIN

static mr_lock_t ptspin_alloc(void)
{
    pthread_spinlock_t *m;
    int             err;

    m = malloc(sizeof(pthread_spinlock_t));
    assert (m != NULL);

    err = pthread_spin_init(m, PTHREAD_PROCESS_SHARED);
    assert (err == 0);

    return (mr_lock_t)m;
}

static void ptspin_acquire(mr_lock_t l)
{
    int err;
    err = pthread_spin_lock(l);
    assert (err == 0);
}

static void ptspin_release(mr_lock_t l)
{
    int err;
    err = pthread_spin_unlock(l);
    assert (err == 0);
}

static void ptspin_free(mr_lock_t l)
{
    pthread_spin_destroy(l);
    free(l);
}

static mr_lock_t ptspin_alloc_per_thread(mr_lock_t l)
{
    return l;
}

static void ptspin_free_per_thread(mr_lock_t l)
{
}

static mr_lock_t ptspin_static_alloc(void *mem)
{
	int err = pthread_spin_init(mem, PTHREAD_PROCESS_SHARED);
	assert(err == 0);

	return (mr_lock_t)mem;
}

static mr_lock_t ptspin_static_alloc_per_thread(void *mem, int thread_id, mr_lock_t l)
{
	return mem;
}

mr_lock_ops mr_ptspin_ops = {
    .alloc = ptspin_alloc,
    .acquire = ptspin_acquire,
    .release = ptspin_release,
    .free = ptspin_free,
    .alloc_per_thread = ptspin_alloc_per_thread,
    .free_per_thread = ptspin_free_per_thread,
	.static_alloc = ptspin_static_alloc,
	.static_alloc_per_thread = ptspin_static_alloc_per_thread,
};

#endif /* MR_LOCK_SPIN */
