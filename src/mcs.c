/* Copyright (c) 2007-2009, Stanford University
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * Neither the name of Stanford University nor the names of its 
*       contributors may be used to endorse or promote products derived from 
*       this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/ 


#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "synch.h"
#include "atomic.h"
#ifdef MR_LOCK_MCS

typedef struct mcs_lock_priv {
    //  mcs_lock can be a pointer as it is only accessed by the owner of
    //  this lock not across VMs.
    struct mcs_lock         *mcs_head;
    //struct mcs_lock_priv    *next;
    long                     next;
    int                     thread_id;
    uintptr_t               locked;
} mcs_lock_priv;

typedef struct mcs_lock {
//    mcs_lock_priv    *head;
    // this has to be a 64-bit value due to the atomic operations
    long long head;
} mcs_lock;

static mr_lock_t static_mcs_alloc(void * memptr)
{
    mcs_lock    *l;

    l = memptr;
//    l = malloc(sizeof(mcs_lock));
    l->head = -1;
//    l->head = -1;

    return l;
}

static mr_lock_t static_mcs_alloc_per_thread(void * memptr, int thread_id, mr_lock_t l)
{
    mcs_lock_priv    *priv;

    priv = memptr + sizeof(mcs_lock) + thread_id * sizeof(mcs_lock_priv);
//    priv = malloc(sizeof(mcs_lock_priv));

    priv->mcs_head = l;
    priv->next = 0;
    priv->thread_id = thread_id;
    priv->locked = 0;

    return priv;
}



static mr_lock_t mcs_alloc(void)
{
    mcs_lock    *l;

    l = malloc(sizeof(mcs_lock));
    l->head = NULL;

    return l;
}

static mr_lock_t mcs_alloc_per_thread(mr_lock_t l)
{
    mcs_lock_priv    *priv;

    priv = malloc(sizeof(mcs_lock_priv));

    priv->mcs_head = l;
    priv->next = -1;
    priv->locked = 0;

    return priv;
}

static void mcs_free (mr_lock_t l)
{
    free(l);
}

static void mcs_free_per_thread (mr_lock_t l)
{
    free(l);
}

static void mcs_acquire(mr_lock_t l)
{
    mcs_lock        *mcs;
    mcs_lock_priv   *prev, *priv;
    long offset;
    int prev_num;

    priv = l;
    mcs = priv->mcs_head;

    assert (priv->locked == 0);

    set_and_flush(priv->next, -1);

    prev_num = (int)(atomic_xchg((uintptr_t)priv->thread_id, (void*)(&mcs->head)));
    if (prev_num == -1) {
        /* has exclusive access on lock */
        return;
    }

    /* someone else has lock */

    /* NOTE: this ordering is important-- if locked after next assignment,
     * we may have a schedule that will spin forever */
    set_and_flush(priv->locked, 1);
    offset = (long)priv - (long)mcs;
    prev = (void *)mcs + sizeof(mcs_lock) + sizeof(mcs_lock_priv) * prev_num;
    set_and_flush(prev->next, offset); // use my offset
//    set_and_flush(prev->next, priv);

    while (atomic_read(&priv->locked)) { asm("":::"memory"); }
}

static void mcs_release (mr_lock_t l)
{
    mcs_lock        *mcs;
    mcs_lock_priv   *priv;
    mcs_lock_priv   *next_ptr;
    int *           locked_ptr;

    priv = l;
    mcs = priv->mcs_head;

    if (priv->next == -1) {
        /* set "head" to -1 (our NULL since we're using IDs not pointers */
        if (cmp_and_swp(
            (uintptr_t)-1,
            (void*)(&mcs->head), (uintptr_t)priv->thread_id)) {
            /* we were the only one on the lock, now it's empty */
            return;
        }

        /* wait for next to get thrown on */
        while (((void*)atomic_read(&(priv->next))) == -1) {
            asm("" ::: "memory");
        }
    }

    next_ptr = (void *)mcs + priv->next;
//    set_and_flush(priv->next->locked, 0);
    set_and_flush(next_ptr->locked, 0);
}

mr_lock_ops mr_mcs_ops = {
    .alloc = mcs_alloc,
    .static_alloc = static_mcs_alloc,
    .acquire = mcs_acquire,
    .release = mcs_release,
    .free = mcs_free,
    .alloc_per_thread = mcs_alloc_per_thread,
    .static_alloc_per_thread = static_mcs_alloc_per_thread,
    .free_per_thread = mcs_free_per_thread,
};

#endif /*  MR_LOCK_MCS */
