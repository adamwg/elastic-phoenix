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

#ifndef STRUCT_H_
#define STRUCT_H_

#include "map_reduce.h"
#include "tunables.h"
#include "queue.h"
#include "taskQ.h"

typedef struct _val_t
{
    int                 size;
    int                 next_insert_pos;
    struct _val_t       *next_val;
    void                *array[];
} val_t;

/* A key and an array of values associated with it. */
typedef struct
{
    int len;
    void *key;
    val_t *vals;
} keyvals_t;

/* A key and a value pair. */
typedef struct 
{
    /* TODO add static assertion to make sure this fits in L2 line */
    union {
        struct {
            int         len;
            int         alloc_len;
            int         pos;
            keyval_t    *arr;
        };
        char pad[L2_CACHE_LINE_SIZE];
    };
} keyval_arr_t;

/* Array of keyvals_t. */
typedef struct 
{
    int len;
    int alloc_len;
    int pos;
    keyvals_t *arr;
} keyvals_arr_t;

/* Thread information.
   Denotes the id and the assigned CPU of a thread. */
typedef struct 
{
    union {
        struct {
            pthread_t tid;
            int curr_task;
        };
        char pad[L2_CACHE_LINE_SIZE];
    };
} thread_info_t;

typedef struct
{
    uintptr_t work_time;
    uintptr_t user_time;
    uintptr_t combiner_time;
} thread_timing_t;

typedef struct {
    task_t          task;
    queue_elem_t    queue_elem;
} task_queued;

#endif /* STRUCT_H_ */
