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

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/unistd.h>
#include <unistd.h>

#include "defines.h"
#include "iterator.h"
#include "locality.h"
#include "map_reduce.h"
#include "memory.h"
#include "processor.h"
#include "queue.h"
#include "scheduler.h"
#include "stddefines.h"
#include "struct.h"
#include "synch.h"
#include "taskQ.h"
#include "tpool.h"
#include "tunables.h"

#include "distributed.h"
#include "shm_alloc.h"

#if !defined(_LINUX_) && !defined(_SOLARIS_)
#error OS not supported
#endif

/* Debug printf */
#ifdef dprintf
#undef dprintf
#define dprintf(...) printf(__VA_ARGS__)
#endif

#define MIN(X,Y) ((X) < (Y) ? (X) : (Y))
#define MAX(X,Y) ((X) > (Y) ? (X) : (Y))
#define OUT_PREFIX "[Phoenix] "

/* Internal map reduce state. */
typedef struct
{
    /* Parameters. */
    int num_map_tasks;              /* # of map tasks. */
    int num_reduce_tasks;           /* # of reduce tasks. */
	int num_merge_threads;          /* # of threads for merge tasks. */
    int chunk_size;                 /* # of units of data for each map task. */
    int num_procs;                  /* # of processors to run on. */
    float key_match_factor;         /* # of values likely to be matched 
                                       to the same key. */

    int intermediate_task_alloc_len;

    /* Callbacks. */
    map_t map;                      /* Map function. */
    reduce_t reduce;                /* Reduce function. */
    partition_t partition;          /* Partition function. */     
    splitter_t splitter;            /* Splitter function. */
    locator_t locator;              /* Locator function. */
    key_cmp_t key_cmp;              /* Key comparator function. */

    /* Structures. */
    map_reduce_args_t * args;       /* Args passed in by the user. */
    thread_info_t * tinfo;          /* Thread information array. */

    keyvals_arr_t **intermediate_vals;
                                    /* Array to send to reduce task. */

    keyval_arr_t *final_vals;       /* Array to send to merge task. */
    keyval_arr_t *merge_vals;       /* Array to send to user. */

    uintptr_t splitter_pos;         /* Tracks position in array_splitter(). */

    /* Policy for mapping threads to cpus. */
    sched_policy    *schedPolicies[TASK_TYPE_TOTAL];

    taskQ_t         *taskQueue;     /* Queues of tasks. */
    tpool_t         *tpool;         /* Thread pool. */

	int worker_id;                  /* The worker ID of this process, used to
									 * offset thread IDs */
} mr_env_t;

#ifdef TIMING
static pthread_key_t emit_time_key;
#endif
static pthread_key_t env_key;       /* Environment for current thread. */

/* Data passed on to each worker thread. */
typedef struct
{
    int             cpu_id;             /* CPU this thread is to run. */
    int             thread_id;          /* Thread index. */
    TASK_TYPE_T     task_type;          /* Assigned task type. */
    int             merge_len;
    keyval_arr_t    *merge_input;
    int             merge_round;
    mr_env_t        *env;
} thread_arg_t;

static inline mr_env_t* env_init (map_reduce_args_t *);
static void env_fini(mr_env_t* env);
static inline void env_print (mr_env_t* env);
static inline void start_workers (mr_env_t* env, thread_arg_t *);
static inline void *start_my_work (thread_arg_t *);
static inline void emit_inline (mr_env_t* env, void *, void *);
static inline mr_env_t* get_env(void);
static inline int getCurrThreadIndex (TASK_TYPE_T);
static inline int getGlobalThreadIndex (mr_env_t *, int);
static inline int getNumTaskThreads (mr_env_t* env, TASK_TYPE_T);
static inline void insert_keyval (
    mr_env_t* env, keyval_arr_t *, void *, void *);
static inline void insert_keyval_merged (
    mr_env_t* env, keyvals_arr_t *, void *, void *);

static int array_splitter (void *, int, map_args_t *, splitter_mem_ops_t *);
static void identity_reduce (void *, iterator_t *itr);
static inline void merge_results (mr_env_t* env, keyval_arr_t*, int);

static void *map_worker (void *);
static void *reduce_worker (void *);
static void *merge_worker (void *);

static int gen_map_tasks (mr_env_t* env);
static int gen_reduce_tasks (mr_env_t* env);

static void split(mr_env_t* mr);
static void map(mr_env_t* mr);
static void reduce(mr_env_t* mr);
static void merge(mr_env_t* mr);

static void USAGE() {
	printf("Dynamic Phoenix arguments:\n"
		   "-h and -v\n"
		   "\tAre we running on the host (-h) or in a VM (-v)?\n"
		   "\tExactly one must be specified.\n"
		   "-m and -w\n"
		   "\tAre we the master (-m) process or a worker (-w)?\n"
		   "\tExactly one must be specified.\n"
		   "-d <name>\n"
		   "\tWhere to access shared memory.  A POSIX memory object name if -h is\n"
		   "\tspecified, or the path the the Nahanni UIO device if -v is specified.\n"
		   "\tRequired.\n"
		   "-s <size>\n"
		   "\tThe size of the shared memory, in megabytes.\n"
		   "\tRequired.\n"
		   "--\n"
		   "\tEnd of Phoenix arguments.\n"
		   "\tAny arguments after -- are given to the application.\n");
}

int 
map_reduce_init (int *argc, char ***argv)
{
	int i;
	int opt;
	int vm = -1;
	char *shm_dev = NULL;
	size_t shm_size = 0;

	master_node = -1;
	
	while((opt = getopt(*argc, *argv, "wmhvd:s:")) != -1) {
		switch(opt) {
		case 'w':
			if(master_node < 0) {
				dprintf("Initializing as a worker.\n");
				master_node = 0;
			} else {
				printf("Must specify exactly one of -w or -m\n");
				USAGE();
				return(-1);
			}
			break;
		case 'm':
			if(master_node < 0) {
				dprintf("Initializing as the master.\n");
				master_node = 1;
			} else {
				printf("Must specify exactly one of -w or -m\n");
				USAGE();
				return(-1);
			}
			break;

		case 'h':
			if(vm < 0) {
				vm = 0;
			} else {
				printf("Must specify exactly one of -v or -h\n");
				USAGE();
				return(-1);
			}
			break;
		case 'v':
			if(vm < 0) {
				vm = 1;
			} else {
				printf("Must specify exactly one of -v or -h\n");
				USAGE();
				return(-1);
			}
			break;

		case 'd':
			shm_dev = optarg;
			break;

		case 's':
			shm_size = atol(optarg);
			if(shm_size < 0) {
				printf("Invalid SHM size '%s'\n", optarg);
				return(-1);
			} else if(shm_size > 131072) {
				printf("Warning: Your SHM seems very large (%lu MB).\n", shm_size);
			}
			break;
			
		default:
			USAGE();
			return(-1);
		}
	}

	/* Check that we got a master or worker argument */
	if(master_node < 0) {
		printf("Must specify one of -w or -m.\n");
		USAGE();
		return(-1);
	}

	if(vm < 0) {
		printf("Must specify one of -v or -h.\n");
		USAGE();
		return(-1);
	}

	if(shm_dev == NULL) {
		printf("Must specify a SHM file or device.\n");
		USAGE();
		return(-1);
	}

	if(shm_size == 0) {
		printf("Must specify a SHM size.\n");
		USAGE();
		return(-1);
	}
	
	/* Map the shared memory and initialize the allocator */
	shm_init(vm, shm_dev, shm_size);
	shm_alloc_init(shm_base + TQ_SIZE + sizeof(mr_shared_env_t),
				   shm_size*1024L*1024L - TQ_SIZE - sizeof(mr_shared_env_t),
				   master_node);
	/* The shared environment lives just after the task queue */
	mr_shared_env = shm_base + TQ_SIZE;

	/* Strip off the arguments we consumed */
	for(i = 0; i < *argc - optind; i++) {
		(*argv)[i+1] = (*argv)[optind+i];
	}
	*argc -= (optind + 1);

	return(0);
}

int
map_reduce(map_reduce_args_t *args) {
	int i;
    struct timeval begin, end;
    mr_env_t* env;

	MASTER {
		mr_shared_env->worker_counter = 0;
		barrier_init(&mr_shared_env->mr_barrier);
		pthread_spin_init(&mr_shared_env->bpl, PTHREAD_PROCESS_SHARED);
		pthread_spin_lock(&mr_shared_env->bpl);
		mr_shared_env->current_stage = TASK_TYPE_MAP;
	}

    assert (args != NULL);
    assert (args->map != NULL);
    assert (args->key_cmp != NULL);
    assert (args->unit_size > 0);
    assert (args->result != NULL);

    get_time (&begin);

	/* Prep - in the master */
	MASTER {
		if(args->prep != NULL) {
			CHECK_ERROR(args->prep(args->task_data) != 0);
		}
	}

    /* Initialize environment. */
    env = env_init (args);
    if (env == NULL) {
       /* could not allocate environment */
       return -1;
    }
    env_print (env);
    env->taskQueue = tq_init ();
    assert (env->taskQueue != NULL);

    /* Create thread pool. */
	env->tpool = tpool_create (L_NUM_THREADS);
	CHECK_ERROR (env->tpool == NULL);

#ifdef TIMING
    CHECK_ERROR (pthread_key_create (&emit_time_key, NULL));
#endif
    CHECK_ERROR (pthread_key_create (&env_key, NULL));
    pthread_setspecific (env_key, env);

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "library init: %u\n", time_diff (&end, &begin));
#endif

	/* Split - in the master thread */
	MASTER {
		mr_shared_env->task_data = shm_alloc(args->task_data_size);
		CHECK_ERROR(mr_shared_env->task_data == NULL);
		memcpy(mr_shared_env->task_data, args->task_data, args->task_data_size);
		
		split (env);
		/* Now we can let workers start, if they've started too soon */
		pthread_spin_unlock(&mr_shared_env->bpl);

		/* The master needs to wait for at least one worker to join before it
		 * falls through to the barrier.
		 */
		while(mr_shared_env->worker_counter == 0);
	}

	/* Copy task data into the workers */
	WORKER {
		memcpy(args->task_data, mr_shared_env->task_data, args->task_data_size);
	}
	
    /* Run map tasks and get intermediate values - in the workers */
	if(mr_shared_env->current_stage == TASK_TYPE_MAP) {
		get_time (&begin);
		WORKER {
			map (env);
		}
		get_time (&end);
#ifdef TIMING
		fprintf (stderr, "map phase: %u\n", time_diff (&end, &begin));
#endif
		BARRIER();

		dprintf("In scheduler, all map tasks are done, now scheduling reduce tasks\n");
		
		MASTER {
			CHECK_ERROR (gen_reduce_tasks (env));
			mr_shared_env->current_stage = TASK_TYPE_REDUCE;
		}
		BARRIER();
	}

	if(mr_shared_env->current_stage == TASK_TYPE_REDUCE) {
		/* Run reduce tasks and get final values - in workers */
		get_time (&begin);
		WORKER {
			reduce (env);
		}
		get_time (&end);
#ifdef TIMING
		fprintf (stderr, "reduce phase: %u\n", time_diff (&end, &begin));
#endif
		dprintf("In scheduler, all reduce tasks are done, now scheduling merge tasks\n");
		BARRIER();
	}

	/* Merge - in the master */
	MASTER {
		/* Cleanup intermediate results. */
		for (i = 0; i < env->intermediate_task_alloc_len; ++i)
		{
			shm_free (env->intermediate_vals[i]);
		}
		shm_free (env->intermediate_vals);
		/* Do the merge */
		get_time (&begin);
		merge (env);
		get_time (&end);
#ifdef TIMING
		fprintf (stderr, "merge phase: %u\n", time_diff (&end, &begin));
#endif
	}
	BARRIER();

	WORKER {
		env->merge_vals = mr_shared_env->merge_vals;
	}
	env->args->result->length = env->merge_vals[0].len;
	env->args->result->data = env->merge_vals[0].arr;

    /* Cleanup. */
    get_time (&begin);
    env_fini(env);
    CHECK_ERROR (tpool_destroy (env->tpool));
    CHECK_ERROR (pthread_key_delete (env_key));
    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "library finalize: %u\n", time_diff (&end, &begin));
    CHECK_ERROR (pthread_key_delete (emit_time_key));
#endif

    return 0;
}

int map_reduce_finalize () {
	return 0;
}

/* This should be called when the application no longer needs data passed back
 * from a mapreduce job. */
void map_reduce_cleanup (map_reduce_args_t *args) {
	MASTER {
		if(args->result->data != NULL) {
			shm_free(args->result->data);
			args->result->data = NULL;
		}
		if(args->cleanup != NULL) {
			args->cleanup(args->task_data);
		}
	}
}

/**
 * Frees memory used by map reduce environment once map reduce has completed.
 * Frees environment pointer.
 */
static void env_fini (mr_env_t* env)
{
    int i;

    tq_finalize (env->taskQueue);

    for (i = 0; i < TASK_TYPE_TOTAL; i++)
        sched_policy_put(env->schedPolicies[i]);

    mem_free (env);
}

/* Setup global state. */
static mr_env_t* 
env_init (map_reduce_args_t *args) 
{
    mr_env_t    *env;
    int         i;
    int         num_procs;
	int         num_units;

    env = mem_malloc (sizeof (mr_env_t));
    if (env == NULL) {
       return NULL;
    }

    mem_memset (env, 0, sizeof (mr_env_t));

    env->args = args;

	/* Acquire a worker id */
	WORKER {
		/* We need to lock around this so that the barrier doesn't change
		   halfway through */
		pthread_spin_lock(&mr_shared_env->bpl);
		/* Since we lock, this doesn't need to be an atomic op */
		env->worker_id = mr_shared_env->worker_counter++;
		pthread_spin_unlock(&mr_shared_env->bpl);
		if((env->worker_id + 1) * L_NUM_THREADS > MAX_WORKER_THREADS) {
			printf("Cannot join mapreduce -- too many workers.\n");
			return NULL;
		}
	}
	
    /* 1. Determine paramenters. */

    /* Determine the number of processors to use. */
    num_procs = proc_get_num_cpus ();
    if (args->num_procs > 0)
    {
        /* Can't have more processors than there are physically present. */
        CHECK_ERROR (args->num_procs > num_procs);
        num_procs = args->num_procs;
    }
    env->num_procs = num_procs;

	MASTER {
		/* Only the master merges */
		env->num_merge_threads = (mr_shared_env->worker_counter * L_NUM_THREADS) / 2;
		/* Assign at least one merge thread. */
		env->num_merge_threads = MAX(env->num_merge_threads, 1);
		
		env->key_match_factor = (args->key_match_factor > 0) ? 
			args->key_match_factor : 2;
	}
	WORKER {
		env->num_merge_threads = 0;
	}

    /* Set num_map_tasks to 0 since we cannot anticipate how many map tasks
       there would be. This does not matter since map workers will loop
       until there is no more data left. */
    env->num_map_tasks = 0;

	/* Map one unit at a time by default. */
	env->chunk_size = 1;
	/* But this might result in a lot of map tasks, in which case do enough to
	 * fill but not overflow our task queue. */
	num_units = args->data_size / args->unit_size;
	if(num_units > N_TASKS - 1) {
		env->chunk_size = num_units / (N_TASKS - 1) + 1;
		printf("There are %d units.  Do %d at a time.\n", num_units, env->chunk_size);
	}

	env->num_reduce_tasks = NUM_REDUCE_TASKS;
    env->num_merge_threads = MIN (
		env->num_merge_threads, env->num_reduce_tasks / 2);
	env->intermediate_task_alloc_len = MAX_WORKER_THREADS;

    /* Register callbacks. */
    env->map = args->map;
    env->reduce = (args->reduce) ? args->reduce : identity_reduce;
    env->partition = (args->partition) ? args->partition : default_partition;
    env->splitter = (args->splitter) ? args->splitter : array_splitter;
    env->locator = args->locator;
    env->key_cmp = args->key_cmp;

    /* 2. Initialize structures. */

    dprintf("%d * %d\n", env->intermediate_task_alloc_len, env->num_reduce_tasks);

	MASTER {
		env->intermediate_vals = (keyvals_arr_t **)shm_alloc (
			env->intermediate_task_alloc_len * sizeof (keyvals_arr_t*));
		CHECK_ERROR(env->intermediate_vals == NULL);
		
		for (i = 0; i < env->intermediate_task_alloc_len; i++)
		{
			env->intermediate_vals[i] = (keyvals_arr_t *)shm_alloc (
				env->num_reduce_tasks * sizeof (keyvals_arr_t));
			CHECK_ERROR(env->intermediate_vals[i] == NULL);
			memset(env->intermediate_vals[i], 0, env->num_reduce_tasks * sizeof (keyvals_arr_t));
		}

		mr_shared_env->intermediate_vals = env->intermediate_vals;
		
		env->final_vals = (keyval_arr_t *)shm_alloc (MAX_WORKER_THREADS * sizeof (keyval_arr_t));
		CHECK_ERROR(env->final_vals == NULL);
		memset(env->final_vals, 0, MAX_WORKER_THREADS * sizeof(keyval_arr_t));
		
		mr_shared_env->final_vals = env->final_vals;
	}
	WORKER {
		env->intermediate_vals = mr_shared_env->intermediate_vals;
		env->final_vals = mr_shared_env->final_vals;
	}

    for (i = 0; i < TASK_TYPE_TOTAL; i++) {
        /* TODO: Make this tunable */
        env->schedPolicies[i] = sched_policy_get(SCHED_POLICY_STRAND_FILL);
    }

    return env;
}

void env_print (mr_env_t* env)
{
    printf (OUT_PREFIX "num_reduce_tasks = %u\n", env->num_reduce_tasks);
    printf (OUT_PREFIX "num_procs = %u\n", env->num_procs);
    printf (OUT_PREFIX "proc_offset = %u\n", env->args->proc_offset);
    printf (OUT_PREFIX "num_merge_threads = %u\n", env->num_merge_threads);
	printf (OUT_PREFIX "worker_id = %d\n", env->worker_id);
}

/**
 * Execute the same function as worker.
 * @param th_arg    arguments
 */
static void *start_my_work (thread_arg_t* th_arg)
{
    switch (th_arg->task_type) {
    case TASK_TYPE_MAP:
        return map_worker (th_arg);
        break;
    case TASK_TYPE_REDUCE:
        return reduce_worker (th_arg);
        break;
    case TASK_TYPE_MERGE:
        return merge_worker (th_arg);
        break;
    default:
        assert (0);
        break;
    }
}

static void start_thread_pool (
    tpool_t *tpool, TASK_TYPE_T task_type, thread_arg_t** th_arg_array, int num_workers)
{
    thread_func     thread_func;

    switch (task_type) {
    case TASK_TYPE_MAP:
        thread_func = map_worker;
        break;
    case TASK_TYPE_REDUCE:
        thread_func = reduce_worker;
        break;
    case TASK_TYPE_MERGE:    
        thread_func = merge_worker;
        break;
    default:
        assert (0);
        break;
    }

    CHECK_ERROR (tpool_set (tpool, thread_func, (void **)th_arg_array, num_workers));
    CHECK_ERROR (tpool_begin (tpool));
}

/** start_workers()
 *  thread_func - function pointer to process splitter data
 *  splitter_func - splitter function pointer
 *  splitter_init - splitter_init function pointer
 *  runs map tasks in a new thread on each the available processors.
 *  returns pointer intermediate value array 
 */
static void 
start_workers (mr_env_t* env, thread_arg_t *th_arg)
{
    int             thread_index;
    TASK_TYPE_T     task_type;
    int             num_threads;
    int             cpu;
    intptr_t        ret_val;
    thread_arg_t    **th_arg_array;
    void            **rets;
#ifdef TIMING
    uint64_t        work_time = 0;
    uint64_t        user_time = 0;
#endif

    assert(th_arg != NULL);

    task_type = th_arg->task_type;
    num_threads = getNumTaskThreads (env, task_type);

    env->tinfo = (thread_info_t *)mem_calloc (
        num_threads, sizeof (thread_info_t));
    th_arg->env = env;

    th_arg_array = (thread_arg_t **)mem_malloc (
        sizeof (thread_arg_t *) * num_threads);
    CHECK_ERROR (th_arg_array == NULL);

    for (thread_index = 0; thread_index < num_threads; ++thread_index) {

        cpu = sched_thr_to_cpu (env->schedPolicies[task_type], thread_index + env->args->proc_offset);
        th_arg->cpu_id = cpu;
		/* Globally unique thread ID */
        th_arg->thread_id = thread_index;

        th_arg_array[thread_index] = mem_malloc (sizeof (thread_arg_t));
        CHECK_ERROR (th_arg_array[thread_index] == NULL);
        mem_memcpy (th_arg_array[thread_index], th_arg, sizeof (thread_arg_t));
    }

    start_thread_pool (
        env->tpool, task_type, &th_arg_array[1], num_threads - 1);

    dprintf("Status: All %d threads have been created\n", num_threads);

    ret_val = (intptr_t)start_my_work (th_arg_array[0]);
#ifdef TIMING
    thread_timing_t *timing = (thread_timing_t *)ret_val;
    work_time += timing->work_time;
    user_time += timing->user_time;
    mem_free (timing);
#endif
    mem_free (th_arg_array[0]);

    /* Barrier, wait for all threads to finish. */
    CHECK_ERROR (tpool_wait (env->tpool));
    rets = tpool_get_results (env->tpool);

    for (thread_index = 1; thread_index < num_threads; ++thread_index)
    {
#ifdef TIMING
        ret_val = (intptr_t)rets[thread_index - 1];
        thread_timing_t *timing = (thread_timing_t *)ret_val;
        work_time += timing->work_time;
        user_time += timing->user_time;
        mem_free (timing);
#endif
        mem_free (th_arg_array[thread_index]);
    }

    mem_free (th_arg_array);
    mem_free (rets);

#ifdef TIMING
    switch (task_type)
    {
        case TASK_TYPE_MAP:
            fprintf (stderr, "map work time: %" PRIu64 "\n",
                                        work_time / num_threads);
            fprintf (stderr, "map user time: %" PRIu64 "\n", 
                                        user_time / num_threads);
            break;

        case TASK_TYPE_REDUCE:
            fprintf (stderr, "reduce work time: %" PRIu64 "\n",
                                        work_time / num_threads);
            fprintf (stderr, "reduce user time: %" PRIu64 "\n", 
                                        user_time / num_threads);
            break;

        case TASK_TYPE_MERGE:
            fprintf (stderr, "merge work time: %" PRIu64 "\n",
                                        work_time / num_threads);

        default:
            break;
    }
#endif

    mem_free(env->tinfo);
    dprintf("Status: All tasks have completed\n"); 
}

typedef struct {
    uint64_t            run_time;
    int                 lgrp;
} map_worker_task_args_t;

/**
 * Dequeue the latest task and run it
 * @return true if ran a task, false otherwise
 */
static bool map_worker_do_next_task (
    mr_env_t *env, int thread_index, map_worker_task_args_t *args)
{
    struct timeval  begin, end;
    int             alloc_len;
    int             curr_task;
    task_t          map_task;
    map_args_t      thread_func_arg;

    alloc_len = env->intermediate_task_alloc_len;

    /* Get new map task. */
    if (tq_dequeue (env->taskQueue, &map_task) == 0) {
        /* no more map tasks */
        return false;
    }

    curr_task = env->num_map_tasks++;
    env->tinfo[thread_index].curr_task = curr_task;

    thread_func_arg.length = map_task.len;
    thread_func_arg.data = (void *)map_task.data;

//    dprintf("Task %d: - Started\n", curr_task);

    /* Perform map task. */
    get_time (&begin);
    env->map (&thread_func_arg);
    get_time (&end);

#ifdef TIMING
    args->run_time = time_diff (&end, &begin);
#endif

//    dprintf("Task %d: - Done\n", curr_task);

    return true;
}

/** 
 * map_worker()
 * args - pointer to thread_arg_t
 * returns 0 on success
 * This runs thread_func() until there is no more data from the splitter().
 * The pointer to results are stored in return_values array.
 */
static void *
map_worker (void *args)
{
    assert (args != NULL);

    struct timeval          work_begin, work_end;
    uintptr_t               user_time = 0;
    thread_arg_t            *th_arg = (thread_arg_t *)args;
    mr_env_t                *env = th_arg->env;
    int                     thread_index = th_arg->thread_id;
    int                     num_assigned = 0;
    map_worker_task_args_t  mwta;
#ifdef TIMING
    uintptr_t               work_time = 0;
#endif

    env->tinfo[thread_index].tid = pthread_self();

    /* Bind thread. */
    CHECK_ERROR (proc_bind_thread (th_arg->cpu_id) != 0);

    CHECK_ERROR (pthread_setspecific (env_key, env));
#ifdef TIMING
    CHECK_ERROR (pthread_setspecific (emit_time_key, 0));
#endif

    mwta.lgrp = loc_get_lgrp();

    get_time (&work_begin);
    while (map_worker_do_next_task (env, thread_index, &mwta)) {
        user_time += mwta.run_time;
        num_assigned++;
    }
    get_time (&work_end);

#ifdef TIMING
    work_time = time_diff (&work_end, &work_begin);
#endif

    dprintf("Status: Total of %d tasks were assigned to cpu_id %d\n", 
        num_assigned, th_arg->cpu_id);

    /* Unbind thread. */
    CHECK_ERROR (proc_unbind_thread () != 0);

#ifdef TIMING
    thread_timing_t *timing = calloc (1, sizeof (thread_timing_t));
    uintptr_t emit_time = (uintptr_t)pthread_getspecific (emit_time_key);
    timing->user_time = user_time - emit_time;
    timing->work_time = work_time - timing->user_time;
    return (void *)timing;
#else
    return (void *)0;
#endif
}

typedef struct {
    struct iterator_t   itr;
    uint64_t            run_time;
    int                 num_map_threads;
    int                 lgrp;
} reduce_worker_task_args_t;

/**
 * Dequeue next reduce task and do it
 * TODO Refactor this even more. It is still gross.
 * @return true if did work, false otherwise
 */
static bool reduce_worker_do_next_task (
    mr_env_t *env, int thread_index, reduce_worker_task_args_t *args)
{
    struct timeval  begin, end;
    intptr_t        curr_reduce_task = 0;
    keyvals_t       *min_key_val, *next_min;
    task_t          reduce_task;
    int             num_map_threads;
    int             curr_thread;

    /* Get the next reduce task. */
    if (tq_dequeue (env->taskQueue, &reduce_task) == 0) {
        /* No more reduce tasks. */
        return false;
    }

    curr_reduce_task = (intptr_t)reduce_task.id;

    env->tinfo[thread_index].curr_task = curr_reduce_task;

    num_map_threads =  args->num_map_threads;

    args->run_time = 0;
    min_key_val = NULL;
    next_min = NULL;

    do {
        for (curr_thread = 0; curr_thread < num_map_threads; curr_thread++) {
            keyvals_t       *curr_key_val;
            keyvals_arr_t   *thread_array;

            /* Find the next array to search. */
            thread_array = 
                &env->intermediate_vals[curr_thread][curr_reduce_task];

            /* Check if the current processor array has been 
               completely searched. */
            if (thread_array->pos >= thread_array->len) {
                continue;
            }

            /* Get the next key in the processor array. */
            curr_key_val = &thread_array->arr[thread_array->pos];

            /* If the key matches the minimum value, 
               add the value to the list of values for that key. */
            if (min_key_val != NULL &&
                !env->key_cmp(curr_key_val->key, min_key_val->key)) {
                CHECK_ERROR (iter_add (&args->itr, curr_key_val));
                thread_array->pos += 1;
                --curr_thread;
            }
            /* Find the location of the next min. */
            else if (next_min == NULL || 
                env->key_cmp(curr_key_val->key, next_min->key) < 0)
            {
                next_min = curr_key_val;
            }
        }

        if (min_key_val != NULL) {
            keyvals_t       *curr_key_val;

            if (env->reduce != identity_reduce) {
                get_time (&begin);
                env->reduce (min_key_val->key, &args->itr);
                get_time (&end);
#ifdef TIMING
                args->run_time += time_diff (&end, &begin);
#endif
            } else {
                env->reduce (min_key_val->key, &args->itr);
            }

            /* Free up memory */
            iter_rewind (&args->itr);
            while (iter_next_list (&args->itr, &curr_key_val)) {
                val_t   *vals, *next;

                vals = curr_key_val->vals;
                while (vals != NULL) {
                    next = vals->next_val;
                    shm_free (vals);
                    vals = next;
                }
            }

            iter_reset(&args->itr);
        }

        min_key_val = next_min;
        next_min = NULL;

        /* See if there are any elements left. */
        for(curr_thread = 0;
            curr_thread < num_map_threads &&
            env->intermediate_vals[curr_thread][curr_reduce_task].pos >=
            env->intermediate_vals[curr_thread][curr_reduce_task].len;
            curr_thread++);
    } while (curr_thread != num_map_threads);

    /* Free up the memory. */
    for (curr_thread = 0; curr_thread < num_map_threads; curr_thread++) {
        keyvals_arr_t   *arr;

        arr = &env->intermediate_vals[curr_thread][curr_reduce_task];
        if (arr->alloc_len != 0) {
            shm_free(arr->arr);
			arr->alloc_len = 0;
		}
    }

    return true;
}

static void *
reduce_worker (void *args)
{
    assert(args != NULL);

    struct timeval              work_begin, work_end;
    uintptr_t                   user_time = 0;
    thread_arg_t                *th_arg = (thread_arg_t *)args;
    int                         thread_index = th_arg->thread_id;
    mr_env_t                    *env = th_arg->env;
    reduce_worker_task_args_t   rwta;
    int                         num_map_threads;
	int                         num_assigned = 0;
#ifdef TIMING
    uintptr_t                   work_time = 0;
#endif

    env->tinfo[thread_index].tid = pthread_self();

    /* Bind thread. */
    CHECK_ERROR (proc_bind_thread (th_arg->cpu_id) != 0);

    CHECK_ERROR (pthread_setspecific (env_key, env));
#ifdef TIMING
    CHECK_ERROR (pthread_setspecific (emit_time_key, 0));
#endif

	num_map_threads = MAX_WORKER_THREADS;

    /* Assuming !oneOutputQueuePerMapTask */
    CHECK_ERROR (iter_init (&rwta.itr, num_map_threads));
    rwta.num_map_threads = num_map_threads;
    rwta.lgrp = loc_get_lgrp();

    get_time (&work_begin);

    while (reduce_worker_do_next_task (env, thread_index, &rwta)) {
        user_time += rwta.run_time;
		++num_assigned;
    }

    get_time (&work_end);

#ifdef TIMING
    work_time = time_diff (&work_end, &work_begin);
#endif
	dprintf("Status: Total of %d tasks were assigned to cpu_id %d\n", 
			num_assigned, th_arg->cpu_id);

    iter_finalize (&rwta.itr);

    /* Unbind thread. */
    CHECK_ERROR (proc_unbind_thread () != 0);

#ifdef TIMING
    thread_timing_t *timing = calloc (1, sizeof (thread_timing_t));
    uintptr_t emit_time = (uintptr_t)pthread_getspecific (emit_time_key);
    timing->user_time = user_time - emit_time;
    timing->work_time = work_time - timing->user_time;
    return (void *)timing;
#else
    return (void *)0;
#endif
}

/** merge_worker()
* args - pointer to thread_arg_t
* returns 0 on success
*/
static void *
merge_worker (void *args) 
{
    assert(args != NULL);

    struct timeval  work_begin, work_end;
    thread_arg_t    *th_arg = (thread_arg_t *)args;
    int             thread_index = th_arg->thread_id;
	int             cpu;
    mr_env_t        *env = th_arg->env;
#ifdef TIMING
    uintptr_t       work_time = 0;
#endif

    env->tinfo[thread_index].tid = pthread_self();

    /* Bind thread.
       Spread out the merge workers as much as possible. */
	cpu = th_arg->cpu_id;
    CHECK_ERROR (proc_bind_thread (cpu) != 0);
	CHECK_ERROR (pthread_setspecific (env_key, env));

    /* Assumes num_merge_threads is modified before each call. */
    int length = th_arg->merge_len / env->num_merge_threads;
    int modlen = th_arg->merge_len % env->num_merge_threads;

    /* Let's make some progress here. */
    if (length <= 1) {
        length = 2;
        modlen = th_arg->merge_len % 2;
    }

    int pos = thread_index * length + 
                ((thread_index < modlen) ? thread_index : modlen);

    if (pos < th_arg->merge_len) {

        keyval_arr_t *vals = &th_arg->merge_input[pos];

        dprintf("Thread %d: cpu_id -> %d - Started\n", 
				thread_index, th_arg->cpu_id);

        get_time (&work_begin);
        merge_results (th_arg->env, vals, length + (thread_index < modlen));
        get_time (&work_end);

#ifdef TIMING
        work_time = time_diff (&work_end, &work_begin);
#endif

        dprintf("Thread %d: cpu_id -> %d - Done\n", 
                    thread_index, th_arg->cpu_id);
    }

    /* Unbind thread. */
    CHECK_ERROR (proc_unbind_thread () != 0);

#ifdef TIMING
    thread_timing_t *timing = calloc (1, sizeof (thread_timing_t));
    timing->work_time = work_time;
    return (void *)timing;
#else
    return (void *)0;
#endif
}

/**
 * Generate all map tasks and queue them up
 * @return number of map tasks created if successful, negative value on error
 */
static int gen_map_tasks (mr_env_t* env)
{
    map_args_t          args;
    task_t              task;
	int                 cur_task_id;

    tq_reset (env->taskQueue);
	static splitter_mem_ops_t mops = { .alloc = &shm_alloc, .free = &shm_free };

    /* split until complete */
    cur_task_id = 0;
	env->splitter_pos = 0;
    while (env->splitter (env->args->task_data, env->chunk_size, &args, &mops) > 0)
    {
        task.id = cur_task_id;
		task.data = (uint64_t)args.data;
        task.len = (uint64_t)args.length;
        if(tq_enqueue_seq (env->taskQueue, &task) < 0) {
			printf("Task queue full at %d tasks!\n", cur_task_id);
			return -1;
		}

        ++cur_task_id;
    }

	return cur_task_id;
}

static int gen_reduce_tasks (mr_env_t* env)
{
    int ret;
    int num_reduce_tasks;
    uint64_t task_id;
    task_t reduce_task;

    tq_reset (env->taskQueue);

    num_reduce_tasks = env->num_reduce_tasks;

    task_id = 0;
	while(num_reduce_tasks > 0) {
		/* New task. */
		reduce_task.id = task_id;
		
		ret = tq_enqueue_seq (env->taskQueue, &reduce_task);
		if (ret < 0) {
			return -1;
		}
		++task_id;
		--num_reduce_tasks;
	}

    return 0;
}

/** emit_intermediate()
 *  inserts the key, val pair into the intermediate array
 */
void 
emit_intermediate (void *key, void *val, int key_size)
{
    struct timeval  begin, end;
    static __thread int curr_thread = -1;
    keyvals_arr_t   *arr;
    mr_env_t        *env;

    get_time (&begin);

    env = get_env();
    if (curr_thread < 0) {
		curr_thread = getGlobalThreadIndex(env, getCurrThreadIndex (TASK_TYPE_MAP));
	}
	
    int reduce_pos = env->partition (env->num_reduce_tasks, key, key_size);
    reduce_pos %= env->num_reduce_tasks;

    /* Insert sorted in global queue at pos curr_proc */
    arr = &env->intermediate_vals[curr_thread][reduce_pos];

    insert_keyval_merged (env, arr, key, val);

    get_time (&end);

#ifdef TIMING
    uintptr_t total_emit_time = (uintptr_t)pthread_getspecific (emit_time_key);
    uintptr_t emit_time = time_diff (&end, &begin);
    total_emit_time += emit_time;
    CHECK_ERROR (pthread_setspecific (emit_time_key, (void *)total_emit_time));
#endif
}

/** emit_inline ()
 *  inserts the key, val pair into the final output array
 */
static inline void 
emit_inline (mr_env_t* env, void *key, void *val)
{
    keyval_arr_t    *arr;
    int             curr_red_queue;
    static __thread int thread_index = -1;
	int g_thread_index;

    if (thread_index < 0)
        thread_index = getCurrThreadIndex (TASK_TYPE_REDUCE);
	g_thread_index = getGlobalThreadIndex(env, thread_index);

	curr_red_queue = g_thread_index;

    /* Insert sorted in global queue at pos curr_proc */
    arr = &env->final_vals[curr_red_queue];
    insert_keyval (env, arr, key, val);
}

/** emit ()
 */
void
emit (void *key, void *val)
{
    struct timeval begin, end;

    get_time (&begin);

    emit_inline (get_env(), key, val);

    get_time (&end);

#ifdef TIMING
    uintptr_t total_emit_time = (uintptr_t)pthread_getspecific (emit_time_key);
    uintptr_t emit_time = time_diff (&end, &begin);
    total_emit_time += emit_time;
    CHECK_ERROR (pthread_setspecific (emit_time_key, (void *)total_emit_time));
#endif
}

static inline void 
insert_keyval_merged (mr_env_t* env, keyvals_arr_t *arr, void *key, void *val)
{
    int high = arr->len, low = -1, next;
    int cmp = 1;
    keyvals_t *insert_pos;
    val_t *new_vals;

    assert(arr->len <= arr->alloc_len);
    if (arr->len > 0)
        cmp = env->key_cmp(arr->arr[arr->len - 1].key, key);

    if (cmp > 0)
    {
        /* Binary search the array to find the key. */
        while (high - low > 1)
        {
            next = (high + low) / 2;
            if (env->key_cmp(arr->arr[next].key, key) > 0)
                high = next;
            else
                low = next;
        }

        if (low < 0) low = 0;
        if (arr->len > 0 &&
                (cmp = env->key_cmp(arr->arr[low].key, key)) < 0)
            low++;
    }
    else if (cmp < 0)
        low = arr->len;
    else
        low = arr->len-1;

    if (arr->len == 0 || cmp)
    {
        /* if array is full, double and copy over. */
        if (arr->len == arr->alloc_len)
        {
            if (arr->alloc_len == 0)
            {
                arr->alloc_len = DEFAULT_KEYVAL_ARR_LEN;
                arr->arr = (keyvals_t *)
                    shm_alloc (arr->alloc_len * sizeof (keyvals_t));
				CHECK_ERROR(arr->arr == NULL);
				memset(arr->arr, 0, arr->alloc_len * sizeof(keyvals_t));
            }
            else
            {
                arr->alloc_len *= 2;
                arr->arr = (keyvals_t *)
                    shm_realloc (arr->arr, arr->alloc_len * sizeof (keyvals_t));
            }
        }

        /* Insert into array. */
        memmove (&arr->arr[low+1], &arr->arr[low], 
                        (arr->len - low) * sizeof(keyvals_t));

        arr->arr[low].key = key;
        arr->arr[low].len = 0;
        arr->arr[low].vals = NULL;
        arr->len++;
    }

    insert_pos = &(arr->arr[low]);

    if (insert_pos->vals == NULL)
    {
        /* Allocate a chunk for the first time. */
        new_vals = shm_alloc 
            (sizeof (val_t) + DEFAULT_VALS_ARR_LEN * sizeof (void *));
        assert (new_vals);
		memset(new_vals, 0, sizeof(val_t) + DEFAULT_VALS_ARR_LEN * sizeof(void *));

        new_vals->size = DEFAULT_VALS_ARR_LEN;
        new_vals->next_insert_pos = 0;
        new_vals->next_val = NULL;

        insert_pos->vals = new_vals;
    }
    else if (insert_pos->vals->next_insert_pos >= insert_pos->vals->size)
    {
		/* Need a new chunk. */
		int alloc_size;
		
		alloc_size = insert_pos->vals->size * 2;
		new_vals = shm_alloc (sizeof (val_t) + alloc_size * sizeof (void *));
		assert (new_vals);
		memset(new_vals, 0, sizeof(val_t) + alloc_size * sizeof(void *));
		
		new_vals->size = alloc_size;
		new_vals->next_insert_pos = 0;
		new_vals->next_val = insert_pos->vals;
		
		insert_pos->vals = new_vals;
    }

    insert_pos->vals->array[insert_pos->vals->next_insert_pos++] = val;

    insert_pos->len += 1;
}

static inline void 
insert_keyval (mr_env_t* env, keyval_arr_t *arr, void *key, void *val)
{
    int high = arr->len, low = -1, next;
    int cmp = 1;

    assert(arr->len <= arr->alloc_len);

    /* If array is full, double and copy over. */
    if (arr->len == arr->alloc_len)
    {
        if (arr->alloc_len == 0)
        {
            arr->alloc_len = DEFAULT_KEYVAL_ARR_LEN;
            arr->arr = (keyval_t*)shm_alloc(arr->alloc_len * sizeof(keyval_t));
			CHECK_ERROR(arr->arr == NULL);
			memset(arr->arr, 0, arr->alloc_len * sizeof(keyval_t));
        }
        else
        {
            arr->alloc_len *= 2;
            arr->arr = (keyval_t*)shm_realloc(arr->arr, arr->alloc_len * sizeof(keyval_t));
        }
    }

	/* Need to sort. */
	if (arr->len > 0)
		cmp = env->key_cmp(arr->arr[arr->len - 1].key, key);
	
	if (cmp > 0)
	{
		/* Binary search the array to find the key. */
		while (high - low > 1)
		{
			next = (high + low) / 2;
			if (env->key_cmp(arr->arr[next].key, key) > 0)
				high = next;
			else
				low = next;
		}
		
		if (low < 0) low = 0;
		if (arr->len > 0 && env->key_cmp(arr->arr[low].key, key) < 0)
			low++;
	}
	else
		low = arr->len;
	
	
	/* Insert into array. */
	memmove (&arr->arr[low+1], &arr->arr[low], 
			 (arr->len - low) * sizeof(keyval_t));
	
	arr->arr[low].key = key;
	arr->arr[low].val = val;
    arr->len++;
}

static inline void 
merge_results (mr_env_t* env, keyval_arr_t *vals, int length) 
{
    int data_idx;
    int total_num_keys = 0;
    int i;
    static __thread int curr_thread = -1;
	int g_curr_thread;
    
    if (curr_thread < 0)
        curr_thread = getCurrThreadIndex (TASK_TYPE_MERGE);
	
	g_curr_thread = getGlobalThreadIndex(env, curr_thread);

    for (i = 0; i < length; i++) {
        total_num_keys += vals[i].len;
    }

    env->merge_vals[g_curr_thread].len = total_num_keys;
    env->merge_vals[g_curr_thread].alloc_len = total_num_keys;
    env->merge_vals[g_curr_thread].pos = 0;
    env->merge_vals[g_curr_thread].arr = (keyval_t *)
        shm_alloc(sizeof(keyval_t) * total_num_keys);
	CHECK_ERROR(env->merge_vals[g_curr_thread].arr == NULL);
	memset(env->merge_vals[g_curr_thread].arr, 0, sizeof(keyval_t) * total_num_keys);

    for (data_idx = 0; data_idx < total_num_keys; data_idx++) {
        /* For each keyval_t. */
        int         min_idx;
        keyval_t    *min_keyval;

        for (i = 0; i < length && vals[i].pos >= vals[i].len; i++);

        if (i == length) break;

        /* Determine the minimum key. */
        min_idx = i;
        min_keyval = &vals[i].arr[vals[i].pos];

        for (i++; i < length; i++) {
            if (vals[i].pos < vals[i].len)
            {
                int cmp_ret;
                cmp_ret = env->key_cmp(
                    min_keyval->key, 
                    vals[i].arr[vals[i].pos].key);

                if (cmp_ret > 0) {
                    min_idx = i;
                    min_keyval = &vals[i].arr[vals[i].pos];
                }
            }
        }

        mem_memcpy (&env->merge_vals[g_curr_thread].arr[data_idx], 
                        min_keyval, sizeof(keyval_t));
        vals[min_idx].pos += 1;
    }
}

static inline int 
getNumTaskThreads (mr_env_t* env, TASK_TYPE_T task_type)
{
    int num_threads;

    switch (task_type)
    {
        case TASK_TYPE_MAP:
            num_threads = L_NUM_THREADS;
            break;

        case TASK_TYPE_REDUCE:
            num_threads = L_NUM_THREADS;
            break;

        case TASK_TYPE_MERGE:
            num_threads = env->num_merge_threads;
            break;

        default:
            assert (0);
            num_threads = -1;
            break;
    }

    return num_threads;
}

static inline int
getGlobalThreadIndex(mr_env_t *env, int local_tid) {
	return (env->worker_id * L_NUM_THREADS) + local_tid;
}

/** getCurrThreadIndex()
 *  Returns the processor the current thread is running on
 */
static inline int 
getCurrThreadIndex (TASK_TYPE_T task_type)
{
    int         i;
    int         num_threads;
    mr_env_t    *env;
    pthread_t   mytid;

    env = get_env();
    num_threads = getNumTaskThreads (env, task_type);
    mytid = pthread_self();
    for (i = 0; i < num_threads && env->tinfo[i].tid != mytid; i++);

    assert(i != num_threads);

    return i;
}

/** array_splitter()
 *
 * Note: In the modified API for shared memory, this function should not be
 * used on mmaped files.  If your data is already (really) in memory then it's
 * no slower than before, but if your data is in a file, it's better to
 * provide a splitter that reads directly into shmem (see wordcount_splitter
 * for an example).
*/
int 
array_splitter (void *data_in, int req_units, map_args_t *out, splitter_mem_ops_t *mem)
{
    assert(out != NULL);

    mr_env_t    *env;
    int         unit_size;

    env = get_env();
    unit_size = env->args->unit_size;

    /* End of data reached, return FALSE. */
    if (env->splitter_pos >= env->args->data_size)
        return 0;

	/* Determine the length. */
    if (env->splitter_pos + (req_units * unit_size) > env->args->data_size)
        out->length = env->args->data_size - env->splitter_pos;
    else
        out->length = req_units * unit_size;

    /* Set the start of the next data. */
    out->data = mem->alloc(out->length);
	CHECK_ERROR(out->data == NULL);
	mem_memcpy(out->data,
			   ((char *)data_in) + env->splitter_pos,
			   out->length);

    env->splitter_pos += out->length;

    /* Return true since the out data is valid. */
    return 1;
}

void 
identity_reduce (void *key, iterator_t *itr)
{
    void        *val;
    mr_env_t    *env;

    env = get_env();
    while (iter_next (itr, &val))
    {
        emit_inline (env, key, val);
    }
}

int 
default_partition (int num_reduce_tasks, void* key, int key_size)
{
    unsigned long hash = 5381;
    char *str = (char *)key;
    int i;

    for (i = 0; i < key_size; i++)
    {
        hash = ((hash << 5) + hash) + ((int)str[i]); /* hash * 33 + c */
    }

    return hash % num_reduce_tasks;
}

static void split (mr_env_t *env) {
	struct timeval begin, end;
	int num_map_tasks;
	
	get_time (&begin);
	num_map_tasks = gen_map_tasks (env);
	get_time (&end);
	
#ifdef TIMING
    fprintf(stderr, "splitter time: %u\n", time_diff (&end, &begin));
#endif
	
	assert (num_map_tasks >= 0);
	
	env->num_map_tasks = num_map_tasks;
	mr_shared_env->num_map_tasks = num_map_tasks;
}

/**
 * Run map tasks and get intermediate values
 */
static void map (mr_env_t* env)
{
    thread_arg_t   th_arg;
	
	env->num_map_tasks = mr_shared_env->num_map_tasks;
	
    dprintf (OUT_PREFIX "num_map_tasks = %d\n", env->num_map_tasks);

	mem_memset (&th_arg, 0, sizeof(thread_arg_t));
	th_arg.task_type = TASK_TYPE_MAP;
	
	start_workers (env, &th_arg);
}

/**
 * Run reduce tasks and get final values. 
 */
static void reduce (mr_env_t* env)
{
    thread_arg_t   th_arg;

	mem_memset (&th_arg, 0, sizeof(thread_arg_t));
	th_arg.task_type = TASK_TYPE_REDUCE;
	
	start_workers (env, &th_arg);
}

/**
 * Merge all reduced data 
 */
static void merge (mr_env_t* env)
{
    thread_arg_t   th_arg;

	mem_memset (&th_arg, 0, sizeof (thread_arg_t));
	th_arg.task_type = TASK_TYPE_MERGE;
	
	th_arg.merge_len = L_NUM_THREADS;
	th_arg.merge_input = env->final_vals;
	
	if (th_arg.merge_len <= 1) {
		/* Already merged, nothing to do here */
		env->args->result->data = env->final_vals->arr;
		env->args->result->length = env->final_vals->len;
		
		shm_free(env->final_vals);
		
		return;
	}
	
	/* have work to merge! */
	while (th_arg.merge_len > 1) {
		th_arg.merge_round += 1;
		
		/* This is the worst case length, 
		   depending on the value of num_merge_threads. */
		env->merge_vals = (keyval_arr_t*) 
			shm_alloc (env->num_merge_threads * sizeof(keyval_arr_t));
		CHECK_ERROR(env->merge_vals == NULL);
		memset(env->merge_vals, 0, env->num_merge_threads * sizeof(keyval_arr_t));
		
		mr_shared_env->merge_vals = env->merge_vals;
		
		/* Run merge tasks and get merge values. */
		start_workers (env, &th_arg);
		
		th_arg.merge_len = env->num_merge_threads;
		
		env->num_merge_threads /= 2;
		if (env->num_merge_threads == 0)
			env->num_merge_threads = 1;

		shm_free(th_arg.merge_input);
		th_arg.merge_input = env->merge_vals;
	}
}

static inline mr_env_t* get_env (void)
{
    return (mr_env_t*)pthread_getspecific (env_key);
}
