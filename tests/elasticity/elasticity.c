/* Copyright (c) 2007-2009, Stanford University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *	   * Redistributions of source code must retain the above copyright
 *		 notice, this list of conditions and the following disclaimer.
 *	   * Redistributions in binary form must reproduce the above copyright
 *		 notice, this list of conditions and the following disclaimer in the
 *		 documentation and/or other materials provided with the distribution.
 *	   * Neither the name of Stanford University nor the names of its 
 *		 contributors may be used to endorse or promote products derived from 
 *		 this software without specific prior written permission.
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
#include <ctype.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "map_reduce.h"
#include "stddefines.h"

#define DEFAULT_DISP_NUM 10

typedef struct {
	int cnt;
	int n;
	final_data_t out;
} data_t;

static int intkeycmp(const void *v1, const void *v2) {
    intptr_t i1 = (intptr_t)v1;
    intptr_t i2 = (intptr_t)v2;

    if (i1 < i2) 
         return 1;
    else if (i1 > i2) 
         return -1;
    else 
         return 0;
}

int elasticity_splitter(void *data_in, int req_units, map_args_t *out,
						splitter_mem_ops_t *mem) {
	data_t * data = (data_t *)data_in;

	if(req_units < 0) {
		return 0;
	}
	if(data->cnt++ == data->n) {
		return 0;
	}

	out->data = mem->alloc(req_units);
	out->length = req_units;
	
	return 1;
}

void elasticity_map(map_args_t *args) {
	uint64_t i;
	for(i = 0; i < args->length; i++) {
		/* 50ms sleep */
		usleep(50000);
	}
	
	emit_intermediate((void *)i, (void *)1, sizeof(void*));
}

void elasticity_reduce(void *key_in, iterator_t *itr) {
	emit(key_in, (void *)1);
}

static int elasticity_partition(int reduce_tasks, void* key, int key_size) {
	return default_partition(reduce_tasks, (void *)&key, key_size);
}

int main(int argc, char *argv[]) {
	int i;
	data_t data;

	i = map_reduce_init (&argc, &argv);
	CHECK_ERROR(i < 0);

	if(argv[1] != NULL) {
		data.n = atoi(argv[1]);
		CHECK_ERROR(data.n <= 0);
		data.cnt = 0;
	}

	printf("Elasticity: Running with %d tasks...\n", data.n);

	// Setup map reduce args
	map_reduce_args_t map_reduce_args;
	memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
	map_reduce_args.task_data = &data;
	map_reduce_args.task_data_size = sizeof(data_t);
	map_reduce_args.data_size = data.n;
	
	map_reduce_args.prep = NULL;
	map_reduce_args.cleanup = NULL;
	map_reduce_args.map = elasticity_map;
	map_reduce_args.reduce = elasticity_reduce;
	map_reduce_args.combiner = NULL;
	map_reduce_args.splitter = elasticity_splitter;
	map_reduce_args.key_cmp = intkeycmp;
	
	map_reduce_args.unit_size = 1;
	map_reduce_args.partition = elasticity_partition; // use default
	map_reduce_args.result = &data.out;
	
	map_reduce_args.L1_cache_size = atoi(GETENV("MR_L1CACHESIZE"));//1024 * 1024 * 2;
	map_reduce_args.num_map_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
	map_reduce_args.num_reduce_threads = atoi(GETENV("MR_NUMTHREADS"));//16;
	map_reduce_args.num_merge_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
	map_reduce_args.num_procs = atoi(GETENV("MR_NUMPROCS"));//16;
	map_reduce_args.key_match_factor = (float)atof(GETENV("MR_KEYMATCHFACTOR"));//2;

	CHECK_ERROR(map_reduce (&map_reduce_args) < 0);
	map_reduce_cleanup(&map_reduce_args);
	CHECK_ERROR (map_reduce_finalize ());

	return 0;
}
