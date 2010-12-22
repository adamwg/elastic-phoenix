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

#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <ctype.h>
#include <inttypes.h>

#include "map_reduce.h"
#include "stddefines.h"
#include "sort.h"

#define DEFAULT_DISP_NUM 10

typedef struct {
	char *fname;
	size_t fsize;
	off_t fpos;
    int fd;
    int unit_size;
} wc_data_t;

enum {
    IN_WORD,
    NOT_IN_WORD
};

    struct timeval begin, end;
#ifdef TIMING
    unsigned int library_time = 0;
#endif

/** mystrcmp()
 *  Comparison function to compare 2 words
 */
int mystrcmp(const void *s1, const void *s2)
{
    return strcmp((const char *)s1, (const char *) s2);
}

keyval_t deref_keyval_t(keyval_t *kv) {
	return *kv;
}

/** mykeyvalcmp()
 *  Comparison function to compare 2 ints
 */
int mykeyvalcmp(const void *v1, const void *v2)
{
    keyval_t* kv1 = (keyval_t*)v1;
    keyval_t* kv2 = (keyval_t*)v2;

    intptr_t *i1 = kv1->val;
    intptr_t *i2 = kv2->val;

    if (i1 < i2) return 1;
    else if (i1 > i2) return -1;
    else {
    /*** don't just return 0 immediately coz the mapreduce scheduler provides 
    1 key with multiple values to the reduce task. since the different words that 
    we require are each part of a different keyval pair, returning 0 makes
    the mapreduce scheduler think that it can just keep one key and disregard
    the rest. That's not desirable in this case. Returning 0 when the values are
    equal produces results where the same word is repeated for all the instances
    which share the same frequency. Instead, we check the word as well, and only 
    return 0 if both the value and the word match ****/
        return strcmp((char *)kv1->key, (char *)kv2->key);
        //return 0;
    }
}

/** wordcount_splitter()
 *  Memory map the file and divide file on a word border i.e. a space.
 */
int wordcount_splitter(void *data_in, int req_units, map_args_t *out,
					   splitter_mem_ops_t *mem)
{
    wc_data_t * data = (wc_data_t *)data_in;
	char *c;
	int less;
    
    assert(data_in);
    assert(out);
    
	assert(data->fpos >= 0);
    assert(data->data_bytes >= 0);
    assert(data->fd > 0);
    assert(req_units);

	// At the end of the file.
	if(data->fpos >= data->data_bytes) {
		return(0);
	}

tryagain:
    // Allocate some memory for the data
	out->length = req_units * data->unit_size;
    out->data = mem->alloc(out->length + 1);
	assert(out->data != NULL);
	// Read the data
	out->length = read(data->fd, out->data, out->length);
	data->fpos += out->length;
    
    // Find the last space in the read data.
	for(c = &((char *)out->data)[out->length - 1], less = 0;
		c >= out->data &&
			*c != ' ' && *c != '\t' &&
			*c != '\r' && *c != '\n';
		c--, less--);

	// Seek back to where the space was.
	if(less < 0) {
		lseek(data->fd, less, SEEK_CUR);
		data->fpos += less;
		out->length += less;
	}

	((char *)out->data)[out->length] = 0;

	// If we just read one big long word, then change the req_units and go back
	// to the top... ugly, but it will work.
	if(out->length == 0) {
		mem->free(out->data);
		req_units++;
		goto tryagain;
	}
	
    return 1;
}

/** wordcount_locator()
 *  Return the memory address where this map task would heavily access.
 */
void *wordcount_locator (map_args_t *task)
{
    assert (task);

    return task->data;
}

/** wordcount_map()
 * Go through the allocated portion of the file and count the words
 */
void wordcount_map(map_args_t *args) 
{
    char *curr_start, curr_ltr;
    int state = NOT_IN_WORD;
    int i;
  
    assert(args);

    char *data = (char *)args->data;

    assert(data);
    curr_start = data;
    
    for (i = 0; i < args->length; i++)
    {
        curr_ltr = toupper(data[i]);
        switch (state)
        {
        case IN_WORD:
            data[i] = curr_ltr;
            if ((curr_ltr < 'A' || curr_ltr > 'Z') && curr_ltr != '\'')
            {
                data[i] = 0;
                emit_intermediate(curr_start, (void *)1, &data[i] - curr_start + 1);
                state = NOT_IN_WORD;
            }
            break;

        default:
        case NOT_IN_WORD:
            if (curr_ltr >= 'A' && curr_ltr <= 'Z')
            {
                curr_start = &data[i];
                data[i] = curr_ltr;
                state = IN_WORD;
            }
            break;
        }
    }

    // Add the last word
    if (state == IN_WORD)
    {
        data[args->length] = 0;
        emit_intermediate(curr_start, (void *)1, &data[i] - curr_start + 1);
    }
}

/** wordcount_reduce()
 * Add up the partial sums for each word
 */
void wordcount_reduce(void *key_in, iterator_t *itr)
{
    char *key = (char *)key_in;
    void *val;
    intptr_t sum = 0;

    assert(key);
    assert(itr);

    while (iter_next (itr, &val))
    {
        sum += (intptr_t)val;
    }

    emit(key, (void *)sum);
}

void *wordcount_combiner (iterator_t *itr)
{
    void *val;
    intptr_t sum = 0;

    assert(itr);

    while (iter_next (itr, &val))
    {
        sum += (intptr_t)val;
    }

    return (void *)sum;
}

int wc_prep(void *data_in, map_reduce_args_t *args) {
    struct stat finfo;
	wc_data_t *data = (wc_data_t *)data_in;

    // Read in the file
    CHECK_ERROR((data->fd = open(data->fname, O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(data->fd, &finfo) < 0);
	args->data_size = finfo.st_size;
	data->data_bytes = finfo.st_size;

	return(0);
}

int wc_cleanup(void *data_in) {
	wc_data_t *data = (wc_data_t *)data_in;
    return(close(data->fd));
}

int main(int argc, char *argv[]) 
{
    final_data_t wc_vals;
    int i;
	wc_data_t wc_data;

    struct timeval starttime,endtime;

    get_time (&begin);

    i = map_reduce_init (&argc, &argv);
	CHECK_ERROR(i < 0);

    wc_data.fname = argv[1];

    printf("Wordcount: Running...\n");

    // Setup splitter args
    wc_data.unit_size = 3; // approx 3 bytes per word
    wc_data.fpos = 0;

    // Setup map reduce args
    map_reduce_args_t map_reduce_args;
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = &wc_data;
	map_reduce_args.task_data_size = sizeof(wc_data_t);
	
	map_reduce_args.prep = wc_prep;
	map_reduce_args.cleanup = wc_cleanup;
	map_reduce_args.map = wordcount_map;
    map_reduce_args.reduce = wordcount_reduce;
    map_reduce_args.combiner = wordcount_combiner;
    map_reduce_args.splitter = wordcount_splitter;
    map_reduce_args.key_cmp = mystrcmp;
	
    map_reduce_args.unit_size = wc_data.unit_size;
    map_reduce_args.partition = NULL; // use default
    map_reduce_args.result = &wc_vals;
	
    map_reduce_args.L1_cache_size = atoi(GETENV("MR_L1CACHESIZE"));//1024 * 1024 * 2;
    map_reduce_args.num_map_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_reduce_threads = atoi(GETENV("MR_NUMTHREADS"));//16;
    map_reduce_args.num_merge_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_procs = atoi(GETENV("MR_NUMPROCS"));//16;
    map_reduce_args.key_match_factor = (float)atof(GETENV("MR_KEYMATCHFACTOR"));//2;

    printf("Wordcount: Calling MapReduce Scheduler Wordcount\n");

    gettimeofday(&starttime,0);

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "initialize: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);
    CHECK_ERROR(map_reduce (&map_reduce_args) < 0);
    get_time (&end);

#ifdef TIMING
    library_time += time_diff (&end, &begin);
#endif

    get_time (&begin);

    gettimeofday(&endtime,0);

    printf("Wordcount: Completed %ld\n",(endtime.tv_sec - starttime.tv_sec));

    printf("Wordcount: MapReduce Completed\n");

    qsort(wc_vals.data, wc_vals.length, sizeof(keyval_t), mykeyvalcmp);

    dprintf("\nWordcount: Results (TOP %d):\n", DEFAULT_DISP_NUM);
    for (i = 0; i < DEFAULT_DISP_NUM && i < wc_vals.length; i++) {
		keyval_t * curr = &((keyval_t *)wc_vals.data)[i];
		dprintf("%15s - %" PRIdPTR "\n", (char *)curr->key, (intptr_t)curr->val);
    }

	map_reduce_cleanup(&map_reduce_args);
    CHECK_ERROR (map_reduce_finalize ());

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "finalize: %u\n", time_diff (&end, &begin));
#endif

    return 0;
}
