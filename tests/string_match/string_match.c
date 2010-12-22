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
#include <sys/times.h>
#include <time.h>
#include <unistd.h>

#include "map_reduce.h"
#include "stddefines.h"

#define DEFAULT_UNIT_SIZE 5
#define MAX_REC_LEN 1024
#define OFFSET 5

char key1_final[MAX_REC_LEN];
char key2_final[MAX_REC_LEN];
char key3_final[MAX_REC_LEN];
char key4_final[MAX_REC_LEN];

typedef struct {
	char *fname_keys;
	int fd_keys;
	size_t keys_file_len;
	size_t offset;
} str_data_t;

char *key1 = "Helloworld";
char *key2 = "howareyou";
char *key3 = "ferrari";
char *key4 = "whotheman";

/** mystrcmp()
 *  Default comparison function
 */
int mystrcmp(const void *v1, const void *v2)
{
    return 1;
}

/** compute_hashes()
 *  Simple Cipher to generate a hash of the word 
 */
void compute_hashes(char* word, char* final_word, int len)
{
    int i;
	memset(final_word, 0, MAX_REC_LEN);

    for(i=0;i<len;i++)
        final_word[i] = word[i]+OFFSET;
}

/** string_match_splitter()
 *  Splitter Function to assign portions of the file to each map task
 */
int string_match_splitter(void *data_in, int req_units, map_args_t *out, splitter_mem_ops_t *mem)
{
    /* Make a copy of the mm_data structure */
    str_data_t * data = (str_data_t *)data_in;
	int less;
    int req_bytes = req_units*DEFAULT_UNIT_SIZE;
    int available_bytes = data->keys_file_len - data->offset;
	char *c;

	if(req_units < 0) {
		if(data->offset > 0) {
			lseek(data->fd_keys, 0, SEEK_SET);
			data->offset = 0;
		}
		return(0);
	}
	
    /* Check whether the various terms exist */
    assert(data_in);
    assert(out);
    assert(req_units >= 0);
    assert(data->offset >= 0);

    if(data->offset >= data->keys_file_len) {
        return 0;
    }

tryagain:
    /* Assign the required number of bytes */
    out->length = (req_bytes < available_bytes)? req_bytes:available_bytes;
    out->data = mem->alloc(out->length + 1);
	CHECK_ERROR(out->data == NULL);
	out->length = read(data->fd_keys, out->data, out->length);
	data->offset += out->length;

    // Find the last space in the read data, assuming we're not done.
	for(c = &((char *)out->data)[out->length - 1], less = 0;
		data->offset != data->keys_file_len &&
		c >= (char *)out->data &&
			*c != ' ' && *c != '\t' &&
			*c != '\r' && *c != '\n';
		c--, less--);

	// Seek back to where the space was.
	if(less < 0) {
		lseek(data->fd_keys, less, SEEK_CUR);
		data->offset += less;
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

/** string_match_map()
 *  Map Function that checks the hash of each word to the given hashes
 */
void string_match_map(map_args_t *args)
{
    assert(args);
    
    int key_len, total_len = 0;
    char *key_file = args->data;
    char *cur_word;

	char cur_word_final[MAX_REC_LEN];

    while(total_len < args->length) {
		for(;
			(*key_file == '\r' || *key_file == '\n') && total_len < args->length;
			key_file += 1, total_len += 1);

		cur_word = key_file;
		key_file = strpbrk(key_file, "\r\n");

		key_len = key_file - cur_word;
		total_len += key_len;
		
		memset(cur_word_final, 0, MAX_REC_LEN);
        compute_hashes(cur_word, cur_word_final, key_len);

        if(!memcmp(key1_final, cur_word_final, key_len)) {
			emit_intermediate(cur_word, (void *)1, key_len);
		}

        if(!memcmp(key2_final, cur_word_final, key_len)) {
			emit_intermediate(cur_word, (void *)1, key_len);
		}

        if(!memcmp(key3_final, cur_word_final, key_len)) {
			emit_intermediate(cur_word, (void *)1, key_len);
		}

        if(!memcmp(key4_final, cur_word_final, key_len)) {
			emit_intermediate(cur_word, (void *)1, key_len);
		}
    }
}

void sm_reduce(void *key_in, iterator_t *itr)
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

int sm_prep(void *data_in, map_reduce_args_t *args) {
	str_data_t *data = (str_data_t *)data_in;
    struct stat finfo_keys;

	// Read in the file
    CHECK_ERROR((data->fd_keys = open(data->fname_keys,O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(data->fd_keys, &finfo_keys) < 0);
    args->data_size = finfo_keys.st_size;
    data->keys_file_len = finfo_keys.st_size;

	return(0);
}

int sm_cleanup(void *data_in) {
	str_data_t *data = (str_data_t *)data_in;
	return(close(data->fd_keys));
}

int main(int argc, char *argv[]) {
    final_data_t str_vals;
    struct timeval begin, end;
    struct timeval starttime,endtime;
    str_data_t str_data;

    get_time (&begin);

    CHECK_ERROR (map_reduce_init (&argc, &argv));

	compute_hashes(key1, key1_final, strlen(key1));
	compute_hashes(key2, key2_final, strlen(key2));
	compute_hashes(key3, key3_final, strlen(key3));
	compute_hashes(key4, key4_final, strlen(key4));

    str_data.offset = 0;
    str_data.fname_keys = argv[1];

    printf("String Match: Running...\n");

    // Setup scheduler args
    map_reduce_args_t map_reduce_args;
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = &str_data;
	map_reduce_args.task_data_size = sizeof(str_data_t);
	
	map_reduce_args.prep = sm_prep;
	map_reduce_args.cleanup = sm_cleanup;
    map_reduce_args.map = string_match_map;
    map_reduce_args.reduce = sm_reduce;
    map_reduce_args.splitter = string_match_splitter;
    map_reduce_args.key_cmp = mystrcmp;
	
    map_reduce_args.unit_size = DEFAULT_UNIT_SIZE;
    map_reduce_args.partition = NULL; // use default
    map_reduce_args.result = &str_vals;
	
    map_reduce_args.L1_cache_size = atoi(GETENV("MR_L1CACHESIZE"));//1024 * 512;
    map_reduce_args.num_map_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_reduce_threads = atoi(GETENV("MR_NUMTHREADS"));//16;
    map_reduce_args.num_merge_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_procs = atoi(GETENV("MR_NUMPROCS"));//16;
    map_reduce_args.key_match_factor = (float)atof(GETENV("MR_KEYMATCHFACTOR"));//2;

    printf("String Match: Calling String Match\n");

    gettimeofday(&starttime,0);

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "initialize: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);
    CHECK_ERROR (map_reduce (&map_reduce_args) < 0);
    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "library: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);

    gettimeofday(&endtime,0);

    printf("\nString Match Results:\n");
	int i;
    for (i = 0; i < str_vals.length; i++) {
		keyval_t * curr = &((keyval_t *)str_vals.data)[i];
		dprintf("%15s - %" PRIdPTR "\n", (char *)curr->key, (intptr_t)curr->val);
    }

    get_time (&end);

	map_reduce_cleanup(&map_reduce_args);
    CHECK_ERROR (map_reduce_finalize ());

#ifdef TIMING
    fprintf (stderr, "finalize: %u\n", time_diff (&end, &begin));
#endif

    return 0;
}
