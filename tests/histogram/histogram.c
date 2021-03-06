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
#include <fcntl.h>
#include <ctype.h>
#include <inttypes.h>

#include "map_reduce.h"
#include "stddefines.h"

#define IMG_DATA_OFFSET_POS 10
#define BITS_PER_PIXEL_POS 28
#define READ_AHEAD 32

typedef struct {
	char *fname;
	int fd;
	int data_bytes;
	int offset;
	int unit_size;
} hist_data_t;

short red_keys[256];
short green_keys[256];
short blue_keys[256];

/* test_endianess
 *
 */
void test_endianess(int *swap) {
    unsigned int num = 0x12345678;
    char *low = (char *)(&(num));
    if (*low ==  0x78) {
        dprintf("No need to swap\n");
        *swap = 0;
    }
    else if (*low == 0x12) {
        dprintf("Need to swap\n");
        *swap = 1;
    }
    else {
        printf("Error: Invalid value found in memory\n");
        exit(1);
    } 
}

/* swap_bytes
 *
 */
void swap_bytes(char *bytes, int num_bytes) {
    int i;
    char tmp;
    
    for (i = 0; i < num_bytes/2; i++) {
        dprintf("Swapping %d and %d\n", bytes[i], bytes[num_bytes - i - 1]);
        tmp = bytes[i];
        bytes[i] = bytes[num_bytes - i - 1];
        bytes[num_bytes - i - 1] = tmp;    
    }
}

/* myshortcmp
 * Comparison function
 */
int myshortcmp(const void *s1, const void *s2)
{    
    short val1 = *((short *)s1);
    short val2 = *((short *)s2);
    
    if (val1 < val2) {
        return -1;
    }
    else if (val1 > val2) {
        return 1;
    }
    else {
        return 0;
    }
}

/** hist_map()
 * Map function that computes the histogram values for the portion
 * of the image assigned to the map task
 */
void hist_map(map_args_t *args) 
{
    int i;
    short *key;
    unsigned char *val;
    intptr_t red[256];
    intptr_t green[256];
    intptr_t blue[256];
	int cnt = 0;

    assert(args);
    unsigned char *data = (unsigned char *)args->data;
    assert(data);

    memset(&(red[0]), 0, sizeof(intptr_t) * 256);
    memset(&(green[0]), 0, sizeof(intptr_t) * 256);
    memset(&(blue[0]), 0, sizeof(intptr_t) * 256);

    for (i = 0; i < args->length; i+=3) 
    {
        val = &(data[i]);
        blue[*val]++;
        
        val = &(data[i+1]);
        green[*val]++;
        
        val = &(data[i+2]);
        red[*val]++;    
    }
    
    for (i = 0; i < 256; i++) 
    {
        if (blue[i] > 0) {
            key = &(blue_keys[i]);
            emit_intermediate((void *)key, (void *)blue[i], (int)sizeof(short));
			cnt++;
        }
        
        if (green[i] > 0) {
            key = &(green_keys[i]);
            emit_intermediate((void *)key, (void *)green[i], (int)sizeof(short));
			cnt++;
        }
        
        if (red[i] > 0) {
            key = &(red_keys[i]);
            emit_intermediate((void *)key, (void *)red[i], (int)sizeof(short));
			cnt++;
        }
    }
}

/** hist_reduce()
 * Reduce function that adds up the values for each location in the array
 */
void hist_reduce(void *key_in, iterator_t *itr)
{
    short *key = (short *)key_in;
    void *val;
    intptr_t sum = 0;

    assert(key);
    assert(itr);
    
    /* dprintf("For key %hd, there are %d vals\n", *key, vals_len); */
    
    while (iter_next (itr, &val))
    {
        sum += (intptr_t)val;
    }

    emit(key, (void *)sum);
}

void *hist_combiner (iterator_t *itr)
{
    void *val;
    intptr_t sum = 0;

    assert(itr);
    
    /* dprintf("For key %hd, there are %d vals\n", *key, vals_len); */
    
    while (iter_next (itr, &val))
    {
        sum += (intptr_t)val;
    }

    return (void *)sum;
}

int hist_splitter(void *data_in, int req_units, map_args_t *out, splitter_mem_ops_t *mem) {
	hist_data_t *data = (hist_data_t *)data_in;
	int r;

	if(req_units < 0) {
		if(data->offset > 0) {
			lseek(data->fd, -(data->offset), SEEK_CUR);
			data->offset = 0;
		}
		return(0);
	}

	if(data->offset >= data->data_bytes) {
		return 0;
	}

	if(data->data_bytes - data->offset < req_units * data->unit_size) {
		out->length = data->data_bytes - data->offset;
	} else {
		out->length = req_units * data->unit_size;
	}

	out->data = mem->alloc(out->length);
	CHECK_ERROR (out->data == NULL);
	r = read(data->fd, out->data, out->length);

	if(r != out->length) {
		out->length -= r % data->unit_size;
		lseek(data->fd, -(r % data->unit_size), SEEK_CUR);
	}

	data->offset += out->length;

	return 1;
}

int hist_prep(void *data_in, map_reduce_args_t *args) {
	hist_data_t *data = (hist_data_t *)data_in;
    struct stat finfo;
	char topdata[READ_AHEAD];

	if(data->fname == NULL) {
		printf("Must specify filename.\n");
		return(-1);
	}
	
    // Read in the file
    CHECK_ERROR((data->fd = open(data->fname, O_RDONLY)) < 0);
	// Get the file info (for file length)
	CHECK_ERROR(fstat(data->fd, &finfo) < 0);

	if(read(data->fd, topdata, READ_AHEAD) != READ_AHEAD) {
		return(-1);
	}
	
	if ((topdata[0] != 'B') || (topdata[1] != 'M')) {
		printf("File is not a valid bitmap file.\n");
		return(-1);
	}
	
	int swap;
	test_endianess(&swap);     // will set the variable "swap"

	unsigned short *bitsperpixel = (unsigned short *)(&(topdata[BITS_PER_PIXEL_POS]));
	if (swap) {
		swap_bytes((char *)(bitsperpixel), sizeof(*bitsperpixel));
	}
	if (*bitsperpixel != 24) {     // ensure its 3 bytes per pixel
		printf("Error: Invalid bitmap format - ");
		printf("This application only accepts 24-bit pictures.\n");
		return(-1);
	}
	
	unsigned short data_offset = *(unsigned short *)(&(topdata[IMG_DATA_OFFSET_POS]));
	if (swap) {
		swap_bytes((char *)(&data_offset), sizeof(data_offset));
	}
	
	data->data_bytes = (int)finfo.st_size - (int)data_offset;
    args->data_size = data->data_bytes;
	printf("This file has %d bytes of image data, %d pixels\n", data->data_bytes,
		   data->data_bytes / 3);
	
	// Seek the FD to the beginning of the data
	lseek(data->fd, data_offset, SEEK_SET);
	data->offset = 0;
	
	return(0);
}

int hist_cleanup(void *data_in) {
	hist_data_t *data = (hist_data_t *)data_in;
    return(close (data->fd));
}

int main(int argc, char *argv[]) {
    
    final_data_t hist_vals;
    int i;
    struct timeval begin, end;

	hist_data_t hist_data;

    get_time (&begin);

    // We use this global variable arrays to store the "key" for each histogram
    // bucket. This is to prevent memory leaks in the mapreduce scheduler
	for (i = 0; i < 256; i++) {
        blue_keys[i] = i;
        green_keys[i] = 1000 + i;
        red_keys[i] = 2000 + i;
    }

	i = map_reduce_init(&argc, &argv);
    CHECK_ERROR (i < 0);

    hist_data.fname = argv[1];

    printf("Histogram: Running...\n");
    
	// Setup map reduce args
    map_reduce_args_t map_reduce_args;
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = &hist_data;
	map_reduce_args.task_data_size = sizeof(hist_data_t);
	map_reduce_args.prep = hist_prep;
	map_reduce_args.cleanup = hist_cleanup;
    map_reduce_args.map = hist_map;
    map_reduce_args.reduce = hist_reduce;
    map_reduce_args.combiner = hist_combiner;
    map_reduce_args.splitter = hist_splitter;
    map_reduce_args.key_cmp = myshortcmp;
    
    map_reduce_args.unit_size = 3;  // 3 bytes per pixel
	hist_data.unit_size = 3;
    map_reduce_args.partition = NULL; // use default
    map_reduce_args.result = &hist_vals;
    
    map_reduce_args.L1_cache_size = atoi(GETENV("MR_L1CACHESIZE"));//1024 * 512;
    map_reduce_args.num_map_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_reduce_threads = atoi(GETENV("MR_NUMTHREADS"));//16;
    map_reduce_args.num_merge_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_procs = atoi(GETENV("MR_NUMPROCS"));//16;
    map_reduce_args.key_match_factor = (float)atof(GETENV("MR_KEYMATCHFACTOR"));//2;

    fprintf(stderr, "Histogram: Calling MapReduce Scheduler\n");

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "initialize: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);
    CHECK_ERROR( map_reduce (&map_reduce_args) < 0);
    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "library: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);

    short pix_val;
    intptr_t freq;
    short prev = 0;
    
    dprintf("\n\nBlue\n");
    dprintf("----------\n\n");
    for (i = 0; i < hist_vals.length; i++)
    {
        keyval_t * curr = &((keyval_t *)hist_vals.data)[i];
        pix_val = *((short *)curr->key);
        freq = (intptr_t)curr->val;
        
        if (pix_val - prev > 700) {
            if (pix_val >= 2000) {
                dprintf("\n\nRed\n");
                dprintf("----------\n\n");
            }
            else if (pix_val >= 1000) {
                dprintf("\n\nGreen\n");
                dprintf("----------\n\n");
            }
        }
        
        dprintf("%hd - %" PRIdPTR "\n", pix_val % 1000, freq);
        
        prev = pix_val;
    }

	map_reduce_cleanup(&map_reduce_args);
	CHECK_ERROR (map_reduce_finalize ());
    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "finalize: %u\n", time_diff (&end, &begin));
#endif

    return 0;
}
