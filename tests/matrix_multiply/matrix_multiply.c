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
#include <time.h>
#include <inttypes.h>
#include <sys/time.h>

#include "map_reduce.h"
#include "stddefines.h"

typedef struct {
	int row_num;
	int col_num;
    int *matrix_A;
    int *matrix_B;
    int matrix_len;
    int unit_size;
	final_data_t output;
} mm_data_t;

int mykeycmp(const void *v1, const void *v2) {
	int x1, y1, x2, y2;

	x1 = (uint64_t)v1 >> 32;
//	y1 = (uint64_t)v1 & 0x00000000ffffffff;
	x2 = (uint64_t)v2 >> 32;
//	y2 = (uint64_t)v2 & 0x00000000ffffffff;
	
    if(x1 < x2) return -1;
    else if(x1 > x2) return 1;
	else return 0;
/*    else
    {
        if(y1 < y2) return -1;
        else if(y1 > y2) return 1;
        else return 0;
    }
*/
}

static int mm_partition(int reduce_tasks, void* key, int key_size) {
	uint64_t x = (uint64_t)key >> 32;
	return x % reduce_tasks;
}

/** matrixmul_splitter()
 *  Assign a set of rows of the output matrix to each map task
 */
int matrixmult_splitter(void *data_in, int req_units, map_args_t *out,
						splitter_mem_ops_t *mem) {
    /* Make a copy of the mm_data structure */
    mm_data_t * data = (mm_data_t *)data_in;
	int i, j;
	static int *rowblock = NULL;

    /* Check whether the various terms exist */
    assert(data_in);
    assert(out);
    
    assert(data->matrix_len >= 0);
    assert(data->unit_size >= 0);

    assert(data->matrix_A);
    assert(data->matrix_B);

	if(req_units < 0) {
		data->col_num = 0;
		data->row_num = 0;
		if(rowblock != NULL) {
			free(rowblock);
			rowblock = NULL;
		}
		return(0);
	}

	/* We take the first block of rows, match it up with the each block of
	 * columns, then continue. */
	if(data->col_num >= data->matrix_len) {
		data->row_num += req_units;
		if(data->row_num >= data->matrix_len) {
			return(0);
		}
		data->col_num = 0;
		free(rowblock);
		rowblock = NULL;
	}

    /* Compute available rows */
    int available_rows = data->matrix_len - data->row_num;
	int available_cols = data->matrix_len - data->col_num;
	int nrows = (req_units < available_rows) ? req_units : available_rows;
	int ncols = (req_units < available_cols) ? req_units : available_cols;
	int asize = nrows * (data->matrix_len + 1);
	int bsize = ncols * (data->matrix_len + 1);
	out->length = (3 + asize + bsize) * sizeof(int);
	int *realout = mem->alloc(out->length);
	out->data = realout;
	realout[0] = data->matrix_len;
	realout[1] = nrows;
	realout[2] = ncols;
	int *outdata = realout + 3;
	int *B = outdata + asize;

	if(rowblock == NULL) {
		rowblock = malloc(asize*sizeof(int));

		// rowblock[i,0] = row_num
		// rowblock[i,j+1] = A[i,j]
		for(i = 0; i < nrows; i++) {
			rowblock[i*(data->matrix_len+1)] = data->row_num + i;
			for(j = 0; j < data->matrix_len; j++) {
				rowblock[i*(data->matrix_len+1)+j+1] = data->matrix_A[(data->row_num+i)*data->matrix_len+j];
			}
		}
	}
	
	memcpy(outdata, rowblock, asize*sizeof(int));
	// data[i,0] = col_num
	// data[i,j+1] = B[j,i] (transposed block)
	for(i = 0; i < ncols; i++) {
		B[i*(data->matrix_len + 1)] = data->col_num + i;
		for(j = 0; j < data->matrix_len; j++) {
			B[i*(data->matrix_len+1)+j+1] = data->matrix_B[j*data->matrix_len+(data->col_num+i)];
		}
	}

	data->col_num += ncols;

	return 1;
}

/** matrixmul_map()
 * Multiplies the allocated regions of matrix to compute partial sums 
 */
void matrixmult_map(map_args_t *args) {
    uint64_t x,y;
	int i;
	uint64_t value;
	uint64_t key;

    assert(args);

    int *input = (int *)args->data;
	int rowlen = input[0];
	int nrows = input[1];
	int ncols = input[2];
	int *A = input + 3;
	int *B = A + (nrows*(rowlen + 1));

	for(x = 0; x < nrows; x++) {
		for(y = 0; y < ncols; y++) {
			key = ((uint64_t)A[x*(rowlen+1)] << 32) | (B[y*(rowlen+1)]);
			value = 0;
			for(i = 0; i < rowlen; i++) {
				value += A[x*(rowlen+1)+i+1] * B[y*(rowlen+1)+i+1];
			}
			emit_intermediate((void *)key, (void *)value, sizeof(void *));
		}
	}
}

static int mm_prep(void *data_in, map_reduce_args_t *args) {
	mm_data_t *data = (mm_data_t *)data_in;
	int i,j;

	data->matrix_A = malloc(data->matrix_len * data->matrix_len * sizeof(int));
	data->matrix_B = malloc(data->matrix_len * data->matrix_len * sizeof(int));
	
	// For consistent results, we make every cell 1.
	for(i=0; i < data->matrix_len; i++) {
		for(j=0; j < data->matrix_len; j++) {
			data->matrix_A[i*data->matrix_len + j] = 1;
			data->matrix_B[i*data->matrix_len + j] = 1;
		}
	}

	args->data_size = 2 * data->matrix_len * data->matrix_len * sizeof(int);

	return(0);
}

int mm_cleanup(void *data_in) {
	mm_data_t *data = (mm_data_t *)data_in;

	printf("%d results\n", data->output.length);
	
    uint64_t sum = 0;
	int i;
    for(i=0; i < data->matrix_len * data->matrix_len; i++) {
		sum += (uint64_t)data->output.data[i].val;
    }
    dprintf("\nMatrixMult: total sum is %lu\n", sum);

	return(0);
}


int main(int argc, char *argv[]) {

	mm_data_t mm_data;
    map_reduce_args_t map_reduce_args;
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
	
    CHECK_ERROR (map_reduce_init (&argc, &argv));

    // Make sure a side length
    if (argv[1] != NULL) {
		CHECK_ERROR ( (mm_data.matrix_len = atoi(argv[1])) < 0);
	}

    printf("MatrixMult: Side of the matrix is %d\n", mm_data.matrix_len);
    printf("MatrixMult: Running...\n");

    // Setup splitter args
    mm_data.unit_size = (1 + 2 * (mm_data.matrix_len + 1)) * sizeof(int);
    mm_data.row_num = 0;
	mm_data.col_num = 0;

    // Setup map reduce args
    map_reduce_args.task_data = &mm_data;
	map_reduce_args.task_data_size = sizeof(mm_data_t);

	map_reduce_args.prep = mm_prep;
	map_reduce_args.cleanup = mm_cleanup;
    map_reduce_args.map = matrixmult_map;
    map_reduce_args.reduce = NULL;
    map_reduce_args.splitter = matrixmult_splitter;
    map_reduce_args.locator = NULL;
    map_reduce_args.key_cmp = mykeycmp;
	
    map_reduce_args.unit_size = mm_data.unit_size;
    map_reduce_args.partition = mm_partition;
    map_reduce_args.result = &mm_data.output;

	map_reduce_args.L1_cache_size = atoi(GETENV("MR_L1CACHESIZE"));//1024 * 8;
    map_reduce_args.num_map_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_reduce_threads = atoi(GETENV("MR_NUMTHREADS"));//16;
    map_reduce_args.num_merge_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_procs = atoi(GETENV("MR_NUMPROCS"));//16;
    map_reduce_args.key_match_factor = (float)atof(GETENV("MR_KEYMATCHFACTOR"));//2;

    printf("MatrixMult: Calling MapReduce Scheduler Matrix Multiplication\n");

    CHECK_ERROR (map_reduce (&map_reduce_args) < 0);
	map_reduce_cleanup(&map_reduce_args);
    CHECK_ERROR (map_reduce_finalize ());

    //dprintf("\n");
    //dprintf("The length of the final output is %d\n",mm_vals.length );
    dprintf("MatrixMult: MapReduce Completed\n");

	return 0;
}
