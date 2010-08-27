#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "atomic.h"
#include "memory.h"
#include "shm_alloc.h"
#include "stddefines.h"

/* The Nahanni device */
#define SHM_DEV        "/dev/uio0"
/* The number of blocks */
#define N_BLOCKS       (256*1024)
/* The number of blocks required for a given size */
#define BLOCKS_REQ(sz) ((sz / BLK_SIZE) + 1)
/* The maximum number of chunks we can allocate */
#define MAX_CHUNKS     (1024*1024)
/* The block index of an address */
#define BLK_INDEX(p)   (p - blocks) / BLK_SIZE

/* A clearer name for gcc's test and set byte */
#define test_and_set_byte(x) !__sync_lock_test_and_set(x, (char)1)

static void *alloc_base;
static uint64_t *last_blk;
static uint64_t *last_chnk;
static char *blkmap;
static void *blocks;

/* The size of the shared memory, in bytes */
static int SHM_SIZE;
/* Minimum allocation size */
static int BLK_SIZE;

typedef struct {
	/* The size of this allocated chunk */
	size_t size;
	/* The start address, in shared memory */
	void *start;
} chunk_t;

static chunk_t *chunklist;

void shm_alloc_init(void *base, size_t size, int master) {
	/* Set up the allocation constants */
	alloc_base = base;
	last_blk = base;
	last_chnk = last_blk + sizeof(uint64_t);
	blkmap = (void *)last_chnk + sizeof(uint64_t);
	chunklist  = (chunk_t *)(blkmap + N_BLOCKS);
	blocks = (char *)((void *)chunklist + MAX_CHUNKS*sizeof(chunk_t));
	SHM_SIZE = size - (blocks - alloc_base);
	BLK_SIZE = SHM_SIZE / N_BLOCKS;

	/* If we've declared ourselves the "master" then clear everything out */
	if(master) {
		*last_blk = 0;
		*last_chnk = 0;
		memset(blkmap, 0, N_BLOCKS);
		memset(chunklist, 0, MAX_CHUNKS * sizeof(chunk_t));
	}
}

void *shm_alloc(size_t size) {
	int req = BLOCKS_REQ(size);
	int i, j, k, coll;
	int upper;

#ifdef SHM_DEBUG
	struct timeval b1, e1, b2, e2;
	uint64_t t1, t2;

	printf("Allocating shm block of size %d ... ", size);
	gettimeofday(&b1, NULL);
#endif

	/* Find a free chunk structure */
	upper = MAX_CHUNKS;
	for(i = *last_chnk; i < upper; i++) {
		/* Atomically check that size is 0 and set it to the new size */
		if(cmp_and_swp(size, &(chunklist[i].size), 0)) {
			/* If it succeeded, then we have the chunk and can continue. */
			*last_chnk = i;
			break;
		}
		/* If it failed, then someone else stole the chunk and we have to
		   keep looking */

		/* If we've reached the end wrap around */
		if(i == MAX_CHUNKS - 1) {
			i = 0;
			upper = *last_chnk;
		}
	}

#ifdef SHM_DEBUG
	gettimeofday(&e1, NULL);
#endif

	/* Out of chunks. */
	if(i == upper) {
#ifdef SHM_DEBUG
		printf("Out of chunks\n");
#endif
		errno = ENOMEM;
		return NULL;
	}

#ifdef SHM_DEBUG
	gettimeofday(&b2, NULL);
#endif

	/* Collect the number of blocks we need */
	coll = 0;
	upper = N_BLOCKS;
	for(j = *last_blk; j < upper; j++) {
		/* Try to get this block */
		if(test_and_set_byte(&blkmap[j])) {
			if(coll == 0) {
				/* If this is the first block we've grabbed, it's the "start" */
				chunklist[i].start = blocks + j*BLK_SIZE;
			}
			
			coll += 1;

			/* If we have enough blocks we're done */
			if(coll == req) {
				break;
			}
		} else {
			/* If we failed to get the block, release any we've already
			   grabbed. */
			for(k = 1; k <= coll; k++) {
				blkmap[j - coll] = 0;
			}
			coll = 0;

			/* Start back at the beginning if we're out of space at the top. */
			if(j == N_BLOCKS - 1) {
				upper = *last_blk;
				j = 0;
			}
		}
	}

#ifdef SHM_DEBUG
	gettimeofday(&e2, NULL);
#endif

	/* Out of space. */
	if(coll < req) {
#ifdef SHM_DEBUG
		printf("Out of space\n");
#endif
		
		for(k = 1; k <= coll; k++) {
			blkmap[j - coll] = 0;
		}
		errno = ENOMEM;
		return NULL;
	} else {
		/* Note that this is not guaranteed to be consistent - but that's OK for
		 * us. */
		*last_blk = j;
	}

#ifdef SHM_DEBUG
	printf("OK\n");
	t1 = (e1.tv_sec - b1.tv_sec)*1000000 + (e1.tv_usec - b1.tv_usec);
	t2 = (e2.tv_sec - b2.tv_sec)*1000000 + (e2.tv_usec - b2.tv_usec);
	printf("SHM_ALLOC TIMING: Phase 1 %lu -- Phase 2 %lu\n", t1, t2);
#endif

	return chunklist[i].start;
}

void *shm_realloc(void *ptr, size_t sz) {
	int i;
	size_t tocopy;
	void *n;

	/* Find the chunk */
	for(i = 0; i < MAX_CHUNKS; i++) {
		if(chunklist[i].start == ptr) {
			break;
		}
	}
	/* We didn't allocate this chunk */
	CHECK_ERROR(i == MAX_CHUNKS);

	/* How much do we need to copy? */
	if(sz > chunklist[i].size) {
		tocopy = chunklist[i].size;
	} else if(sz < chunklist[i].size) {
		tocopy = sz;
	} else {
		/* If we're reallocing the same amount of memory, don't bother. */
		return ptr;
	}

	/* Allocate a new chunk */
	n = shm_alloc(sz);
	if(n == NULL) {
		errno = ENOMEM;
		return NULL;
	}

	/* Copy over the data and free the old chunk */
	/* NOTE: we could temporarily copy the data into local memory, then free,
	 * then allocate and copy, to reduce failures here. --awg */
	mem_memcpy(n, ptr, tocopy);
	shm_free(ptr);

	return n;
}

void shm_free(void *ptr) {
	int i,j;

	/* Find the chunk */
	for(i = 0; i < MAX_CHUNKS; i++) {
		if(chunklist[i].start == ptr) {
			break;
		}
	}

	/* Passed a pointer we didn't allocate */
	CHECK_ERROR(i == MAX_CHUNKS);

	/* Free the blocks */
	for(j = BLK_INDEX(ptr); j < BLOCKS_REQ(chunklist[i].size); j++) {
		blkmap[j] = 0;
	}

	/* Free up the chunk entry */
	chunklist[i].start = NULL;
	chunklist[i].size = 0;
}

