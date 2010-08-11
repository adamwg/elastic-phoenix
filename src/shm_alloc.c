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
/* Allocate memory in 4k blocks */
#define BLK_SIZE       4096
/* The number of blocks */
#define N_BLOCKS       (SHM_SIZE / BLK_SIZE)
/* The number of blocks required for a given size */
#define BLOCKS_REQ(sz) ((sz / BLK_SIZE) + 1)
/* The maximum number of chunks we can allocate */
#define MAX_CHUNKS     4096
/* The block index of an address */
#define BLK_INDEX(p)   (p - blocks) / BLK_SIZE

/* A clearer name for gcc's test and set byte */
#define test_and_set_byte(x) !__sync_lock_test_and_set(x, (char)1)

static void *alloc_base;
static char *blkmap;
static void *blocks;

/* The size of the shared memory, in bytes */
static int SHM_SIZE;

typedef struct {
	/* The size of this allocated chunk */
	size_t size;
	/* The start address, in shared memory */
	void *start;
} chunk_t;

static chunk_t *chunklist;

void shm_alloc_init(void *base, size_t size, int master) {
	/* Set up the allocation constants */
	SHM_SIZE = size;
	alloc_base = base;
	blkmap = alloc_base;
	chunklist  = (chunk_t *)(blkmap + N_BLOCKS);
	blocks = (char *)((void *)chunklist + MAX_CHUNKS*sizeof(chunk_t));

	/* If we've declared ourselves the "master" then clear everything out */
	if(master) {
		memset(blkmap, 0, N_BLOCKS);
		memset(chunklist, 0, MAX_CHUNKS * sizeof(chunk_t));
	}
}

void *shm_alloc(size_t size) {
	int req = BLOCKS_REQ(size);
	int i, j, k, coll;

	/* Find a free chunk structure */
	for(i = 0; i < MAX_CHUNKS; i++) {
		/* Atomically check that size is 0 and set it to the new size */
		if(cmp_and_swp(size, &(chunklist[i].size), 0)) {
			/* If it succeeded, then we have the chunk and can continue. */
			break;
		}
		/* If it failed, then someone else stole the chunk and we have to
		   keep looking */
	}

	/* Out of chunks. */
	if(i == MAX_CHUNKS) {
		errno = ENOMEM;
		return NULL;
	}

	/* Collect the number of blocks we need */
	coll = 0;
	for(j = 0; j < N_BLOCKS; j++) {
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
		}
	}

	/* Out of space. */
	if(coll < req) {
		for(k = 1; k <= coll; k++) {
			blkmap[j - coll] = 0;
		}
		errno = ENOMEM;
		return NULL;
	}

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

