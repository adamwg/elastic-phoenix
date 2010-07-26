#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "distributed.h"
#include "stddefines.h"

/* The Nahanni device */
#define SHM_DEV        "/dev/uio0"
/* The size of the shared memory, in bytes */
#define SHM_SIZE       (1024L * 1024L * 1024L)
/* Where we should map the memory */
#define SHM_LOC        (void *)(1024L * 1024L * 1024L * 1024L)

void shm_init() {
	int fd;

	/* Map the shmem region */
	fd = open(SHM_DEV, O_RDWR);
	CHECK_ERROR(fd < 0);
	shm_base = mmap(SHM_LOC, SHM_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 1*getpagesize());
	CHECK_ERROR(shm_base == MAP_FAILED);
}
