#ifndef DISTRIBUTED_H_
#define DISTRIBUTED_H_

#define N_WORKERS 2

/* The Nahanni device */
#define SHM_DEV        "/dev/uio0"
/* The size of the shared memory, in bytes */
#define SHM_SIZE       (1024L * 1024L * 1024L)
/* Where we should map the memory */
#define SHM_LOC        (void *)(1024L * 1024L * 1024L * 1024L)

#ifdef MASTER_APP
#ifdef WORKER_APP
#error "Enable only one of MASTER_APP or WORKER_APP"
#endif /* WORKER_APP */
#define MASTER if(1)
#else /* !MASTER_APP */
#define MASTER if(0)
#endif /* MASTER_APP */

#ifdef WORKER_APP
#ifdef MASTER_APP
#error "Enable only one of MASTER_APP or WORKER_APP"
#endif /* MASTER_APP */
#define WORKER if(1)
#else /* !WORKER_APP */
#define WORKER if(0)
#endif /* WORKER_APP */

void *shm_base;

void shm_init();

#endif /* DISTRIBUTED_H_ */
