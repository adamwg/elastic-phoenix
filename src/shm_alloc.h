#ifndef SHM_ALLOC_H_
#define SHM_ALLOC_H_

void shm_alloc_init(void *, size_t, int);
void *shm_alloc(size_t);
void shm_free(void *);

#endif /* SHM_ALLOC_H_ */
