#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <openssl/sha.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/select.h>
#include <errno.h>
#include "ivshmem.h"
#include "synch.h"

#define CHUNK_SZ (1024l*1024l*4l)
#define MSI_VECTOR 0 /* the default vector */

int do_select(int fd);

int main(int argc, char ** argv){

    long num_chunks, length;
    void * memptr, *regptr;
    int * num;
    int fd, rv;
    int i, j, k;
    long param;
    int other;
    mr_lock_t per_thread;
    mr_lock_t parent;

    if (argc != 4){
        printf("USAGE: sum <filename> <param> <other vm>\n");
        exit(-1);
    }

    fd = open(argv[1], O_RDWR);
    printf("[SUM] opening file %s\n", argv[1]);
    param = atol(argv[2]);
    other = atoi(argv[3]);

    printf("[SUM] length is %ld\n", length);

    if ((regptr = mmap(NULL, 256, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0 * getpagesize())) == (void *) -1){
        printf("mmap failed (0x%p)\n", regptr);
        close (fd);
        exit (-1);
    }

    length = CHUNK_SZ;
    if ((memptr = mmap(NULL, length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 1 * getpagesize())) == (void *) -1){
        printf("mmap failed (0x%p)\n", memptr);
        close (fd);
        exit (-1);
    }

    num = (void*) memptr + 1024;
    parent = static_lock_alloc(memptr);
    per_thread = static_lock_alloc_per_thread(memptr, ivshmem_get_posn(regptr), parent);

    printf("sleeping for 10\n");
    lock_acquire(per_thread);
    sleep(10);
    lock_release(per_thread);

    for (i = 0; i < 1024*1024; i++) {
        lock_acquire(per_thread);
        *num = *num + 1;
        lock_release(per_thread);
        if (i % (100 * 1024) == 0)
            printf("%d\n", i);
    }

//    printf("md is *%20s*\n", md);

    munmap(memptr, length);
    munmap(regptr, 256);
    close(fd);

    printf("[SUM] Exiting...\n");
}

int do_select (int fd) {

    fd_set readset;

    FD_ZERO(&readset);
    /* conn socket is in Live_vms at posn 0 */
    FD_SET(fd, &readset);

    select(fd + 1, &readset, NULL, NULL, NULL);

    return 1;

}
