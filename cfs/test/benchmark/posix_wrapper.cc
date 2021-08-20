
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include "fsapi.h"

/*
 * A wrapper of posix interface for the counterpart of cfs
 */


int fs_register(void) {
    fprintf(stdout, "fs_register() posix\n");
    return 0;
}
int fs_init(key_t key) {
    fprintf(stdout, "fs_init() posix\n");
    return 0;
}
int fs_open(const char *path, int flags, mode_t mode) {
    //return open(path, flags, mode);
    return open(path, flags);
}
int fs_close(int fd) {
    return close(fd);
}
ssize_t fs_read(int fd, void *buf, size_t count) {
    return read(fd, buf, count);
}
ssize_t fs_pread(int fd, void *buf, size_t count, off_t offset) {
    return pread(fd, buf, count, offset);
}
ssize_t fs_write(int fd, void *buf, size_t count) {
    return write(fd, buf, count);
}
ssize_t fs_allocated_read(int fd, int *idx, void *buf, size_t count) {
    fprintf(stdout, "fs_allocated_read posix\n");
    return 0;
}
ssize_t fs_allocated_write(int fd, int* idx, void *buf, size_t count) {
    fprintf(stdout, "fs_allocated_write\n");
    return 0;
}
void* fs_allocate(size_t size, int *idx_ptr) {
    fprintf(stdout, "fs_allocate\n");
    return 0;
}
int fs_release_buf(int idx) {
    fprintf(stdout, "fs_release_buf\n");
    return 0;
}
