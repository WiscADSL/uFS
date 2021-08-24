
#include <stdio.h>
#include "spdk/nvme.h"


int main(int argc, char **argv){
    struct spdk_env_opts opts;
    fprintf(stderr, "size of spdk_env_opts:%ld\n", sizeof(opts));
    return 0;
}
