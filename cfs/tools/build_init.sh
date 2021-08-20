#!/usr/bin/env bash

CURR_DIR="`pwd`"
PROJ_ROOT="`dirname $CURR_DIR`"

git submodule update --init
bash ./folly_install.sh

# download and build `fio` required by `spdk`
cd $PROJ_ROOT/lib
git clone https://github.com/axboe/fio
cd $PROJ_ROOT/lib/fio && git checkout fio-3.8
make

# build `spdk`
cd $PROJ_ROOT/lib/spdk
sudo scripts/pkgdep.sh
./configure --with-fio=$PROJ_ROOT/lib/fio   # requires fio compiled in that dir
make
make -f Makefile.sharedlib

# build `tbb`
cd $PROJ_ROOT/lib/tbb
make

# build config4cpp
cd $PROJ_ROOT/lib/config4cpp
make

cd $PROJ_ROOT/build/
cmake ..
make -j
# If fail to find tbb, goto tbb dir to see if the version is correct
# tbb build dir differs according to the gcc version and libc version

# resolve `No free hugepages reported in hugepages`
#echo 1024 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages

# resolve `Could not find NVMe controller`
# Note that the NVMe drive needs to be unmounted first via `umount /dev/<name>`
#cd $PROJ_ROOT/lib/spdk/scripts/
#./setup.sh


