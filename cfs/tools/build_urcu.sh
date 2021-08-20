#! /bin/bash

set -e

CUR_DIR=$(pwd)
cd ../lib/userspace-rcu
./bootstrap
./configure
make -j
sudo make install
sudo ldconfig

cd "${CUR_DIR}"
