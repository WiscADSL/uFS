#!/bin/sh

# create a directory to for resources
mkdir folly_and_dependencies && cd folly_and_dependencies

# install required packages
sudo apt-get install -y \
    g++ \
    cmake \
    libboost-all-dev \
    libevent-dev \
    libdouble-conversion-dev \
    libgoogle-glog-dev \
    libgflags-dev \
    libiberty-dev \
    liblz4-dev \
    liblzma-dev \
    libsnappy-dev \
    make \
    zlib1g-dev \
    binutils-dev \
    libjemalloc-dev \
    libssl-dev \
    pkg-config \
    libunwind-dev
# if advanced debugging functionality is required
sudo apt-get install -y \
    libunwind8-dev \
    libelf-dev \
    libdwarf-dev

# install fmt
git clone https://github.com/fmtlib/fmt.git && cd fmt
mkdir _build && cd _build
cmake ..
# change to NumCore that is suitable for your environment
make -j
sudo make install
cd ../../

# install folly
wget https://github.com/facebook/folly/archive/v2020.07.20.00.tar.gz
tar -zxvf v2020.07.20.00.tar.gz
cd folly-2020.07.20.00/
mkdir _build && cd _build
cmake ..
# change to NumCore that is suitable for your environment
make -j
sudo make install
