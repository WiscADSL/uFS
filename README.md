# uFS

![Status](https://img.shields.io/badge/Version-Experimental-green.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

uFS is a filesystem semi-microkernel that designs for device performance delivery and scalability.
We primarily design for modern ultra-fast NVMe devices and multi-core machine. uFS runs as a standalone user-level filesystem process, provides POSIX compatible APIs and crash consistency guarantee, and dynamically adapts to the application demands.

You could learn more about uFS design and semi-microkernel approach in our SOSP'21 paper *[Scale and Performance in a Filesystem Semi-microkernel](https://research.cs.wisc.edu/adsl/Publications/ufs-sosp21.pdf)*.


## Repo Structure

```
uFS
 |---- cfs
         |---- include               # uFS's cpp headers
         |---- lib                   # uFS's dependent libraries
         |---- src                   # uFS's cpp source code
         |---- test                  # tests for uFS, including utility tools like cli and mkfs
         |---- tools
 |---- cfs_bench
         |---- bench                 # cpp source code for uFS's microbench
         |---- exprs                 # scripts to run uFS's experiments
         |---- helpers
         |---- include
         |---- port
         |---- util
```

The other benchmarks in our paper besides microbenchmark (`cfs_bench`) are in a separate repo [uFS-bench](https://github.com/WiscADSL/uFS-bench), including `filebench`, `leveldb` and `scalefs_bench`.

## Get Started

We have tested uFS on Ubuntu 20.04 LTS and Ubuntu 18.04 LTS (both with Linux 5.4). We use `c++20`, `gcc-10`, and `g++-10`. uFS relies on
the user-level NVMe driver provided by SPDK and the version (18.04) is embeded in this repo.

### Download and Build

Please check this [section](https://github.com/WiscADSL/uFS/tree/main/cfs_bench/exprs/artifact_eval#initialization) in artifact evalution document to *setup the environments* and *install necessary dependencies*.
Then to build uFS, try these:

```
# assume all the dependencies have been installed by artifact_eval.sh
cd cfs
mkdir build && cd build
cmake ..
make -j $(proc)              # proc: set it according to core number
```

### A Simple Example

After building uFS, please reboot the machine and we recommand always run `ae init-after-reboot` immediately after booting to setup the pinned memory required by SPDK. This [document](./cfs/test/client/CLI-README.md) illustrates the very basic example to domenstrate uFS is successfully running with our command line tool (CLI).

## Artifact and Experiments of the Paper

We provide full automation for building, running and visualizing our experiments in the SOSP paper [here](https://github.com/WiscADSL/uFS/tree/sosp-21/cfs_bench/exprs/artifact_eval); please check tag [`sosp-21`](https://github.com/WiscADSL/uFS/tree/sosp-21) and tag [`sosp-21-filebench-config`](https://github.com/WiscADSL/uFS/tree/sosp-21-filebench-config) for details.

We thank the anonymous artifact evaluators for giving us feedback.

## Contact

If you have any questions, feel free to open issues at this repository or contact `jingliu [at] cs [dot] wisc [dot] edu` for assistance.
