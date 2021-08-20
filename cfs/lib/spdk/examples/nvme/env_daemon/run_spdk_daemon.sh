#! /bin/bash

# start daemon that uses spdk env to avoid the long launching time when starting fsp

SPDK_SHM_ID=9
sudo ./perf -q 1 -s 1024 -w read -t 2 -c 0x100000000 -i "${SPDK_SHM_ID}"
