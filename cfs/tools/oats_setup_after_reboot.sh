#!/bin/bash

if [ "$(id -u)" -eq 0 ]; then
    echo "run as regular user"
    exit 1
fi

# NOTE: following is only for oats and varies for different hosts
# arebello@oats:~$ lspci | grep -i ssd
# 5e:00.0 Non-Volatile memory controller: Intel Corporation Optane SSD 900P Series
# HUGENODE can be 0 or 1 based on numactl --show
SPDK_SETUP_FILE="${HOME}/workspace/ApparateFS/cfs/lib/spdk/scripts/setup.sh"

if [ ! -f "${SPDK_SETUP_FILE}" ]; then
    echo "Could not find {SPDK_SETUP_FILE}"
    exit 1
fi

sudo HUGEMEM=16384 PCI_WHITELIST="0000:5e:00.0" HUGENODE=0 "${SPDK_SETUP_FILE}"
sudo HUGEMEM=16384 PCI_WHITELIST="0000:5e:00.0" HUGENODE=1 "${SPDK_SETUP_FILE}"

# disable hyperthreading
echo off | sudo tee /sys/devices/system/cpu/smt/control
# disable aslr
echo 0 | sudo tee /proc/sys/kernel/randomize_va_space

# TODO: enable cpu performance mode
