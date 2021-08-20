#! /usr/bin/env python3

import os
from sarge import run, Capture
import subprocess
import sys
import time

assert(len(sys.argv) == 2)

JOB = sys.argv[1]
# 	(read, write, randread, randwrite, rw, randrw)]

# note: see this as reference: https://spdk.io/doc/app_overview.html#cpu_mask


RW_RATIO = 0.5

def get_core_mask_str(nthreads):
    cur_str = '['
    for i in range(nthreads):
        cur_str += str(i)
        cur_str += ','
    #cur_str[len(cur_str) - 1] = ']'
    cur_str = cur_str[0:-1]
    cur_str += ']'
    return cur_str


def drop_page_cache():
    cmd = 'sync; echo 3 > /proc/sys/vm/drop_caches'
    p = run(cmd)
    assert(p.returncode == 0)


for NT in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
    CORE_MASK = get_core_mask_str(NT)
    for QD in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 16, 32]:
        for SZ in [4096, 4096 * 4]:
            cur_cmd = r'./perf -q {} -s {} -w {} -t 5 -c {}'.format(
                QD, SZ, JOB, CORE_MASK)
            if JOB == 'rw' or JOB == 'randrw':
                cur_cmd = '{} -M {}'.format(cur_cmd, RW_RATIO)
            print(cur_cmd)
            sys.stdout.flush()
            drop_page_cache()
            result = subprocess.check_output(cur_cmd, shell=True)
            print(result.decode("utf-8"))
            sys.stdout.flush()
