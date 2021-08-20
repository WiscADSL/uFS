#! /usr/bin/env python3
"""
Prep data for load management microbenchmarks

NOTE:
- this will do mkfs (i.e., cleanup all the data on device),
    create a bunch of files and then write to them
- please run as root

"""

from cfs_test_common import get_microbench_bin
from cfs_test_common import start_bench_coordinator
from cfs_test_common import check_if_process_running
from cfs_test_common import get_fsmain_bin
from cfs_test_common import expr_mkfs

import os
import sys
import time
import subprocess
import shlex

#######
# Global Variables
#######

## binary path+names
BENCH_BIN = get_microbench_bin()
FS_MAIN_BIN = get_fsmain_bin()
EXIT_FILE_NAME = '/tmp/cfs_exit'

KEY_LIST = [1, 11, 21, 31, 41, 51, 61, 71, 81, 91]
NUM_APP = 10

GB = 1024 * 1024 * 1024
MB = 1024 * 1024
KB = 1024

## for lb
LB_PER_APP_NF_DICT = {
    # aid: (file size, # of files)
    0: (6 * MB, 200),
    1: (6 * MB, 200),
    2: (6 * MB, 200),
    3: (6 * MB, 100),
    4: (6 * MB, 100),
    5: (6 * MB, 100),
}

## for nc
NC_PER_APP_NF_DICT = {
    # aid: (file size, # of files)
    0: (6 * MB, 100),
    1: (6 * MB, 100),
    2: (6 * MB, 100),
    3: (6 * MB, 100),
    4: (6 * MB, 100),
    5: (6 * MB, 100),
    6: (6 * MB, 100),
    7: (6 * MB, 100),
}

#######
# Helper Functions
#######


def prep_lm_read_files():
    wid = 0
    cur_bmap_wid = 0
    print(PER_APP_NF_DICT)
    for aid in PER_APP_NF_DICT.keys():
        for fno in range(PER_APP_NF_DICT[aid][1]):
            cur_fname = 'readn_a{}_f{}'.format(aid, fno)
            print('cur_fname:{}'.format(cur_fname))
            cur_key_list = [str(k + wid) for k in KEY_LIST]
            cur_key_str = ','.join(cur_key_list)
            cur_bmap_wid = (cur_bmap_wid + 1) % 10
            cur_num_op = int(PER_APP_NF_DICT[aid][0] / 4096)
            cur_sync_op = min([cur_num_op, 32])
            cur_cmd = '{} --threads=1 --benchmarks=seqwrite --value_size=4096 --sync_numop={} --numop={} \
                    --fs_worker_key_list={} --fname={} --core_ids=11 --wid={} --signowait=1'.format(
                BENCH_BIN, cur_sync_op, cur_num_op, cur_key_str, cur_fname,
                cur_bmap_wid)
            print(cur_cmd)
            start_bench_coordinator()
            ret = subprocess.call(cur_cmd, shell=True)
            if ret != 0:
                print('ERROR prep_lm_read_files')
                exit(1)
            time.sleep(1)


def prep_lm_write_files():
    num_app = 6
    num_write_files = 200  # each app 200 files
    cur_key_list = [str(k) for k in KEY_LIST]
    cur_key_str = ','.join(cur_key_list)
    for aid in range(num_app):
        cur_cmd_dict = {
            '--threads=': 1,
            '--benchmarks=': 'create',
            '--dir=': 'db',
            '--numop=': num_write_files,
            '--fs_worker_key_list=': cur_key_str,
            '--core_ids=': 11 + aid,
            '--value_size=': 0,  # init empty file
            '--dirfile_name_prefix=': '{}_a{}_f'.format('writesync', aid)
        }
        cur_args = ' '.join(k + str(v) for k, v in cur_cmd_dict.items())
        cur_cmd = '{} {}'.format(BENCH_BIN, cur_args)
        print(cur_cmd)
        start_bench_coordinator()
        ret = subprocess.call(cur_cmd, shell=True)
        if ret != 0:
            print('ERROR prep_lm_write_files')
            exit(1)
        time.sleep(1)


def print_prep_lm_str(s):
    print('  >>>>>>> {} >>>>>>>  '.format(s))


def print_usage(argv0):
    print('Usage: {} <lb|nc>'.format(argv0))


#######
# Running
#######

if len(sys.argv) == 2:
    if sys.argv[1] == 'lb':
        PER_APP_NF_DICT = LB_PER_APP_NF_DICT
    elif sys.argv[1] == 'nc':
        PER_APP_NF_DICT = NC_PER_APP_NF_DICT
    else:
        print_usage(sys.argv[0])
        exit(1)
else:
    print_usage(sys.argv[0])
    exit(1)

# mkfs
expr_mkfs()

# start uServer
os.system('rm -f {}'.format(EXIT_FILE_NAME))
cur_fsp_cmd = '{} 10 10 1,11,21,31,41,51,61,71,81,91 {} no.conf 1,2,3,4,5,6,7,8,9,10'.\
        format(FS_MAIN_BIN, EXIT_FILE_NAME)
print(cur_fsp_cmd)
p_fsp = subprocess.Popen(shlex.split(cur_fsp_cmd))
## wait for uServer to fully inited
time.sleep(15)

## double check fsMain is running
if not check_if_process_running(FS_MAIN_BIN.split('/')[-1]):
    print('fsMain is not running. Start it by: {}'.format(
        'fsMain 10 10 1,11,21,31,41,51,61,71,81,91 /tmp/cfs_exit no.conf 1,2,3,4,5,6,7,8,9,10'
    ))
    sys.exit(1)

print_prep_lm_str('prep files for reading')
prep_lm_read_files()
print_prep_lm_str('prep files for writing')
prep_lm_write_files()

# Shutdown uServer
time.sleep(1)
os.system('touch {}'.format(EXIT_FILE_NAME))
print_prep_lm_str('Wait for fsMain exit~~')
p_fsp.wait()

print_prep_lm_str('fsMain exit, and prep DONE!')
