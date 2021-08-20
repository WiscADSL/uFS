#!/usr/bin/env python3
# encoding: utf-8

from sarge import run, Capture
import sys
import os
import time

from cfs_test_common import start_fs_proc
from cfs_test_common import get_microbench_bin
from cfs_test_common import shutdown_fs

import cfs_expr_read as read_expr
import cfs_expr_write as write_expr
import cfs_test_common as common_expr


# will run this three workloads in order
# Sequential write (init data, actually append), on-disk random read


def print_usage():
    print('Usage: {} ext4|ext4_nj|fsp <numFiles>'.format(sys.argv[0]))


##########################################################################
# script
# init data files for mt-experiments
##########################################################################


if len(sys.argv) < 2:
    print_usage()
    sys.exit(1)

cur_is_fsp = None
if 'ext4' in sys.argv[1]:
    cur_is_fsp = False
elif 'fsp' in sys.argv[1]:
    cur_is_fsp = True
else:
    print_usage()
    sys.exit(1)

# 4G file
# BENCH_FSIZE = 4096 * 1024 * 1024
LOG_BASE = 'log_{}'.format(sys.argv[1])
BENCH_FSIZE = (5 * 1024 * 1024 * 1024)
NUM_FILES = 10

if len(sys.argv) == 3:
    NUM_FILES = int(sys.argv[2])

cur_numop = int(BENCH_FSIZE / 4096)

print('Init data files for benchmarking - BENCH_FSIZE(GB):{} NUM_FILES:{}'.
      format(BENCH_FSIZE / (5 * 1024 * 1024 * 1024), NUM_FILES))

if cur_is_fsp:
    common_expr.expr_mkfs()
else:
    common_expr.expr_mkfs_for_kfs()

common_expr.write_fsp_cfs_config_file(
    split_policy=0,
    dirtyFlushRatio=0.9)

# init data file
for i in range(NUM_FILES):
    cur_log_dir = common_expr.get_proj_log_dir(
        common_expr.get_expr_user(),
        suffix=common_expr.get_ts_dir_name())
    write_expr.expr_write_1fsp_1t(
        cur_log_dir,
        is_seq=True,
        cur_numop=cur_numop,
        cur_value_size=4096,
        is_fsp=cur_is_fsp,
        cfg_update_dict={
            '--fname=': 'bench_f_{}'.format(i),
            '--fs_worker_key_list=': 1})
    if cur_is_fsp:
        common_expr.fsp_do_offline_checkpoint()

    time.sleep(2)

CUR_ARKV_DIR = '{}_mt_dataprep'.format(LOG_BASE)

if not os.path.exists(CUR_ARKV_DIR):
    os.mkdir(CUR_ARKV_DIR)
os.system("mv log{}* {}".format(common_expr.get_year_str(), CUR_ARKV_DIR))

# clean the root-permissioned shms
if cur_is_fsp:
    os.system("rm -rf /dev/shm/fsp_*")
