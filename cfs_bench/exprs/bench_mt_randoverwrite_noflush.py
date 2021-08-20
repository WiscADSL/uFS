#!/usr/bin/env python3
# encoding: utf-8

import sys
import os
import time

import cfsmt_expr_write as mte_wr
import cfs_test_common as tc


def print_usage():
    print('Usage: {} <ext4 | fsp> [append]'.format(sys.argv[0]))


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
print('is_fsp? - {}'.format(str(cur_is_fsp)))

cur_is_append = False
if len(sys.argv) >= 3 and 'append' in sys.argv[2]:
    cur_is_append = True

LOG_BASE = 'log_{}'.format(sys.argv[1])

num_app_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
#num_app_list = [9]
for num_app in num_app_list:
    CUR_ARKV_DIR = '{}_randwrite_app_{}'.format(LOG_BASE, num_app)
    cur_num_fs_wk_list = [(i + 1) for i in range(num_app)]
    #cur_num_fs_wk_list = [num_app]
    print(cur_num_fs_wk_list)
    cur_log_dir = tc.get_proj_log_dir(tc.get_expr_user(),
                                      suffix=tc.get_ts_dir_name(),
                                      do_mkdir=True)
    mte_wr.bench_seq_write(
        cur_log_dir,
        num_app_proc=num_app,
        is_fsp=cur_is_fsp,
        is_append=cur_is_append,
        num_fsp_worker_list=cur_num_fs_wk_list)
    os.mkdir(CUR_ARKV_DIR)
    os.system("mv log{}* {}".format(tc.get_year_str(), CUR_ARKV_DIR))
    time.sleep(2)
