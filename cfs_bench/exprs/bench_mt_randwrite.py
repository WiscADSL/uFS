#!/usr/bin/env python3
# encoding: utf-8

import sys
import os
import time

import cfsmt_expr_write as mte_wr
import cfs_test_common as tc


def print_usage():
    print(
        'Usage: {} <ext4 | fsp> [cached] [share] [mpstat] [numapp=]'.format(
            sys.argv[0]))


if len(sys.argv) < 2:
    print_usage()
    sys.exit(1)

cur_is_fsp = None
if 'ext4' in sys.argv[1]:
    cur_is_fsp = False
    cur_dev_name = tc.get_kfs_dev_name()
elif 'fsp' in sys.argv[1]:
    cur_is_fsp = True
else:
    print_usage()
    sys.exit(1)
print('is_fsp? - {}'.format(str(cur_is_fsp)))

cur_is_cached = False
cur_is_no_overlap = True
cur_is_dump_mpstat = False
cur_is_share = False
cur_sync_op = 4
cur_numapp = None


if len(sys.argv) >= 3:
    for v in sys.argv[2:]:
        if 'mpstat' in v:
            cur_is_dump_mpstat = True
            continue
        if 'cached' in v:
            cur_is_cached = True
            cur_is_no_overlap = False
            continue
        if 'share' in v:
            cur_is_share = True
            continue
        if 'numapp=' in v:
            cur_numapp = int(v[v.index('=') + 1:])

print('is_cached? - {}'.format(str(cur_is_cached)))
print('is_dump_mpstat? - {}'.format(str(cur_is_dump_mpstat)))
print('is_share_file? - {}'.format(str(cur_is_share)))


LOG_BASE = 'log_{}'.format(sys.argv[1])

num_app_list = [1, 2, 3, 4, 5, 6]
num_app_list = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
# num_app_list = [20 - i for i in range(20)]
if cur_numapp is not None:
    num_app_list = list(range(1, cur_numapp + 1))
    num_app_list.reverse()

if tc.use_exact_num_app():
    num_app_list = [cur_numapp]
print('num_app_list:{}'.format(num_app_list))

for num_app in num_app_list:
    cur_num_fs_wk_list = [(i + 1) for i in range(num_app)]
    # cur_num_fs_wk_list = [num_app]
    cur_cfs_update_dict = None
    if not cur_is_fsp:
        cur_num_fs_wk_list = [1]
    else:
        cur_num_fs_wk_list = list(set([1, num_app]))
    if tc.use_single_worker():
        cur_num_fs_wk_list = [1]
    print(cur_num_fs_wk_list)
    cur_log_dir = tc.get_proj_log_dir(tc.get_expr_user(),
                                      suffix=tc.get_ts_dir_name(),
                                      do_mkdir=True)
    cur_dump_io_stat = False
    if not cur_is_fsp:
        cur_dump_io_stat = True

    # stress sharing
    if cur_is_share:
        per_app_fname = {i: 'bench_f_{}'.format(0) for i in range(num_app)}
        cur_num_fs_wk_list = [1]
    else:
        per_app_fname = {i: 'bench_f_{}'.format(i) for i in range(num_app)}

    # cached random write
    if cur_is_cached:
        CUR_ARKV_DIR = '{}_crwrite_app_{}'.format(LOG_BASE, num_app)
    else:
        # random write (NOTE: needs the size to be okay)
        CUR_ARKV_DIR = '{}_rwrite_app_{}'.format(LOG_BASE, num_app)
        # cur_cfs_update_dict = {
        #    '--sync_numop=': cur_sync_op,
        # }
        # cur_is_no_overlap = True

    mte_wr.bench_rand_write(
        cur_log_dir,
        num_app_proc=num_app,
        is_fsp=cur_is_fsp,
        is_cached=cur_is_cached,
        per_app_fname=per_app_fname,
        dump_iostat=cur_dump_io_stat,
        cfs_update_dict=cur_cfs_update_dict,
        num_fsp_worker_list=cur_num_fs_wk_list)

    os.mkdir(CUR_ARKV_DIR)
    os.system("mv log{}* {}".format(tc.get_year_str(), CUR_ARKV_DIR))
    if not cur_is_fsp:
        os.system("tune2fs -l /dev/{} > {}/kfs_mount_option".format(
            cur_dev_name, CUR_ARKV_DIR))
        tc.dump_kernel_dirty_flush_config(CUR_ARKV_DIR)
    time.sleep(1)

tc.save_default_cfg_config(CUR_ARKV_DIR)
