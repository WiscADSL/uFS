#!/usr/bin/env python3
# encoding: utf-8

import sys
import os
import time

import cfsmt_expr_meta as mte_meta
import cfs_test_common as tc


def print_usage():
    print(
        'Usage: {} <ext4 | fsp> [share | dynamic] [numapp=] [think=]'.format(
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

cur_is_share = False
cur_is_dynamic_benchmark = False
cur_numapp = None
cur_think_nano = 0

if len(sys.argv) >= 3:
    for v in sys.argv[2:]:
        if 'share' in v:
            cur_is_share = True
        if 'dynamic' in v:
            cur_is_dynamic_benchmark = True
        if 'numapp=' in v:
            cur_numapp = int(v[v.index('=') + 1:])
        if 'think=' in v:
            cur_think_nano = int(v[v.index('=') + 1:])


print('is_share? - {}'.format(str(cur_is_share)))
print('is_dynamic_benchmark? - {}'.format(str(cur_is_dynamic_benchmark)))

LOG_BASE = 'log_{}'.format(sys.argv[1])
CUR_WK_TYPE = 'stat1'
CUR_NUM_OP = 1000000

num_app_list = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]

if cur_numapp is not None:
    num_app_list = list(range(1, cur_numapp + 1))
    num_app_list.reverse()


if tc.use_exact_num_app():
    num_app_list = [cur_numapp]

for num_app in num_app_list:
    cur_num_fs_wk_list = [(i + 1) for i in range(num_app)]
    if cur_is_fsp:
        #cur_num_fs_wk_list = list(set([1, num_app]))
        cur_num_fs_wk_list = [num_app]
        if cur_is_share:
            cur_num_fs_wk_list = [1]
    else:
        cur_num_fs_wk_list = [1]
    if tc.use_single_worker():
        cur_num_fs_wk_list = [1]
    print(cur_num_fs_wk_list)
    cur_log_dir = tc.get_proj_log_dir(tc.get_expr_user(),
                                      suffix=tc.get_ts_dir_name(),
                                      do_mkdir=True)
    # dump io stats for kernel fs
    cur_dump_io_stat = False
    if not cur_is_fsp:
        cur_dump_io_stat = True

    per_app_dir_name = None
    if cur_is_share:
        per_app_dir_name = {
            i: 'a/b/c/d/e/DIR/f{}'.format(i) for i in range(num_app)}
    else:
        per_app_dir_name = {
            #    i: 'a/b/c/d/e/DIR{}/f0'.format(i) for i in range(num_app)}
            i: 'a{}/b{}/c{}/d{}/e{}/DIR{}/f0'.format(i, i, i, i, i, i) for i in range(num_app)}
    print(per_app_dir_name)
    CUR_ARKV_DIR = '{}_{}_app_{}'.format(LOG_BASE, CUR_WK_TYPE, num_app)

    mte_meta.bench_stat(
        cur_log_dir,
        num_app_proc=num_app,
        is_fsp=cur_is_fsp,
        num_op=CUR_NUM_OP,
        is_dynamic_benchmark=cur_is_dynamic_benchmark,
        num_fsp_worker_list=cur_num_fs_wk_list,
        per_app_dir_name=per_app_dir_name,
        dump_iostat=cur_dump_io_stat,
        dump_mpstat=False,
        cfs_update_dict={'--think_nano=':cur_think_nano}
        #perf_cmd='perf stat -d '
    )

    os.mkdir(CUR_ARKV_DIR)
    os.system("mv log{}* {}".format(tc.get_year_str(), CUR_ARKV_DIR))
    # save the mount option for the device to check the kernel FS experiment
    # config
    if not cur_is_fsp:
        os.system("tune2fs -l /dev/{} > {}/kfs_mount_option".format(
            cur_dev_name, CUR_ARKV_DIR))
        tc.dump_kernel_dirty_flush_config(CUR_ARKV_DIR)

    time.sleep(2)

tc.save_default_cfg_config(CUR_ARKV_DIR)
