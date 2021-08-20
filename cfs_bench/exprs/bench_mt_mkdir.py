#!/usr/bin/env python3
# encoding: utf-8

import sys
import os
import time

import cfsmt_expr_meta as mte_meta
import cfs_test_common as tc


def print_usage():
    print(
        'Usage: {} <ext4 | fsp> [create] [share] [numapp=]'.format(
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
cur_is_create = False
cur_numapp = None
cur_perf_cmd = None
# cur_perf_cmd = 'perf stat -d '
# cur_perf_cmd = 'perf record -vv -e context-switches -c 1 '
# cur_perf_cmd = 'perf ftrace'

if len(sys.argv) >= 3:
    for v in sys.argv[2:]:
        if 'share' in v:
            cur_is_share = True
            continue
        if 'create' in v:
            cur_is_create = True
            continue
        if 'numapp=' in v:
            cur_numapp = int(v[v.index('=') + 1:])


print('is_share? - {}'.format(str(cur_is_share)))


LOG_BASE = 'log_{}'.format(sys.argv[1])
CUR_WK_TYPE = 'mkdir'
if cur_is_create:
    CUR_WK_TYPE = 'create'
TOTAL_NUM_OP = 60000

num_app_list = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
#num_app_list = [20 - i for i in range(20)]

if cur_numapp is not None:
    num_app_list = list(range(1, cur_numapp + 1))
    num_app_list.reverse()

if tc.use_exact_num_app():
    num_app_list = [cur_numapp]

for num_app in num_app_list:
    cur_num_fs_wk_list = [(i + 1) for i in range(num_app)]
    # if not cur_is_fsp:
    #    cur_num_fs_wk_list = [1]
    cur_num_fs_wk_list = [num_app]
    if tc.use_single_worker():
        cur_num_fs_wk_list = [1]
    print(cur_num_fs_wk_list)
    cur_log_dir = tc.get_proj_log_dir(tc.get_expr_user(),
                                      suffix=tc.get_ts_dir_name(),
                                      do_mkdir=True)
    cur_num_op = int(TOTAL_NUM_OP / num_app)
    # dump io stats for kernel fs
    cur_dump_io_stat = False
    if not cur_is_fsp:
        cur_dump_io_stat = True

    per_app_name_prefix = None
    if cur_is_share:
        per_app_name_prefix = {
            i: 'app{}-'.format(
                str(i)) for i in range(num_app)}
    print(per_app_name_prefix)
    CUR_ARKV_DIR = '{}_{}_app_{}'.format(LOG_BASE, CUR_WK_TYPE, num_app)

    mte_meta.bench_mkdir(
        cur_log_dir,
        num_app_proc=num_app,
        is_fsp=cur_is_fsp,
        is_create=cur_is_create,
        num_op=cur_num_op,
        num_fsp_worker_list=cur_num_fs_wk_list,
        per_app_name_prefix=per_app_name_prefix,
        dump_iostat=cur_dump_io_stat,
        perf_cmd=cur_perf_cmd
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
