#!/usr/bin/env python3
# encoding: utf-8

import sys
import os
import time

import cfsmt_expr_meta as mte_meta
import cfs_test_common as tc


def print_usage():
    print(
        'Usage: {} <ext4 | fsp> <dirWidth> [share] [openclose | stat | statonly] [rand] [numapp=]'.format(
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

cur_mkdir_num_op = int(sys.argv[2])
print('dir_width - {}'.format(str(cur_mkdir_num_op)))

cur_is_share = False
listdir_option = 'listdir'
cur_is_random = False
cur_numapp = None

if len(sys.argv) >= 4:
    for v in sys.argv[3:]:
        if 'share' in v:
            cur_is_share = True
            continue
        if 'openclose' in v:
            # if set, will invoke "opendir" benchmark
            listdir_option = "opendir"
            continue
        if 'stat' in v:
            # if set, invoke "listdirinfo1" benchmark (timing the whole)
            listdir_option = "listdirinfo1"
        if 'statonly' in v:
            # if set, invoke "listdirinfo2" benchmark (timing only stat)
            listdir_option = "listdirinfo2"
        if 'rand' in v:
            # will randomlize the file path to access (to avoid the contention)
            cur_is_random = True
        if 'numapp=' in v:
            cur_numapp = int(v[v.index('=') + 1:])


print('is_share? - {}'.format(str(cur_is_share)))
print('listdir_option? - {}'.format(listdir_option))

LOG_BASE = 'log_{}'.format(sys.argv[1])
CUR_WK_TYPE = 'listdir'
TOTAL_MKDIR_NUM_OP = 30000
LISTDIR_NUM_OP = 100000

num_readdir_list = [cur_mkdir_num_op]
cur_listdir_num_op = LISTDIR_NUM_OP

num_app_list = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
#num_app_list = [20 - i for i in range(20)]

if cur_numapp is not None:
    num_app_list = list(range(1, cur_numapp + 1))
    num_app_list.reverse()

if tc.use_exact_num_app():
    num_app_list = [cur_numapp]

for num_app in num_app_list:
    for num_readdir in num_readdir_list:
        cur_num_fs_wk_list = [(i + 1) for i in range(num_app)]
        if not cur_is_fsp:
            cur_num_fs_wk_list = [1]
        if tc.use_single_worker():
            cur_num_fs_wk_list = [1]
        cur_num_fs_wk_list = [num_app]
        print(cur_num_fs_wk_list)
        cur_log_dir = tc.get_proj_log_dir(tc.get_expr_user(),
                                          suffix=tc.get_ts_dir_name(),
                                          do_mkdir=True)
        # cur_mkdir_num_op = int(TOTAL_MKDIR_NUM_OP / num_app)
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
        # first mkdir
        if cur_is_share:
            cur_num_app = 1
        else:
            cur_num_app = num_app
        mte_meta.bench_mkdir(
            cur_log_dir,
            num_app_proc=cur_num_app,
            is_fsp=cur_is_fsp,
            num_op=cur_mkdir_num_op,
            is_create=True,
            num_fsp_worker_list=cur_num_fs_wk_list,
            per_app_name_prefix=per_app_name_prefix,
            dump_iostat=cur_dump_io_stat,
            cfs_update_dict={"--coordinator=": 0, "--value_size=": 1024}
        )

        cur_random_size = 0
        if cur_is_random:
            cur_random_size = 1
        cur_update_dict = {'--value_random_size=': cur_random_size}

        mte_meta.bench_mkdir(
            cur_log_dir,
            num_app_proc=num_app,
            is_fsp=cur_is_fsp,
            num_op=cur_listdir_num_op,
            listdir_option=listdir_option,
            num_fsp_worker_list=cur_num_fs_wk_list,
            per_app_name_prefix=per_app_name_prefix,
            dump_iostat=cur_dump_io_stat,
            cfs_update_dict=cur_update_dict
        )
        os.mkdir(CUR_ARKV_DIR)
        os.system("mv log{}* {}".format(tc.get_year_str(), CUR_ARKV_DIR))

        # save the mount option for the device to check the kernel FS experiment
        # config
        if not cur_is_fsp:
            os.system("tune2fs -l /dev/{} > {}/kfs_mount_option".format(
                cur_dev_name, CUR_ARKV_DIR))
            tc.dump_kernel_dirty_flush_config(CUR_ARKV_DIR)
        time.sleep(1)

tc.save_default_cfg_config(CUR_ARKV_DIR)
