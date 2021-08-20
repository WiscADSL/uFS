#!/usr/bin/env python3
# encoding: utf-8

import sys
import os
import time

import cfsmt_expr_write as mte_wr
import cfs_test_common as tc


def print_usage():
    print(
        'Usage: {} <ext4 | fsp> [append] [share] [numapp=]'.format(
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

cur_is_append = False
cur_is_share = False
cur_numapp = None


if len(sys.argv) >= 3:
    for v in sys.argv[2:]:
        if 'append' in v:
            cur_is_append = True
            continue
        if 'share' in v:
            cur_is_share = True
            continue
        if 'numapp=' in v:
            cur_numapp = int(v[v.index('=') + 1:])

print('is_append? - {}'.format(str(cur_is_append)))
print('is_share? - {}'.format(str(cur_is_share)))

LOG_BASE = 'log_{}'.format(sys.argv[1])
CUR_WK_TYPE = 'seqwrite'
if cur_is_append:
    CUR_WK_TYPE = 'append'

num_app_list = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
#num_app_list = [20 - i for i in range(20)]
if cur_numapp is not None:
    num_app_list = list(range(1, cur_numapp + 1))
    num_app_list.reverse()

#sync_op_list = [4, 8, 16, 32, 64, 128]
sync_op_list = [1, 2, 4, 8, 16]
sync_op_list = [4]

if tc.use_exact_num_app():
    num_app_list = [cur_numapp]

for sync_op in sync_op_list:
    for num_app in num_app_list:
        cur_cfs_update_dict = {
            '--sync_numop=': sync_op,
        }
        CUR_ARKV_DIR = '{}_{}_app_{}'.format(LOG_BASE, CUR_WK_TYPE, num_app)
        # if len(sync_op_list) > 1:
        CUR_ARKV_DIR = '{}_sync-{}'.format(CUR_ARKV_DIR, sync_op)
        cur_num_fs_wk_list = [(num_app - i) for i in range(num_app)]
        if not cur_is_fsp:
            cur_num_fs_wk_list = [1]
        else:
            cur_num_fs_wk_list = [num_app]
        if tc.use_single_worker():
            cur_num_fs_wk_list = [1]
        #cur_num_fs_wk_list = [num_app]
        print(cur_num_fs_wk_list)
        if cur_is_share:
            per_app_fname = {i: 'bench_f_{}'.format(0) for i in range(num_app)}
            cur_cfs_update_dict['--share_mode='] = 1
            cur_num_fs_wk_list = [1]
            if cur_is_append:
                cur_cfs_update_dict['--o_append='] = 1
        else:
            per_app_fname = {i: 'bench_f_{}'.format(i) for i in range(num_app)}

        cur_log_dir = tc.get_proj_log_dir(tc.get_expr_user(),
                                          suffix=tc.get_ts_dir_name(),
                                          do_mkdir=True)
        mte_wr.bench_seq_sync_write(
            cur_log_dir,
            num_app_proc=num_app,
            is_fsp=cur_is_fsp,
            is_append=cur_is_append,
            per_app_fname=per_app_fname,
            dump_iostat=(
                not cur_is_fsp),
            num_fsp_worker_list=cur_num_fs_wk_list,
            cfs_update_dict=cur_cfs_update_dict)
        os.mkdir(CUR_ARKV_DIR)
        os.system("mv log{}* {}".format(tc.get_year_str(), CUR_ARKV_DIR))

        if not cur_is_fsp:
            os.system("tune2fs -l /dev/{} > {}/kfs_mount_option".format(
                cur_dev_name, CUR_ARKV_DIR))
            tc.dump_kernel_dirty_flush_config(CUR_ARKV_DIR)

        time.sleep(2)

# save this conf
tc.save_default_cfg_config(CUR_ARKV_DIR)
