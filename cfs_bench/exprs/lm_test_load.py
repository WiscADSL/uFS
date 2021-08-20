#!/usr/bin/env python3
# encoding: utf-8
"""
Test the load status of FSP
Make it easier to add new cases then microbench
Change the offered load at the client side
REQUIRED: set ENABLE_DUMPLOAD for building *cfs_bench*
"""

import sys
import math
import logging
import os
import argparse
import subprocess
import cfs_test_common as cfs_tc
import cfsmt_expr_read as me_read
import time


def get_think_nano_list(verbose=3):
    if verbose == 0:
        tn_list = [0, 10000]
    elif verbose == 1:
        tn_list = [0, 1000, 10000]
    elif verbose == 2:
        tn_list = [0, 100, 500, 1000, 2000, 3000, 4000, 5000, 10000, 20000]
    elif verbose == 3:
        tn_list = [10000]
    else:
        raise RuntimeError('tn verbose not supported')
    return tn_list


def get_total_inum_limit():
    return 60000


def get_per_app_dir_name(numapp, same_dir=True):
    if same_dir:
        return {i: 'db' for i in range(numapp)}
    else:
        raise RuntimeError('must share the dir for now')


def get_per_app_name_prefix(numapp):
    return {i: 'app{}-'.format(i) for i in range(numapp)}


def get_default_fspexpr_kwargs():
    kwargs = {
        'is_fsp': True,
        'clear_pgcache': True,
        'pin_cpu': True,
    }
    return kwargs


def get_log_dir_name(case, numworker, numapp):
    return 'log_loadexpr_{}_NW-{}_NA-{}'.format(case, numworker, numapp)


def do_load_test_for_dyn_mix_common(CASE_NAME,
                                    THINK_NANO,
                                    numworker,
                                    numapp,
                                    per_app_cfg_dict,
                                    per_app_name_prefix_dict,
                                    common_base_dict,
                                    args=None,
                                    extra_think_nano=None):
    NUM_SEG = 1
    app_think_nano_list = {i: [THINK_NANO] * NUM_SEG for i in range(numapp)}
    if extra_think_nano is not None:
        for k, v in extra_think_nano.items():
            app_think_nano_list[k] = [v] * NUM_SEG

    P1 = 8
    P2 = 8
    DO_MANUAL = True
    DO_MOD = False
    if DO_MANUAL:
        P1 = 0
        P2 = 0
    if DO_MOD:
        P1 = 102
        P2 = 0

    for app_idx in range(numapp):
        # per_app_cfg_dict[app_idx]['--wid='] = app_idx % numworker
        cur_num_seg = NUM_SEG
        per_app_cfg_dict[app_idx]['--num_segment='] = cur_num_seg
        if DO_MANUAL:
            per_app_cfg_dict[app_idx]['--wid='] = app_idx
        cur_think_nano_list = app_think_nano_list[app_idx]
        per_app_cfg_dict[app_idx]['--per_segment_think_ns_list='] = ",".join(
            map(str, cur_think_nano_list))

    kwargs = get_default_fspexpr_kwargs()
    kwargs.update({
        'bench_cfg_dict': common_base_dict.copy(),
        'per_app_cfg_dict': per_app_cfg_dict,
        'per_app_name_prefix': per_app_name_prefix_dict,
    })

    cur_arkv_dir = get_log_dir_name(CASE_NAME, numworker, numapp)
    if not os.path.exists(cur_arkv_dir):
        os.mkdir(cur_arkv_dir)

    cur_run_log_dir = cfs_tc.get_proj_log_dir(
        cfs_tc.get_expr_user(),
        suffix='-numseg-{}_sz-{}'.format(NUM_SEG, 4096),
        do_mkdir=True)
    cfs_tc.write_fsp_cfs_config_file(
        split_policy=P1,
        dirtyFlushRatio=0.9,
        serverCorePolicy=P2,
        cgst_ql=args.cgstql)
    me_read.expr_read_mtfsp_multiapp(
        cur_run_log_dir,
        numworker,
        numapp,
        **kwargs,
    )
    os.system("mv log-numseg-* {}".format(cur_arkv_dir))


# this is finally used to setup the data for LB (write)
# basically writesync_a{i}_f (first 100) need to be written to make it overwrite later
def do_load_test_for_dyn_setup_lb_write(numworker, numapp, args=None):
    CASE_NAME = 'mix1'
    SEG_US = 3000000
    THINK_NANO = 3000
    common_dict = {
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
    }

    statn_bench_cfg_dict = {
        '--benchmarks=': 'dynamic_statn',
        '--value_size=': 0,
        '--num_files=': 100,
        '--dir=': 'db',
    }
    statn_bench_cfg_dict.update(common_dict)

    write_sync_cfg_dict = {
        '--benchmarks=': 'dynamic_writesync',
        '--value_size=': int(16 * 1024),
        '--dir=': 'db',
        '--num_files=': 100,
        '--exit_delay_sec=': 10,
        '--max_file_size=': int(8 * 1024 * 1024),
        '--sync_numop=': 0,
    }
    write_sync_cfg_dict.update(common_dict)

    per_app_cfg_dict = {
        0: write_sync_cfg_dict.copy(),
        1: write_sync_cfg_dict.copy(),
        2: write_sync_cfg_dict.copy(),
        3: write_sync_cfg_dict.copy(),
        4: write_sync_cfg_dict.copy(),
        5: write_sync_cfg_dict.copy(),
    }
    per_app_name_prefix_dict = {}

    for i in range(6):
        per_app_cfg_dict[i]['--value_size='] = 16384
        per_app_name_prefix_dict[i] = 'writesync_a{}_f'.format(i)
    do_load_test_for_dyn_mix_common(CASE_NAME, THINK_NANO, numworker, numapp,
                                    per_app_cfg_dict, per_app_name_prefix_dict,
                                    statn_bench_cfg_dict, args)
    time.sleep(1)
    os.system('rm -rf log_loadexpr_mix1_NW*')
    cfs_tc.fsp_do_offline_checkpoint()


def do_load_test(numworker, numapp, case, cmd_args):
    func_name = 'do_load_test_for_{}'.format(case)
    if 'mix' in case:
        globals()[func_name](numworker, numapp, args=cmd_args)
        pass
    elif cmd_args.rwbytes is not None:
        globals()[func_name](numworker, numapp, rwbytes=cmd_args.rwbytes)
    else:
        globals()[func_name](numworker, numapp, args=cmd_args)


def main(args, loglevel):
    logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)
    if not cfs_tc.use_exact_num_app():
        raise RuntimeError("Must manually set use_exact_num_app() to true")
    if args.all:
        do_all_test()
    else:
        do_load_test(args.numworker, args.numapp, args.case, args)


def parse_cmd_args():
    parser = argparse.ArgumentParser(
        description="Test the load condition of FSP")
    parser.add_argument(
        '--numworker', type=int, required=True, help='number of fsp workers')
    parser.add_argument(
        '--numapp', type=int, required=True, help='number of app proc')
    parser.add_argument('--case', required=True, help='case name to run')
    parser.add_argument('--rwbytes', help='read/write size in bytes')
    parser.add_argument('--cgstql', help='congest queue length', default=1.2)
    parser.add_argument('--nf', help='number of files', default=1)
    parser.add_argument(
        '--all', help='run all tests', action='store_true', default=False)
    return (parser.parse_args())


if __name__ == '__main__':
    loglevel = logging.INFO
    args = parse_cmd_args()
    main(args, loglevel)
