#!/usr/bin/env python3
# encoding: utf-8

import sys
import os
import time

import cfs_test_common as cfs_tc
import cfsmt_expr_read as me_read


def bench_mkdir(log_dir,
                num_app_proc=1,
                is_fsp=True,
                num_op=1000,
                listdir_option=None,
                is_create=False,
                num_fsp_worker_list=None,
                per_app_name_prefix=None,
                per_app_dir_name=None,
                init_dir_tree=True,
                dump_mpstat=False,
                dump_iostat=False,
                perf_cmd=None,
                cfs_update_dict=None):

    # decied which case
    case_name = 'mkdir'
    if listdir_option is not None:
        case_name = listdir_option

    case_log_dir = '{}/{}'.format(log_dir, case_name)
    bench_cfg_dict = {
        '--benchmarks=': case_name,
        '--value_size=': 0,
    }

    if cfs_update_dict is not None:
        bench_cfg_dict.update(cfs_update_dict)

    # target directory names
    if per_app_name_prefix is None:
        # private parent directory
        if per_app_dir_name is None:
            per_app_dir_name = {
                i: 'mkd{}'.format(i) for i in range(num_app_proc)}
    else:
        # shared single parent directory
        if per_app_dir_name is None:
            per_app_dir_name = {
                i: 'mkd{}'.format(0) for i in range(num_app_proc)}
    print(per_app_dir_name)
    print(per_app_name_prefix)

    pin_cpu_list = [True]
    clear_pc_list = [True]
    if num_fsp_worker_list is None:
        num_fsp_worker_list = [1]
    for cp in clear_pc_list:
        for pc in pin_cpu_list:
            for nfswk in num_fsp_worker_list:
                cur_run_log_dir = \
                    '{}_isFsp-{}_clearPc-{}_pinCpu-{}-numFsWk-{}'.format(
                        case_log_dir, str(is_fsp), str(cp), str(pc),
                        str(nfswk))
                cfs_tc.mk_accessible_dir(cur_run_log_dir)

                if listdir_option is None and init_dir_tree:
                    # prepare the benchmarking directory
                    # mkfs
                    if is_fsp:
                        cfs_tc.expr_mkfs()
                    else:
                        cfs_tc.expr_mkfs_for_kfs()
                    # wait a bit after mkfs

                    if per_app_name_prefix is not None:
                        bench_cfg_dict['--numop='] = 1
                    else:
                        bench_cfg_dict['--numop='] = num_app_proc
                    # mkdir the directory as target
                    me_read.expr_read_mtfsp_multiapp(
                        cur_run_log_dir,
                        1,  # num_fs_worker
                        1,  # num_app_proc
                        bench_cfg_dict,
                        is_fsp=is_fsp,
                        per_app_name_prefix={i: 'mkd' for i in range(1)},
                        log_no_save=True
                    )
                    time.sleep(1)

                bench_cfg_dict['--numop='] = num_op
                if is_create:
                    bench_cfg_dict['--benchmarks='] = 'create'
                cur_per_app_name_prefix = None
                if listdir_option is None:
                    cur_per_app_name_prefix = per_app_name_prefix
                me_read.expr_read_mtfsp_multiapp(
                    cur_run_log_dir,
                    nfswk,
                    num_app_proc,
                    bench_cfg_dict,
                    is_fsp=is_fsp,
                    clear_pgcache=cp,
                    pin_cpu=pc,
                    per_app_name_prefix=cur_per_app_name_prefix,
                    per_app_dir_name=per_app_dir_name,
                    perf_cmd=perf_cmd,
                    dump_mpstat=dump_mpstat,
                    dump_iostat=dump_iostat)
                time.sleep(1)


# assume files are created
def bench_unlink(log_dir,
                 num_app_proc=1,
                 is_fsp=True,
                 num_op=1000,
                 is_move=False,
                 num_fsp_worker_list=None,
                 per_app_name_prefix=None,
                 dump_mpstat=False,
                 dump_iostat=False,
                 perf_cmd=None,
                 cfs_update_dict=None):

    # decied which case
    case_name = 'unlink'
    if is_move:
        case_name = 'moveunlink'

    case_log_dir = '{}/{}'.format(log_dir, case_name)
    bench_cfg_dict = {
        '--benchmarks=': case_name,
        '--value_size=': 0,
    }

    if cfs_update_dict is not None:
        bench_cfg_dict.update(cfs_update_dict)

    # target directory names
    if per_app_name_prefix is None:
        per_app_dir_name = {i: 'mkd{}'.format(i) for i in range(num_app_proc)}
    else:
        per_app_dir_name = {i: 'mkd{}'.format(0) for i in range(num_app_proc)}

    pin_cpu_list = [True]
    clear_pc_list = [True]
    if num_fsp_worker_list is None:
        num_fsp_worker_list = [1]
    for cp in clear_pc_list:
        for pc in pin_cpu_list:
            for nfswk in num_fsp_worker_list:
                cur_run_log_dir = \
                    '{}_isFsp-{}_clearPc-{}_pinCpu-{}-numFsWk-{}'.format(
                        case_log_dir, str(is_fsp), str(cp), str(pc),
                        str(nfswk))
                cfs_tc.mk_accessible_dir(cur_run_log_dir)

                bench_cfg_dict['--numop='] = num_op
                cur_per_app_name_prefix = per_app_name_prefix
                me_read.expr_read_mtfsp_multiapp(
                    cur_run_log_dir,
                    nfswk,
                    num_app_proc,
                    bench_cfg_dict,
                    is_fsp=is_fsp,
                    clear_pgcache=cp,
                    pin_cpu=pc,
                    per_app_name_prefix=cur_per_app_name_prefix,
                    per_app_dir_name=per_app_dir_name,
                    perf_cmd=perf_cmd,
                    dump_mpstat=dump_mpstat,
                    dump_iostat=dump_iostat)
                time.sleep(1)


def bench_rename(log_dir,
                 num_app_proc=1,
                 is_fsp=True,
                 num_op=1000,
                 is_move=False,
                 num_fsp_worker_list=None,
                 per_app_name_prefix=None,
                 dump_mpstat=False,
                 dump_iostat=False,
                 perf_cmd=None,
                 cfs_update_dict=None):

    # decied which case
    case_name = 'rename'

    case_log_dir = '{}/{}'.format(log_dir, case_name)
    bench_cfg_dict = {
        '--benchmarks=': case_name,
        '--value_size=': 0,
    }

    if cfs_update_dict is not None:
        bench_cfg_dict.update(cfs_update_dict)

    # target directory names
    if per_app_name_prefix is None:
        per_app_dir_name = {i: 'mkd{}'.format(i) for i in range(num_app_proc)}
        per_app_dir2_name = {i: 'mkdDST{}'.format(
            i) for i in range(num_app_proc)}
    else:
        per_app_dir_name = {i: 'mkd{}'.format(0) for i in range(num_app_proc)}
        per_app_dir2_name = {i: 'mkdDST{}'.format(
            0) for i in range(num_app_proc)}

    pin_cpu_list = [True]
    clear_pc_list = [True]
    if num_fsp_worker_list is None:
        num_fsp_worker_list = [1]
    for cp in clear_pc_list:
        for pc in pin_cpu_list:
            for nfswk in num_fsp_worker_list:
                cur_run_log_dir = \
                    '{}_isFsp-{}_clearPc-{}_pinCpu-{}-numFsWk-{}'.format(
                        case_log_dir, str(is_fsp), str(cp), str(pc),
                        str(nfswk))
                cfs_tc.mk_accessible_dir(cur_run_log_dir)

                # prepare the benchmarking directory
                # mkfs
                if is_fsp:
                    cfs_tc.expr_mkfs()
                else:
                    cfs_tc.expr_mkfs_for_kfs()
                # wait a bit after mkfs

                bench_cfg_dict['--numop='] = num_app_proc
                bench_cfg_dict['--benchmarks='] = 'mkdir'
                # mkdir the directory as target
                me_read.expr_read_mtfsp_multiapp(
                    cur_run_log_dir,
                    1,  # num_fs_worker
                    1,  # num_app_proc
                    bench_cfg_dict,
                    is_fsp=is_fsp,
                    per_app_name_prefix={i: 'mkd' for i in range(1)},
                    log_no_save=True
                )
                me_read.expr_read_mtfsp_multiapp(
                    cur_run_log_dir,
                    1,  # num_fs_worker
                    1,  # num_app_proc
                    bench_cfg_dict,
                    is_fsp=is_fsp,
                    per_app_name_prefix={i: 'mkdDST' for i in range(1)},
                    log_no_save=True
                )
                time.sleep(1)

                bench_cfg_dict['--numop='] = num_op
                bench_cfg_dict['--benchmarks='] = 'create'
                bench_cfg_dict['--coordinator='] = 0
                cur_per_app_name_prefix = per_app_name_prefix
                me_read.expr_read_mtfsp_multiapp(
                    cur_run_log_dir,
                    nfswk,
                    num_app_proc,
                    bench_cfg_dict,
                    is_fsp=is_fsp,
                    clear_pgcache=cp,
                    pin_cpu=pc,
                    per_app_name_prefix=cur_per_app_name_prefix,
                    per_app_dir_name=per_app_dir_name,
                    perf_cmd=perf_cmd,
                    log_no_save=True,
                    dump_mpstat=dump_mpstat,
                    dump_iostat=dump_iostat)

                bench_cfg_dict['--coordinator='] = 1
                bench_cfg_dict['--numop='] = num_op
                bench_cfg_dict['--benchmarks='] = 'rename'
                bench_cfg_dict['--value_size='] = 1
                me_read.expr_read_mtfsp_multiapp(
                    cur_run_log_dir,
                    nfswk,
                    num_app_proc,
                    bench_cfg_dict,
                    is_fsp=is_fsp,
                    clear_pgcache=cp,
                    pin_cpu=pc,
                    per_app_name_prefix=cur_per_app_name_prefix,
                    per_app_dir_name=per_app_dir_name,
                    per_app_dir2_name=per_app_dir2_name,
                    perf_cmd=perf_cmd,
                    dump_mpstat=dump_mpstat,
                    dump_iostat=dump_iostat)

                time.sleep(1)


def bench_stat(log_dir,
               num_app_proc=1,
               is_fsp=True,
               num_op=1000,
               is_openclosedir=False,
               is_dynamic_benchmark=False,
               num_fsp_worker_list=None,
               # if None, then all the apps share a directory
               per_app_dir_name=None,
               dump_mpstat=False,
               dump_iostat=False,
               perf_cmd=None,
               cfs_update_dict=None):
    case_name = 'stat1'
    if is_dynamic_benchmark:
        case_name = 'stat1_dynamic'
    case_log_dir = '{}/{}'.format(log_dir, case_name)

    bench_cfg_dict = {
        '--benchmarks=': case_name,
        '--value_size=': 0,
        '--numop=': num_op,
    }

    if cfs_update_dict is not None:
        bench_cfg_dict.update(cfs_update_dict)

    assert(per_app_dir_name is not None)

    pin_cpu_list = [True]
    clear_pc_list = [True]
    if num_fsp_worker_list is None:
        num_fsp_worker_list = [1]
    for cp in clear_pc_list:
        for pc in pin_cpu_list:
            for nfswk in num_fsp_worker_list:
                cur_run_log_dir = \
                    '{}_isFsp-{}_clearPc-{}_pinCpu-{}-numFsWk-{}'.format(
                        case_log_dir, str(is_fsp), str(cp), str(pc),
                        str(nfswk))
                cfs_tc.mk_accessible_dir(cur_run_log_dir)

                # prepare the benchmarking directory
                # mkfs
                if is_fsp:
                    if '--think_micro=' not in bench_cfg_dict:
                        cfs_tc.expr_mkfs()
                else:
                    cfs_tc.expr_mkfs_for_kfs()
                # wait a bit after mkfs

                me_read.expr_read_mtfsp_multiapp(
                    cur_run_log_dir,
                    nfswk,
                    num_app_proc,
                    bench_cfg_dict,
                    is_fsp=is_fsp,
                    clear_pgcache=cp,
                    pin_cpu=pc,
                    per_app_dir_name=per_app_dir_name,
                    perf_cmd=perf_cmd,
                    dump_mpstat=dump_mpstat,
                    dump_iostat=dump_iostat)
                time.sleep(1)
