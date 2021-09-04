#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""analyze the lb behavior

- it effectively solve the problem of load balancing

"""

import sys
import math
from typing import List
import numpy as np
from numpy.lib.function_base import median
import pandas as pd
import logging
import os
import argparse
import subprocess
import cfs_test_common as cfs_tc
import cfsmt_expr_read as me_read
from itertools import combinations


# import warnings
# warnings.filterwarnings("error")


def get_attr_from_item(item, attr_name, deli, tp):
    return tp(item[len(attr_name + deli):])


def get_default_fspexpr_kwargs():
    kwargs = {
        'is_fsp': True,
        'clear_pgcache': True,
        'pin_cpu': True,
    }
    return kwargs


def get_log_dir_name(case, numworker, numapp):
    return 'log_loadexpr_{}_NW-{}_NA-{}'.format(case, numworker, numapp)


def get_special_cfg(expr_no, args):
    cgst_ql = args.cgstql
    nw = args.num_worker
    na = args.num_app
    if expr_no == 101:
        if not args.max_perf:
            nw = 3
    elif expr_no == 200:
        cgst_ql = 1.3
    elif expr_no == 201:
        cgst_ql = 1.3
    elif expr_no == 202:
        # na = 4
        cgst_ql = 1.1
        if not args.max_perf:
            nw = 4
    elif expr_no == 203:
        cgst_ql = 1.3
    elif expr_no == 204:
        cgst_ql = 1.1
        if not args.max_perf:
            nw = 5
    elif expr_no == 300:
        na = 10
        nw = 5
        cgst_ql = 1.5
        if args.max_perf:
            nw = na
    return cgst_ql, nw, na


class AppCfg(object):
    def __init__(self, job_str, size, app_idx, expr_no, seg_us, write_idx=None):
        self.job_str = job_str
        assert job_str in AppCfg.get_valid_job_list()
        self.size = size
        self.app_idx = app_idx
        self.expr_no = expr_no
        self.write_idx = write_idx
        self.seg_us = seg_us
        self.cfg = None

    def get_str(self):
        cur_job_str = self.job_str.split('_')[1]
        return '{}-{}'.format(cur_job_str, self.size)

    def has_io(self):
        if 'readlbio' in self.job_str or 'writesync' in self.job_str:
            return True
        return False

    def is_stat(self):
        if 'statn' in self.job_str:
            return True
        return False

    def get_prefix_str(self):
        if 'writesync' in self.job_str or 'sync' in self.job_str or 'write' in self.job_str:
            assert self.write_idx is not None
            cur_str = 'writesync_a{}_f'.format(self.write_idx)
            return cur_str
        if 'overwrite' in self.job_str:
            assert self.write_idx is not None
            cur_str = 'writesync_a{}_f'.format(self.write_idx)
            return cur_str
        if 'statn' in self.job_str:
            return 'statn_a0_f'
        return None

    def get_cfg_dict(self):
        if self.cfg is not None:
            return self.cfg
        return AppCfg.job_cfg_example(self.job_str, self.seg_us, self.size)

    @staticmethod
    def get_valid_job_list():
        return ['dynamic_statn', 'dyn_readlbio', 'dyn_readlbio2',
                'dyn_readlb', 'dynamic_writesync', 'dynamic_overwrite', 'dyn_readlbnc',
                'dyn_read_iomem', 'dynamic_fixsync', 'dyn_writelbnc']

    @staticmethod
    def get_common_cfg(seg_us):
        return AppCfg.job_cfg_example('common_base', seg_us, 0)

    @staticmethod
    def job_cfg_example(job_str, seg_us, value_size):
        common_dict = {
            # normal field
            '--threads=': 1,
            '--histogram=': 1,
            '--segment_us=': seg_us,
            '--numop=': 0,
        }
        statn_bench_cfg_dict = {
            '--benchmarks=': 'dynamic_statn',
            '--value_size=': 0,
            '--dir=': 'db',
        }
        statn_bench_cfg_dict.update(common_dict)

        on_disk_read_cfg_dict = {
            '--benchmarks=': 'dyn_readlbio',
            '--num_files=': 200,
            '--value_size=': value_size,
        }
        if value_size == 4096:
            on_disk_read_cfg_dict['--in_mem_file_size='] = int(4*1024*1024)
        else:
            on_disk_read_cfg_dict['--in_mem_file_size='] = int(6*1024*1024)
        on_disk_read_cfg_dict.update(common_dict)

        in_mem_read_cfg_dict = {
            '--benchmarks=': 'dyn_readlb',
            '--num_files=': 40,
            '--value_size=': value_size,
            '--in_mem_file_size=': int(1*1024*1024),
        }
        # makes this hotter
        if value_size != 4096:
            in_mem_read_cfg_dict['--num_files='] = 10

        in_mem_read_cfg_dict.update(common_dict)

        write_sync_cfg_dict = {
            '--benchmarks=': 'dynamic_writesync',
            '--value_size=': value_size,
            '--dir=': 'db',
            '--num_files=': 120,
            '--max_file_size=': int(8*1024*1024),
            '--sync_numop=': 0,
        }
        write_sync_cfg_dict.update(common_dict)
        if job_str == 'dynamic_statn':
            statn_bench_cfg_dict['--num_files='] = 20
            return statn_bench_cfg_dict
        elif job_str == 'dyn_readlbio':
            return on_disk_read_cfg_dict
        elif job_str == 'dyn_readlb':
            return in_mem_read_cfg_dict
        elif job_str == 'dynamic_writesync':
            return write_sync_cfg_dict
        elif job_str == 'common_base':
            # must-have field
            common_dict['--benchmarks='] = 'dynamic_statn'
            common_dict['--value_size='] = 0
            return common_dict
        else:
            raise RuntimeError('Error job not supported - ' + job_str)


class ExprCfg(object):
    def __init__(self, nw, expr_no, num_rpt, seg_us):
        self.nw = nw
        self.expr_no = expr_no
        self.num_rpt = num_rpt
        self.seg_us = seg_us
        self.app_cfg_list = []
        self.num_stat = 0
        self.num_large = 0
        self.think_nano = None
        self.per_app_think_nano = None
        self.per_core_ut = None
        self.bypass_exit_sync = False

    def add_app_cfg(self, app_cfg):
        app_cfg.app_idx = len(self.app_cfg_list)
        self.app_cfg_list.append(app_cfg)
        if app_cfg.is_stat():
            self.num_stat += 1
        if app_cfg.size > 8192:
            self.num_large += 1

    def display(self):
        print('---cfg think:{}'.format(self.think_nano))
        for app in self.app_cfg_list:
            print(app.job_str)
        print('----')

    def next_idx(self):
        return len(self.app_cfg_list)

    def num_app(self):
        return len(self.app_cfg_list)

    def num_io_app(self):
        num = 0
        for app in self.app_cfg_list:
            if app.has_io():
                num += 1
        return num

    def set_bypass_exit_sync(self, b):
        self.bypass_exit_sync = b

    def num_stat_app(self):
        return self.num_stat

    def set_per_app_think_nano(self, think_dict):
        self.per_app_think_nano = think_dict

    def get_per_app_think_nano(self):
        return self.per_app_think_nano

    def set_think_nano(self, nano):
        self.think_nano = nano

    def get_think_nano(self):
        if self.think_nano is not None:
            return self.think_nano
        num_io_app = self.num_io_app()
        cur_nano = None
        if num_io_app == 4:
            cur_nano = 3000
            if self.num_large >= 3:
                cur_nano += 100*(self.num_large - 2)
            return cur_nano
        if num_io_app == 3:
            cur_nano = 5000
            return cur_nano
        if num_io_app == 2:
            cur_nano = 7000
            return cur_nano
        raise RuntimeError('num_io_app wrong')

    def get_per_app_cfg_dict(self):
        cur_app_cfg_dict = {}
        for app_cfg in self.app_cfg_list:
            cur_app_cfg_dict[app_cfg.app_idx] = app_cfg.get_cfg_dict()
        return cur_app_cfg_dict

    def get_per_app_name_prefix(self):
        prefix_dict = {}
        for app_cfg in self.app_cfg_list:
            if app_cfg.get_prefix_str() is not None:
                prefix_dict[app_cfg.app_idx] = app_cfg.get_prefix_str()
        return prefix_dict

    def print_str(self):
        cur_str = '{} -'.format(self.expr_no)
        for cur_item in self.app_cfg_list:
            cur_str += (' ' + cur_item.get_str())
        return cur_str


def run_one_expr(expr_cfg, args):
    CASE_NAME = str(expr_cfg.expr_no)
    NUM_SEG = 1
    app_think_nano_list = expr_cfg.get_per_app_think_nano()
    if app_think_nano_list is None:
        app_think_nano_list = {
            i: [expr_cfg.get_think_nano()]*NUM_SEG for i in range(expr_cfg.num_app())}

    P1 = 8
    P2 = 8
    DO_MANUAL = args.max_perf
    DO_MOD = args.use_mod
    DO_NC = args.nc

    if DO_NC:
        P1 = 9
        P2 = 9
    if DO_MANUAL:
        P1 = 0
        P2 = 0
    if DO_MOD:
        P1 = 102
        P2 = 0

    per_app_cfg_dict = expr_cfg.get_per_app_cfg_dict()
    assert per_app_cfg_dict is not None
    per_app_name_prefix_dict = expr_cfg.get_per_app_name_prefix()
    for app_idx in range(expr_cfg.num_app()):
        cur_think_nano_list = app_think_nano_list[app_idx]
        cur_num_seg = len(cur_think_nano_list)
        per_app_cfg_dict[app_idx]['--num_segment='] = cur_num_seg
        if DO_MANUAL:
            per_app_cfg_dict[app_idx]['--wid='] = app_idx
        per_app_cfg_dict[app_idx]['--per_segment_think_ns_list='] = ",".join(
            map(str, cur_think_nano_list))

    kwargs = get_default_fspexpr_kwargs()
    kwargs.update({
        'bench_cfg_dict': AppCfg.get_common_cfg(expr_cfg.seg_us),
        'per_app_cfg_dict': per_app_cfg_dict,
        'per_app_name_prefix': per_app_name_prefix_dict,
    })
    if expr_cfg.bypass_exit_sync:
        kwargs.update({'bypass_exit_sync':True})

    for rpt_no in range(args.num_rpt):
        cur_arkv_dir = get_log_dir_name(
            'no-{}_ql-{}_rpt-{}'.format(CASE_NAME, args.cgstql, rpt_no), expr_cfg.nw, expr_cfg.num_app())
        if not os.path.exists(cur_arkv_dir):
            os.mkdir(cur_arkv_dir)

        cur_run_log_dir = cfs_tc.get_proj_log_dir(
            cfs_tc.get_expr_user(),
            suffix='-numseg-{}'.format(NUM_SEG),
            do_mkdir=True)
        if DO_NC:
            assert expr_cfg.per_core_ut is not None
            cfs_tc.write_fsp_cfs_config_file(
                split_policy=P1, dirtyFlushRatio=0.9, serverCorePolicy=P2,
                percore_ut=expr_cfg.per_core_ut, cgst_ql=args.cgstql)
        else:
            cfs_tc.write_fsp_cfs_config_file(
                split_policy=P1, dirtyFlushRatio=0.9, serverCorePolicy=P2, cgst_ql=args.cgstql)
        me_read.expr_read_mtfsp_multiapp(
            cur_run_log_dir,
            expr_cfg.nw,
            expr_cfg.num_app(),
            **kwargs,
        )
        os.system("mv log-numseg-* {}".format(cur_arkv_dir))


def do_lb_readonly_a(args, gen_cfg=False):
    NA = 6
    NW = 4
    if args.max_perf:
        NW = 6
    SEG_US = 1500000
    THINK_NANO = 4500
    Ssz = 4096
    expr_no = 100
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)
    on_disk_read_cfg_dict = {
        '--benchmarks=': 'dyn_readlbio2',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--num_files=': 200,
        '--num_hot_files=': 40,
        '--in_mem_file_size=': int(6*1024*1024),
        '--value_size=': Ssz,
    }
    in_mem_read_cfg_dict = {
        '--benchmarks=': 'dyn_readlb',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--num_files=': 40,
        '--value_size=': Ssz,
        '--in_mem_file_size=': int(1*1024*1024),
    }

    cfg_list = []

    on_disk_read_num = 3
    #args.cgstql = 1.1
    for i in range(on_disk_read_num):
        app_cfg = AppCfg('dyn_readlbio2', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US)
        app_cfg.cfg = on_disk_read_cfg_dict.copy()
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)

    in_mem_read_num = NA - on_disk_read_num
    for i in range(in_mem_read_num):
        app_cfg = AppCfg('dyn_readlb', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US)
        app_cfg.cfg = in_mem_read_cfg_dict.copy()
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)

    if gen_cfg:
        return cfg_list

    expr_cfg.set_think_nano(THINK_NANO)
    run_one_expr(expr_cfg, args)


def do_lb_readonly_b(args, gen_cfg=False):
    NA = 6
    NW = 3
    if args.max_perf:
        NW = 6
    SEG_US = 1500000
    THINK_NANO = 2000
    Ssz = 4096
    Lsz = 16384
    expr_no = 101
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)
    on_disk_read_cfg_dict = {
        '--benchmarks=': 'dyn_readlbio2',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--num_files=': 200,
        '--num_hot_files=': 40,
        '--in_mem_file_size=': int(6*1024*1024),
        '--value_size=': Lsz,
    }

    # ql = 1.1
    large_num = 3
    small_num = NA - large_num
    cfg_list = []
    for i in range(large_num):
        app_cfg = AppCfg('dyn_readlbio2', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US)
        app_cfg.cfg = on_disk_read_cfg_dict.copy()
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)
    for i in range(small_num):
        app_cfg = AppCfg('dyn_readlbio2', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US)
        cur_cfg = on_disk_read_cfg_dict.copy()
        cur_cfg['--num_files='] = 100
        cur_cfg['--value_size='] = Ssz
        app_cfg.cfg = cur_cfg
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)
    if gen_cfg:
        return cfg_list
    expr_cfg.set_think_nano(THINK_NANO)
    run_one_expr(expr_cfg, args)


def do_lb_readonly_c(args, gen_cfg=False):
    NA = 6
    NW = 4
    if args.max_perf:
        NW = 6
    SEG_US = 1500000
    THINK_NANO = 5500
    Ssz = 4096
    expr_no = 102
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)

    in_mem_read_cfg_dict = {
        '--benchmarks=': 'dyn_readlb',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--num_files=': 40,
        '--value_size=': Ssz,
        '--in_mem_file_size=': int(1*1024*1024),
    }

    # ql = 1.1
    cold_num = 3
    hot_num = NA - cold_num
    cfg_list = []
    for i in range(cold_num):
        app_cfg = AppCfg('dyn_readlb', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US)
        cur_cfg = in_mem_read_cfg_dict.copy()
        if i == 0:
            cfg_list.append(app_cfg)
        app_cfg.cfg = cur_cfg
        expr_cfg.add_app_cfg(app_cfg)

    for i in range(hot_num):
        app_cfg = AppCfg('dyn_readlb', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US)
        if i == 0:
            cfg_list.append(app_cfg)
        cur_cfg = in_mem_read_cfg_dict.copy()
        cur_cfg['--num_files='] = 5
        app_cfg.cfg = cur_cfg
        expr_cfg.add_app_cfg(app_cfg)
    if gen_cfg:
        return cfg_list
    expr_cfg.set_think_nano(THINK_NANO)
    run_one_expr(expr_cfg, args)


def do_lb_readonly_d(args, gen_cfg=False):
    NA = 6
    NW = 4
    if args.max_perf:
        NW = 6
    SEG_US = 1500000
    THINK_NANO = 4000
    expr_no = 103
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)

    a_cfg_list = do_lb_readonly_a(args, gen_cfg=True)
    b_cfg_list = do_lb_readonly_b(args, gen_cfg=True)
    c_cfg_list = do_lb_readonly_c(args, gen_cfg=True)
    cfg_list = a_cfg_list + b_cfg_list + c_cfg_list
    if gen_cfg:
        return cfg_list
    for app_cfg in cfg_list:
        expr_cfg.add_app_cfg(app_cfg)
    expr_cfg.set_think_nano(THINK_NANO)
    expr_cfg.display()
    run_one_expr(expr_cfg, args)


def do_lb_writeonly_a(args, gen_cfg=False):
    NA = 6
    NW = 4
    if args.max_perf:
        NW = 6
    SEG_US = 1500000
    #THINK_NANO = 13000
    THINK_NANO = 8000
    Ssz = 4096
    Lsz = 16384
    expr_no = 200
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)
    write_sync_cfg_dict = {
        '--benchmarks=': 'dynamic_writesync',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--dir=': 'db',
        '--num_files=': 100,
        '--num_hot_files=': 0,
        '--sync_numop=': 1,
        '--max_file_size=': int(8*1024*1024),
        '--value_size=': Lsz,
    }

    # ql = 1.3
    # I think the reason for this larger ql is
    # 1. the write+sync workload generates something like write and immediately sync
    # which break the access distribution
    # 2. the randomness from IO
    large_num = 3
    small_num = NA - large_num
    cfg_list = []
    write_idx = 0
    for i in range(large_num):
        app_cfg = AppCfg('dynamic_writesync', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US, write_idx=write_idx)
        app_cfg.cfg = write_sync_cfg_dict.copy()
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)
        write_idx += 1
    for i in range(small_num):
        app_cfg = AppCfg('dynamic_writesync', Lsz,
                         expr_cfg.next_idx(), expr_no, SEG_US, write_idx=write_idx)
        cur_cfg = write_sync_cfg_dict.copy()
        cur_cfg['--value_size='] = Ssz
        app_cfg.cfg = cur_cfg
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)
        write_idx += 1
    if gen_cfg:
        return cfg_list
    expr_cfg.set_think_nano(THINK_NANO)
    run_one_expr(expr_cfg, args)


def do_lb_writeonly_b(args, gen_cfg=False):
    NA = 6
    NW = 4
    if args.max_perf:
        NW = 6
    SEG_US = 1000000
    if gen_cfg:
        SEG_US = 1500000
    # THINK_NANO = 13000
    #THINK_NANO = 8000
    THINK_NANO = 10000
    Ssz = 4096
    expr_no = 201
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)
    write_sync_cfg_dict = {
        '--benchmarks=': 'dynamic_writesync',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--dir=': 'db',
        '--num_files=': 100,
        # no-sync then
        '--sync_numop=': 0,
        '--num_hot_files=': 0,
        '--max_file_size=': int(8*1024*1024),
        '--value_size=': Ssz,
    }

    # ql = 1.1
    # ql = 1.2 works
    overwrite_num = 3
    append_num = NA - overwrite_num
    cfg_list = []
    write_idx = 0
    for i in range(overwrite_num):
        app_cfg = AppCfg('dynamic_writesync', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US, write_idx=write_idx)
        app_cfg.cfg = write_sync_cfg_dict.copy()
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)
        write_idx += 1
    for i in range(append_num):
        app_cfg = AppCfg('dynamic_writesync', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US, write_idx=write_idx)
        cur_cfg = write_sync_cfg_dict.copy()
        # this is append
        cur_cfg['--num_hot_files='] = 100
        app_cfg.cfg = cur_cfg
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)
        write_idx += 1
    if gen_cfg:
        return cfg_list
    expr_cfg.set_bypass_exit_sync(True)
    expr_cfg.set_think_nano(THINK_NANO)
    run_one_expr(expr_cfg, args)


def do_lb_writeonly_c(args, gen_cfg=False):
    NA = 6
    NW = 4
    if args.max_perf:
        NW = 6
    SEG_US = 1500000
    THINK_NANO = 9000
    Ssz = 4096
    expr_no = 202
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)
    write_sync_cfg_dict = {
        '--benchmarks=': 'dynamic_overwrite',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--dir=': 'db',
        '--num_files=': 10,
        '--in_mem_file_size=': int(1*1024*1024),
        '--value_size=': Ssz,
    }

    # ql = 1.3
    hot_num = 3
    cold_num = NA - hot_num
    cfg_list = []
    write_idx = 0
    for i in range(hot_num):
        app_cfg = AppCfg('dynamic_overwrite', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US, write_idx=write_idx)
        app_cfg.cfg = write_sync_cfg_dict.copy()
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)
        write_idx += 1
    for i in range(cold_num):
        app_cfg = AppCfg('dynamic_overwrite', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US, write_idx=write_idx)
        cur_cfg = write_sync_cfg_dict.copy()
        cur_cfg['--num_files='] = 80
        app_cfg.cfg = cur_cfg
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)
        write_idx += 1
    if gen_cfg:
        return cfg_list
    expr_cfg.set_think_nano(THINK_NANO)
    run_one_expr(expr_cfg, args)


def do_lb_writeonly_e(args, gen_cfg=False):
    NA = 6
    NW = 5
    if args.max_perf:
        NW = 6
    SEG_US = 1500000
    THINK_NANO = 5000
    Ssz = 4096
    expr_no = 204
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)
    write_sync_cfg_dict = {
        '--benchmarks=': 'dynamic_overwrite',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--dir=': 'db',
        '--num_files=': 10,
        '--in_mem_file_size=': int(1*1024*1024),
        '--value_size=': Ssz,
    }

    # ql = 1.3
    hot_num = 3
    cold_num = NA - hot_num
    cfg_list = []
    write_idx = 0
    for i in range(hot_num):
        app_cfg = AppCfg('dynamic_overwrite', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US, write_idx=write_idx)
        app_cfg.cfg = write_sync_cfg_dict.copy()
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)
        write_idx += 1
    for i in range(cold_num):
        app_cfg = AppCfg('dynamic_overwrite', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US, write_idx=write_idx)
        cur_cfg = write_sync_cfg_dict.copy()
        cur_cfg['--value_size='] = 16384
        app_cfg.cfg = cur_cfg
        expr_cfg.add_app_cfg(app_cfg)
        if i == 0:
            cfg_list.append(app_cfg)
        write_idx += 1
    if gen_cfg:
        return cfg_list
    expr_cfg.set_think_nano(THINK_NANO)
    run_one_expr(expr_cfg, args)


def do_lb_writeonly_d(args, gen_cfg=False):
    NA = 6
    NW = 4
    if args.max_perf:
        NW = 6
    SEG_US = 1500000
    THINK_NANO = 13000
    #THINK_NANO = 12000

    expr_no = 203
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)

    # ql = 1.3
    # small vs. large
    a_cfg_list = do_lb_writeonly_a(args, gen_cfg=True)
    # append vs. overwrite
    b_cfg_list = do_lb_writeonly_b(args, gen_cfg=True)
    # hot vs. cold
    c_cfg_list = do_lb_writeonly_c(args, gen_cfg=True)

    # because append can only be the last 3
    cfg_list = a_cfg_list + c_cfg_list + b_cfg_list
    i = 0
    for app_cfg in cfg_list:
        app_cfg.write_idx = i
        expr_cfg.add_app_cfg(app_cfg)
        i += 1
    if gen_cfg:
        return a_cfg_list + b_cfg_list
    expr_cfg.set_bypass_exit_sync(True)
    expr_cfg.set_think_nano(THINK_NANO)
    expr_cfg.display()
    run_one_expr(expr_cfg, args)


def do_lb_readwrite_a(args, gen_cfg=False):
    NA = 10
    NW = 5
    # ql = 1.5 works well
    if args.max_perf:
        NW = 10
    SEG_US = 1500000
    THINK_NANO = 15000
    expr_no = 300
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)

    read_cfg_list = do_lb_readonly_d(args, gen_cfg=True)
    write_cfg_list = do_lb_writeonly_d(args, gen_cfg=True)
    cfg_list = read_cfg_list + write_cfg_list
    print(len(write_cfg_list))
    for app_cfg in cfg_list:
        expr_cfg.add_app_cfg(app_cfg)
    expr_cfg.set_think_nano(THINK_NANO)
    expr_cfg.display()
    run_one_expr(expr_cfg, args)


def do_nc_1(args):
    expr_no = 1001
    NW = 8
    SEG_US = 1000000
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)
    # in-mem vs. on-disk
    # read_a_cfg_list = do_lb_readonly_a(args, gen_cfg=True)
    # small vs. large (on-disk)
    read_b_cfg_list = do_lb_readonly_b(args, gen_cfg=True)
    assert(len(read_b_cfg_list) == 2)
    for rb_cfg in read_b_cfg_list:
        rb_cfg.cfg['--segment_us='] = SEG_US
        rb_cfg.cfg['--num_files='] = 200
        expr_cfg.add_app_cfg(rb_cfg)
        rb_cfg.cfg['--exit_delay_sec='] = 10
    # hot vs. cold
    read_c_cfg_list = do_lb_readonly_c(args, gen_cfg=True)
    assert(len(read_c_cfg_list) == 2)
    for rc_cfg in read_c_cfg_list:
        rc_cfg.cfg['--segment_us='] = SEG_US
        rc_cfg.cfg['--exit_delay_sec='] = 10
        expr_cfg.add_app_cfg(rc_cfg)
    # write + sync: small vs. large
    write_idx = 2
    write_a_cfg_list = do_lb_writeonly_a(args, gen_cfg=True)
    assert(len(write_a_cfg_list) == 2)
    for wa_cfg in write_a_cfg_list:
        wa_cfg.cfg['--benchmarks='] = 'dynamic_fixsync'
        wa_cfg.cfg['--segment_us='] = SEG_US
        wa_cfg.cfg['--exit_delay_sec='] = 10
        # wa_cfg.cfg['--max_file_size='] = int(4*1024*1024)
        wa_cfg.cfg['--in_mem_file_size='] = int(1*1024*1024)
        wa_cfg.write_idx = write_idx
        expr_cfg.add_app_cfg(wa_cfg)
        write_idx += 1
    # overwrite + append: this should be only run one seg
    write_b_cfg_list = do_lb_writeonly_b(args, gen_cfg=True)
    for wb_cfg in write_b_cfg_list:
        wb_cfg.write_idx = write_idx
        wb_cfg.cfg['--segment_us='] = SEG_US
        wb_cfg.cfg['--exit_delay_sec='] = 10
        expr_cfg.add_app_cfg(wb_cfg)
        write_idx += 1
    assert(len(write_b_cfg_list) == 2)
    # hot vs. cold
    # write_c_cfg_list = do_lb_writeonly_c(args, gen_cfg=True)

    # NOTE: 0.4, 1.2 turns out work well

    LONG = 2000000
    per_app_think_nano = {
        # large on-disk read
        0: [LONG, LONG, LONG, LONG, LONG, 50000, 50000, 50000, 100000],
        # small on-disk read
        1: [LONG, LONG, LONG, LONG, 20000, 20000, 20000, 20000, 40000],
        # in-memory cold read
        2: [LONG, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 12000],
        # in-memory hot read
        3: [5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 12000],
        # write-sync large
        4: [LONG, LONG, LONG, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 10000],
        # write-sync small
        5: [LONG, LONG, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 4000, 10000],
        # overwrite in-mem
        6: [LONG, LONG, LONG, LONG, LONG, LONG, LONG, 40000, 80000],
        # append in-mem
        7: [LONG, LONG, LONG, LONG, LONG, LONG, 40000, 40000, 80000],
    }
    expr_cfg.set_bypass_exit_sync(True)
    expr_cfg.set_per_app_think_nano(per_app_think_nano)
    expr_cfg.per_core_ut = 0.4
    run_one_expr(expr_cfg, args)

# a increase number of in-mem operations


def do_nc_2(args):
    expr_no = 1002
    SEG_US = 1000000
    NW = 6
    NA = 6
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)

    Ssz = 4096
    read_cfg_dict = {
        '--benchmarks=': 'dyn_read_iomem',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--num_files=': 40,
        '--value_size=': Ssz,
        '--in_mem_file_size=': int(1*1024*1024),
        '--num_hot_files=': 20,
        '--exit_delay_sec=': 10,
        # '--rw_align_bytes=': 0,
    }

    SHORT = 2000
    # ql=1.15 seems work
    if not args.burst:
        #num_seg = 17
        num_seg = 19
        per_app_think_nano = {i: [SHORT]*num_seg for i in range(NA)}
        rw_bws_dict = {
            0: 101,
            1: 101,
            2: 101,
            3: 101,
            4: 101,
            5: 101,
        }
    else:
        #num_seg = 7
        num_seg = 7
        per_app_think_nano = {i: [SHORT]*num_seg for i in range(NA)}
        rw_bws_dict = {
            0: 102,
            1: 102,
            2: 102,
            3: 102,
            4: 102,
            5: 102,
        }

    for aid in range(NA):
        app_cfg = AppCfg('dyn_read_iomem', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US)
        app_cfg.cfg = read_cfg_dict.copy()
        expr_cfg.add_app_cfg(app_cfg)
        app_cfg.cfg['--rw_align_bytes='] = rw_bws_dict[aid]

    expr_cfg.set_per_app_think_nano(per_app_think_nano)
    expr_cfg.per_core_ut = 0.4
    run_one_expr(expr_cfg, args)


def do_nc_3(args):
    expr_no = 1003
    SEG_US = 1000000
    NW = 6
    NA = 6
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)

    #Ssz = 16384
    Ssz = 4096
    read_cfg_dict = {
        '--benchmarks=': 'dyn_readlb',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--num_files=': 40,
        # '--dir=': 'db',
        '--value_size=': Ssz,
        '--sync_numop=': 1,
        '--in_mem_file_size=': int(1*1024*1024),
        # '--num_hot_files=': 20,
        '--exit_delay_sec=': 10,
    }

    # ql=1.1
    if not args.burst:
        #cur_seg_think_list = list(range(37000, 4999, -1000))
        cur_seg_think_list = [15000, 12000, 10000,
                              8000, 6000, 5000, 4000, 3000, 2500, 2000]
        # cur_seg_think_list = list(range(10000, 0, -4000))
        cur_seg_think_list = cur_seg_think_list + \
            list(reversed(cur_seg_think_list))
        per_app_think_nano = {i: cur_seg_think_list for i in range(NA)}
        pass
    else:
        #cur_seg_think_list = list(range(37000, 4999, -8000))
        # cur_seg_think_list = [10000, 0, -2000]
        cur_seg_think_list = [15000, 7000, 2000]
        cur_seg_think_list = cur_seg_think_list + \
            list(reversed(cur_seg_think_list))
        per_app_think_nano = {i: cur_seg_think_list for i in range(NA)}

    for aid in range(NA):
        app_cfg = AppCfg('dyn_readlb', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US, write_idx=aid)
        app_cfg.cfg = read_cfg_dict.copy()
        expr_cfg.add_app_cfg(app_cfg)

    expr_cfg.set_per_app_think_nano(per_app_think_nano)
    expr_cfg.per_core_ut = 0.3
    run_one_expr(expr_cfg, args)


# add one app at a time
def do_nc_4(args):
    expr_no = 1004
    SEG_US = 1000000
    NW = 6
    NA = 6
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)

    #Ssz = 16384
    Ssz = 4096
    read_cfg_dict = {
        '--benchmarks=': 'dyn_readlb',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--num_files=': 40,
        # '--dir=': 'db',
        '--value_size=': Ssz,
        '--sync_numop=': 1,
        '--in_mem_file_size=': int(1*1024*1024),
        # '--num_hot_files=': 20,
        '--exit_delay_sec=': 10,
    }

    # ql=1.1
    LONG = 2000000
    SHORT = 6000
    if not args.burst:
        per_app_think_nano = {
            0: [LONG, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT],
            1: [LONG, LONG, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT],
            2: [LONG, LONG, LONG, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT, SHORT],
            3: [LONG, LONG, LONG, LONG, SHORT, SHORT, SHORT, SHORT, SHORT],
            4: [LONG, LONG, LONG, LONG, LONG, SHORT, SHORT, SHORT],
            5: [LONG, LONG, LONG, LONG, LONG, LONG,  SHORT]
        }
    else:
        #cur_seg_think_list = list(range(37000, 4999, -8000))
        # cur_seg_think_list = [10000, 0, -2000]
        per_app_think_nano = {
            0: [LONG, SHORT, SHORT, SHORT],
            1: [LONG, SHORT, SHORT, SHORT],
            2: [LONG, SHORT, SHORT, SHORT],
            3: [LONG, LONG, SHORT, LONG],
            4: [LONG, LONG, SHORT, LONG],
            5: [LONG, LONG, SHORT, LONG],
        }

    for aid in range(NA):
        app_cfg = AppCfg('dyn_readlb', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US, write_idx=aid)
        app_cfg.cfg = read_cfg_dict.copy()
        expr_cfg.add_app_cfg(app_cfg)

    expr_cfg.set_per_app_think_nano(per_app_think_nano)
    expr_cfg.per_core_ut = 0.4
    run_one_expr(expr_cfg, args)


# incease/decrease the cost by changing the read/write size
def do_nc_5(args):
    NW = 6
    NA = 6
    SEG_US = 1000000
    expr_no = 1005
    expr_cfg = ExprCfg(NW, expr_no, args.num_rpt, SEG_US)

    Ssz = 4096
    read_cfg_dict = {
        '--benchmarks=': 'dyn_writelbnc',
        '--threads=': 1,
        '--histogram=': 1,
        '--segment_us=': SEG_US,
        '--numop=': 0,
        '--num_files=': 40,
        '--value_size=': Ssz,
        '--dir=': 'db',
        '--in_mem_file_size=': int(1*1024*1024),
        '--exit_delay_sec=': 10,
        '--rw_align_bytes=': 2,
    }

    SHORT = 8000
    if not args.burst:
        #read_cfg_dict['--rw_align_bytes='] = int((32-4) * 1024)
        num_seg = 17
        read_cfg_dict['--rw_align_bytes='] = 8192
        per_app_think_nano = {i: [SHORT]*num_seg for i in range(NA)}
    else:
        num_seg = 5
        read_cfg_dict['--rw_align_bytes='] = 32768
        per_app_think_nano = {i: [SHORT]*num_seg for i in range(NA)}

    for aid in range(NA):
        app_cfg = AppCfg('dyn_writelbnc', Ssz,
                         expr_cfg.next_idx(), expr_no, SEG_US, write_idx=aid)
        app_cfg.cfg = read_cfg_dict.copy()
        expr_cfg.add_app_cfg(app_cfg)

    expr_cfg.set_per_app_think_nano(per_app_think_nano)
    expr_cfg.per_core_ut = 0.4
    run_one_expr(expr_cfg, args)


def do_lb_test_combination(args, print_only=False):
    NA = 6
    NW = 4
    if args.max_perf:
        NW = 6
    SEG_US = 1500000
    expr_cfg_list = []
    Lsz = 16384
    Ssz = 4096
    expr_no = 0
    for num_disk_read in [1, 2]:
        for num_disk_read_large in range(0, num_disk_read+1):
            for num_write_sync in [1, 2]:
                for num_write_sync_large in range(0, num_write_sync+1):
                    for num_stat in [0, 1]:
                        num_in_mem_read = NA - num_disk_read - num_write_sync - num_stat
                        for num_in_mem_read_large in range(1, num_in_mem_read):
                            cur_expr_cfg = ExprCfg(
                                NW, expr_no, args.num_rpt, SEG_US)
                            for i in range(num_disk_read):
                                if i < num_disk_read_large:
                                    cur_expr_cfg.add_app_cfg(
                                        AppCfg('dyn_readlbio', Lsz, cur_expr_cfg.next_idx(), expr_no, SEG_US))
                                else:
                                    cur_expr_cfg.add_app_cfg(
                                        AppCfg('dyn_readlbio', Ssz, cur_expr_cfg.next_idx(), expr_no, SEG_US))
                            for i in range(num_write_sync):
                                if i < num_write_sync_large:
                                    cur_expr_cfg.add_app_cfg(
                                        AppCfg('dynamic_writesync', Lsz, cur_expr_cfg.next_idx(), expr_no, SEG_US, write_idx=i))
                                else:
                                    cur_expr_cfg.add_app_cfg(
                                        AppCfg('dynamic_writesync', Ssz, cur_expr_cfg.next_idx(), expr_no, SEG_US, write_idx=i))
                            for i in range(num_stat):
                                cur_expr_cfg.add_app_cfg(
                                    AppCfg('dynamic_statn', 0, cur_expr_cfg.next_idx(), expr_no, SEG_US))
                            for i in range(num_in_mem_read):
                                if i < num_in_mem_read_large:
                                    cur_expr_cfg.add_app_cfg(
                                        AppCfg('dyn_readlb', Lsz, cur_expr_cfg.next_idx(), expr_no, SEG_US))
                                else:
                                    cur_expr_cfg.add_app_cfg(
                                        AppCfg('dyn_readlb', Ssz, cur_expr_cfg.next_idx(), expr_no, SEG_US))
                            expr_cfg_list.append(cur_expr_cfg)
                            # print
                            if print_only:
                                print(cur_expr_cfg.print_str())
                            expr_no += 1

    if print_only:
        return expr_cfg_list
    if args.expr_no is not None:
        cur_expr_cfg = expr_cfg_list[args.expr_no]
        print(cur_expr_cfg.print_str())
        run_one_expr(cur_expr_cfg, args)
    else:
        for expr_cfg in expr_cfg_list:
            if args.num_io_app is not None and expr_cfg.num_io_app() != args.num_io_app:
                continue
            if args.num_stat is not None and expr_cfg.num_stat_app() != args.num_stat:
                continue
            run_one_expr(expr_cfg, args)


def get_one_log_stable_time(fname):
    first_nano = None
    last_nano = None
    cur_nano = None
    # print(fname)
    lb_nano_list = []
    diff_nano_list = []
    with open(fname) as f:
        for line in f:
            line = line.strip()
            items = line.split()
            if '[KPLoadStatsSummary]' not in line and 'LbPlan' not in line:
                continue
            if 'invalidateAppShmByName' in line:
                break
            if 'LbPlan' in line:
                last_nano = cur_nano
                # print(line)
                assert(cur_nano is not None)
                lb_nano_list.append(cur_nano)
                if len(lb_nano_list) > 1:
                    diff_nano_list.append(lb_nano_list[-1] - lb_nano_list[-2])
            if 'cpu_ut:' in line:
                items = line.split()
                nano_item = items[2]
                nano = int(nano_item[len('real_nano:'):])
                if first_nano is None:
                    first_nano = nano
                if first_nano is not None and (nano - first_nano > 2*1e9):
                    break
                cur_nano = nano
    stable_lb_idx = None
    for idx in range(1, len(diff_nano_list)):
        if diff_nano_list[idx] > 10*diff_nano_list[idx-1]:
            stable_lb_idx = idx
            break
    # print(lb_nano_list)
    # print(diff_nano_list)
    # print(stable_lb_idx)
    if stable_lb_idx is not None:
        last_nano = lb_nano_list[stable_lb_idx]
    return last_nano - first_nano


def process_fsp_log_cpu(fname, sample_k=None, cal_mean_sec_range=None):
    per_worker_series_dict = {}
    first_nano = None
    per_worker_idx = {}
    with open(fname) as f:
        for line in f:
            line = line.strip()
            if '[KPLoadStatsSummary]' not in line:
                continue
            if 'invalidateAppShmByName' in line:
                break
            if 'BlkDevSpdk' in line:
                continue
            if '[warning]' in line:
                continue
            if 'FsProc.cc' in line:
                continue
            if 'cpu_ut:' in line:
                items = line.split()
                wid_item = items[1]
                nano_item = items[2]
                utilization_item = items[-3]
                # print(line)
                wid = int(wid_item[len('wid:'):])
                nano = int(nano_item[len('real_nano:'):])
                if first_nano is None:
                    first_nano = nano
                utilization = float(
                    utilization_item[len('cpu_ut:'):])
                if wid not in per_worker_series_dict:
                    per_worker_series_dict[wid] = ([], [])
                    per_worker_idx[wid] = 0

                if sample_k is not None and per_worker_idx[wid] % sample_k != 0:
                    per_worker_idx[wid] += 1
                    continue
                cur_sec = (nano - first_nano)*1e-9
                if cal_mean_sec_range is not None and cur_sec > cal_mean_sec_range:
                    break
                per_worker_series_dict[wid][0].append((nano - first_nano)*1e-9)
                per_worker_series_dict[wid][1].append(utilization)
                per_worker_idx[wid] += 1
    if cal_mean_sec_range is not None:
        num_worker = len(per_worker_series_dict)
        per_worker_avg = {}
        for wid, wid_ut_tp in per_worker_series_dict.items():
            per_worker_avg[wid] = cfs_tc.compute_avg(wid_ut_tp[1])
        total_ut = sum(list(per_worker_avg.values()))
        mean_ut = total_ut / num_worker
        return mean_ut, total_ut, num_worker
    else:
        return per_worker_series_dict


def process_one_bench_log(fname):
    time_sec_list = []
    num_op_list = []
    case = None
    total_sec = 0
    total_done = 0
    with open(fname) as f:
        bucket_us = None
        cur_idx = 0
        for line in f:
            line = line.strip()
            items = line.split()
            if 'cfs_bench --benchmarks' in line:
                case_item = items[1]
                case = get_attr_from_item(case_item, '--benchmarks', '=', str)
            if 'segment_bucket_us:' in line:
                # print('{} {}'.format(line, fname))
                # assert(bucket_us is None)
                bucket_us_item = items[0]
                bucket_us = int(bucket_us_item[len('segment_bucket_us:'):])
            if 'segment_idx:' in line:
                bucket_ops_item = items[-2]
                bucket_ops = float(bucket_ops_item[len('bucket_ops:'):])
                time_sec_list.append(bucket_us * cur_idx * 1e-6)
                num_op_list.append(bucket_ops)
                cur_idx += 1
    if len(time_sec_list) == 0:
        raise RuntimeError('{} does not have valid output'.format(fname))
    total_sec = max(time_sec_list)
    total_done = sum(num_op_list)
    return time_sec_list, total_sec, total_done, case


def process_bench_logs(aid_list, log_names):
    total_time_list = None
    per_app_iops_dict = {}
    per_case_iops_dict = {}
    for aid, log_name in zip(aid_list, log_names):
        time_sec_list, total_sec, total_done, case = process_one_bench_log(
            log_name)
        # some verification
        if total_time_list is None:
            total_time_list = time_sec_list
        else:
            assert(total_time_list == time_sec_list)

        cur_iops = total_done/total_sec
        per_app_iops_dict[aid] = cur_iops
        if case not in per_case_iops_dict:
            per_case_iops_dict[case] = 0
        per_case_iops_dict[case] += cur_iops
    return per_app_iops_dict, per_case_iops_dict


def process_log_dir_perf(args, expr_no_list):
    row_list = []
    row_name_list = ['no', 'per_core_ut_mean', 'per_core_ut_median',
                     'total_ut_mean', 'total_ut_median']
    MAX_NA = 10
    for aid in range(MAX_NA):
        row_name_list.append('aid-{}_iops_mean'.format(aid))
        row_name_list.append('aid-{}_iops_median'.format(aid))
    for case in AppCfg.get_valid_job_list():
        row_name_list.append('case-{}_iops_mean'.format(case))
        row_name_list.append('case-{}_iops_median'.format(case))
    row_name_list.append('total_iops')

    for expr_no in expr_no_list:
        expr_per_core_ut_list = []
        expr_total_ut_list = []
        expr_per_app_iops_list = {i: [] for i in range(MAX_NA)}
        expr_per_case_iops_list = {c: [] for c in AppCfg.get_valid_job_list()}
        cur_row = [expr_no]
        cur_cgst_ql, cur_nw, cur_na = get_special_cfg(expr_no, args)
        for rpt_no in range(args.num_rpt):
            cur_log_dir = '{}/log_loadexpr_no-{}_ql-{}_rpt-{}_NW-{}_NA-{}/log-numseg-1/'.format(
                args.log_dir, expr_no, cur_cgst_ql, rpt_no, cur_nw, cur_na)
            cur_fsp_log = '{}/fsp_log'.format(cur_log_dir)
            per_core_ut, total_ut, nw = process_fsp_log_cpu(
                cur_fsp_log, cal_mean_sec_range=1.5)
            #assert nw == args.num_worker
            assert nw == cur_nw
            expr_per_core_ut_list.append(per_core_ut)
            expr_total_ut_list.append(total_ut)
            aid_list = range(cur_na)
            bench_log_list = [
                '{}/bench_log_{}'.format(cur_log_dir, aid) for aid in aid_list]
            # print(cur_log_dir)
            per_app_iops_dict, per_case_iops_dict = process_bench_logs(
                aid_list, bench_log_list)
            for aid, iops in per_app_iops_dict.items():
                expr_per_app_iops_list[aid].append(iops)
            for case, iops in per_case_iops_dict.items():
                expr_per_case_iops_list[case].append(iops)
        cur_row += [cfs_tc.compute_avg(expr_per_core_ut_list), np.median(expr_per_core_ut_list),
                    cfs_tc.compute_avg(expr_total_ut_list), np.median(expr_total_ut_list)]
        cur_total_iops = 0
        for aid in range(MAX_NA):
            if aid not in expr_per_app_iops_list or aid >= cur_na:
                # mean
                cur_row.append(0)
                # median
                cur_row.append(0)
                continue
            cur_row.append(cfs_tc.compute_avg(expr_per_app_iops_list[aid]))
            cur_total_iops += cur_row[-1]
            if len(expr_per_app_iops_list[aid]) == 0:
                raise RuntimeError('error no iops for aid:{}'.format(aid))
            cur_row.append(np.median(expr_per_app_iops_list[aid]))
        for case, case_iops_list in expr_per_case_iops_list.items():
            cur_row.append(cfs_tc.compute_avg(case_iops_list))
            if len(case_iops_list) > 0:
                cur_row.append(np.median(case_iops_list))
            else:
                cur_row.append(0)
        cur_row.append(cur_total_iops)
        row_list.append(cur_row)
        print(cur_row)
    df = pd.DataFrame(row_list, columns=row_name_list)
    return df


def process_log_dir(args):
    expr_no_list = []
    if args.expr_list is not None:
        expr_no_list = [int(a) for a in eval(args.expr_list)]
    elif args.expr_no is not None:
        expr_no_list.append(args.expr_no)
    else:
        expr_no_list = range(65)
    avg_list = []
    median_list = []
    if not args.use_mod and not args.max_perf:
        for expr_no in expr_no_list:
            cur_expr_stable_ms = []
            cur_cgst_ql, cur_nw, cur_na = get_special_cfg(expr_no, args)
            for rpt_no in range(args.num_rpt):
                cur_log_name = '{}/log_loadexpr_no-{}_ql-{}_rpt-{}_NW-{}_NA-{}/log-numseg-1/fsp_log'.format(
                    args.log_dir, expr_no, cur_cgst_ql, rpt_no, cur_nw, cur_na)
                cur_stable_nano = get_one_log_stable_time(cur_log_name)
                cur_expr_stable_ms.append(cur_stable_nano*1e-6)
            cur_avg = cfs_tc.compute_avg(cur_expr_stable_ms)
            cur_median = np.median(cur_expr_stable_ms)
            avg_list.append(cur_avg)
            median_list.append(cur_median)
            print('no:{} avg:{} median:{}'.format(
                expr_no, cur_avg, cur_median))
        df = pd.DataFrame(list(zip(expr_no_list, avg_list, median_list)), columns=[
            'expr_no', 'avg', 'med_val'])
        df.to_csv('{}/stable_time.csv'.format(args.log_dir), index=False)
    perf_df = process_log_dir_perf(args, expr_no_list)
    perf_df.to_csv('{}/perf.csv'.format(args.log_dir), index=False)


def main(args, loglevel):
    logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)
    if args.summary_log:
        assert args.log_dir is not None
        assert args.num_worker is not None
        assert args.num_app is not None
        process_log_dir(args)
        return
    if args.show_exprs:
        do_lb_test_combination(args, print_only=True)
        return
    if args.nc:
        assert args.expr_no is not None and args.expr_no > 1000
        func_idx = args.expr_no - 1000
        func_name = 'do_nc_' + chr(ord('0') + func_idx)
        globals()[func_name](args)
        return
    if args.num_stat is not None:
        assert args.num_stat == 0 or args.num_stat == 1
    if args.expr_no is not None and args.expr_no >= 100:
        if args.expr_no < 200:
            func_idx = args.expr_no - 100
            func_name = 'do_lb_readonly_' + chr(ord('a') + func_idx)
            globals()[func_name](args)
            return
        elif args.expr_no < 300:
            func_idx = args.expr_no - 200
            func_name = 'do_lb_writeonly_' + chr(ord('a') + func_idx)
            globals()[func_name](args)
            return
        elif args.expr_no < 400:
            func_idx = args.expr_no - 300
            func_name = 'do_lb_readwrite_' + chr(ord('a') + func_idx)
            globals()[func_name](args)
            return
        else:
            raise RuntimeWarning(
                'cannot find match for expr:{}'.format(args.expr_no))
    else:
        do_lb_test_combination(args)


def parse_cmd_args():
    parser = argparse.ArgumentParser(
        description="Test the load condition of FSP")
    parser.add_argument('--num_rpt', type=int, required=True)
    parser.add_argument('--cgstql', help='congest queue length', required=True)

    # optional
    parser.add_argument('--num_io_app', type=int)
    parser.add_argument('--num_stat', type=int)
    parser.add_argument('--expr_no', type=int)
    parser.add_argument('--expr_list', type=str)
    parser.add_argument('--log_dir', type=str, help='directory contains logs')
    parser.add_argument('--num_worker', type=int, help='number of workers')
    parser.add_argument('--num_app', type=int, help='number of apps')

    # with default value
    parser.add_argument('--max_perf', action='store_true', default=False)
    parser.add_argument('--use_mod', action='store_true', default=False)
    parser.add_argument('--show_exprs', action='store_true', default=False)
    parser.add_argument('--summary_log', action='store_true', default=False)
    parser.add_argument('--nc', action='store_true', default=False)
    parser.add_argument('--burst', action='store_true', default=False)
    return (parser.parse_args())


if __name__ == '__main__':
    loglevel = logging.INFO
    args = parse_cmd_args()
    print(args)
    main(args, loglevel)
