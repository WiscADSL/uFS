#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import sys

DIR_LIST = ['gradual_ufs', 'burst_ufs', 'gradual_max', 'burst_max']
NUM_RPT = 5


def print_usage_and_exit():
    print(f"Usage: {sys.argv[0]} <calloc_dir>")
    exit(1)


if len(sys.argv) != 2:
    print_usage_and_exit()

calloc_dir = sys.argv[1]

log_dir_name_list = [
    'log_loadexpr_no-1002_ql-1.1_rpt-0_NW-6_NA-6/log-numseg-1/',
    'log_loadexpr_no-1003_ql-1.1_rpt-0_NW-6_NA-6/log-numseg-1/',
    'log_loadexpr_no-1004_ql-1.1_rpt-0_NW-6_NA-6/log-numseg-1/',
    'log_loadexpr_no-1005_ql-1.2_rpt-0_NW-6_NA-6/log-numseg-1/',
]

exper_short_names = ["a", "b", "c", "d"]


class ExprReport(object):
    def __init__(self, expr_name):
        self.expr_name = expr_name
        self.dir_qps_dict = {dir_name: {} for dir_name in DIR_LIST}
        self.dir_num_cores_dict = {dir_name: {} for dir_name in DIR_LIST}

    def add_qps(self, dir_name, rpt_no, total_qps, avg_qps):
        assert (dir_name in self.dir_qps_dict)
        cur_dict = self.dir_qps_dict[dir_name]
        assert (rpt_no not in cur_dict)
        cur_dict[rpt_no] = total_qps

    def add_avg_nc(self, dir_name, rpt_no, avg_nc):
        assert (dir_name in self.dir_num_cores_dict)
        cur_dict = self.dir_num_cores_dict[dir_name]
        assert (rpt_no not in cur_dict)
        cur_dict[rpt_no] = avg_nc

    def report(self):
        print(f'Experiment-{self.expr_name} (repeat {NUM_RPT} times)')
        total_qps_median_dict = {}
        avg_nc_median_dict = {}
        for dir_name, vals in self.dir_qps_dict.items():
            assert (len(vals) == NUM_RPT)
            total_qps_median_dict[dir_name] = np.median(list(vals.values()))
        for dir_name, vals in self.dir_num_cores_dict.items():
            if 'ufs' not in dir_name:
                continue
            assert (len(vals) == NUM_RPT)
            avg_nc_median_dict[dir_name] = np.median(list(vals.values()))
        avg_num_cores = avg_nc_median_dict['gradual_ufs']
        tp_ratio = total_qps_median_dict['gradual_ufs'] / \
                total_qps_median_dict['gradual_max']
        print(
            f'[median] gradual:\tavg_num_core: {avg_num_cores:.2f}\tthroughput_ratio: {tp_ratio:.3f}'
        )
        avg_num_cores = avg_nc_median_dict['burst_ufs']
        tp_ratio = total_qps_median_dict['burst_ufs'] / \
            total_qps_median_dict['burst_max']
        print(
            f'[median] burst:  \tavg_num_core: {avg_num_cores:.2f}\tthroughput_ratio: {tp_ratio:.3f}'
        )


def get_single_run_qps(fname):
    with open(fname) as f:
        lines = f.readlines()
        assert (len(lines) == 3)
        qps_line = lines[2]
        qps_line = qps_line.strip()
        items = qps_line.split()
        total_qps = float(items[0])
        avg_qps = float(items[1])
    return total_qps, avg_qps


def get_single_num_cores(fname):
    with open(fname) as f:
        line = f.readline()
        avg_nc = float(line)
    return avg_nc


for name, orig_log_dir in zip(exper_short_names, log_dir_name_list):
    cur_report = ExprReport(name)
    for cur_dir in DIR_LIST:
        for rpt in range(1, NUM_RPT + 1):
            cur_output_dir = f"{calloc_dir}/{cur_dir}/run_{rpt}/{orig_log_dir}"
            qps_fname = '{}/qps'.format(cur_output_dir)
            total_qps, avg_qps = get_single_run_qps(qps_fname)
            cur_report.add_qps(cur_dir, rpt, total_qps, avg_qps)
            if 'ufs' in cur_dir:
                num_cores_fname = '{}/mean_num_core'.format(cur_output_dir)
                avg_nc = get_single_num_cores(num_cores_fname)
                cur_report.add_avg_nc(cur_dir, rpt, avg_nc)
    cur_report.report()
