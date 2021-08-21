#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import sys


def print_usage_and_exit():
    print(f"Usage: {sys.argv[0]} <ufs_dir> <rr_dir> <max_dir>")
    exit(1)


if len(sys.argv) != 4:
    print_usage_and_exit()

case_dir_dict = {
    'ufs': sys.argv[1],
    'rr': sys.argv[2],
    'max': sys.argv[3],
}

exp_no_char_dict = {
    100: 'a  ',
    101: 'b  ',
    102: 'c  ',
    103: 'abc',
    200: 'e  ',
    201: 'f  ',
    202: 'g  ',
    203: 'efg',
    300: 'all',
}


def get_case_perf_df(exp_no, case_name):
    if exp_no < 200:
        cur_df = pd.read_csv('{}/read_perf.csv'.format(
            case_dir_dict[case_name]))
    else:
        cur_df = pd.read_csv('{}/perf.csv'.format(case_dir_dict[case_name]))
    return cur_df


dyn_read_stable_time_df = pd.read_csv('{}/read_stable_time.csv'.format(
    case_dir_dict['ufs']))
dyn_stable_time_df = pd.read_csv('{}/stable_time.csv'.format(
    case_dir_dict['ufs']))

for exp_no in sorted(list(exp_no_char_dict.keys())):
    dyn_df = get_case_perf_df(exp_no, 'ufs')
    mod_df = get_case_perf_df(exp_no, 'rr')
    perf_df = get_case_perf_df(exp_no, 'max')
    dyn_exp_df = dyn_df.loc[dyn_df['no'] == exp_no]
    mod_exp_df = mod_df.loc[mod_df['no'] == exp_no]
    perf_exp_df = perf_df.loc[perf_df['no'] == exp_no]
    dyn_raio = list(dyn_exp_df.total_iops)[0] / list(perf_exp_df.total_iops)[0]
    mod_raio = list(mod_exp_df.total_iops)[0] / list(perf_exp_df.total_iops)[0]
    if exp_no < 200:
        cur_stable_time_df = dyn_read_stable_time_df
    else:
        cur_stable_time_df = dyn_stable_time_df
    cur_stable_time_df = cur_stable_time_df.loc[cur_stable_time_df['expr_no']
                                                == exp_no]
    print('{}\tufs_ratio: {:.3f}\tufsrr_raio: {:.3f}\tmedian_stable: {:.2f}'.
          format(exp_no_char_dict[exp_no], dyn_raio, mod_raio,
                 list(cur_stable_time_df.med_val)[0]))
