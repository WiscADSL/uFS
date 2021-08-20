#! /usr/bin/env python3

# plot all microbenchmark cases
# REQUIRED: need to use my fork of zplot (for csv support)
# https://github.com/jingliu9/z-plot.git
from zplot import *
import pandas as pd
from numerize import numerize
import sys


def print_usage_and_exit():
    print(f'Usage: {sys.argv[0]} <plot_name> <ufs_data> <ext4_data> [ext4-nora_data]')
    print(f'  Plot a comparsion of uFS and ext4 (possibly with ext4-nora)')
    print(f'    plot_name:  Name of the figure to plot')
    print(f'    ufs_dir:    Formatted as "ufs_legend:ufs_dir"')
    print(f'    ext4_dir:   Formatted as "ext4_legend:ext4_dir"')
    print(f'    ext4-nora_dir: Formatted as "ext4-nora_legend:ext4-nora_dir"')
    exit(1)


if len(sys.argv) != 4 and len(sys.argv) != 5:
    print_usage_and_exit()

plot_name = sys.argv[1]
for arg in sys.argv[2:]:
    if not ":" in arg:
        print_usage_and_exit

fs_dir_dict = {}
fs_name_dict = {}

ufs_name, ufs_dir = sys.argv[2].split(":", 1)
ext4_name, ext4_dir = sys.argv[3].split(":", 1)

fs_name_dict["ufs"] = ufs_name
fs_name_dict["ext4"] = ext4_name
fs_dir_dict["ufs"] = ufs_dir
fs_dir_dict["ext4"] = ext4_dir

draw_nora = False
ext4_nora_jobs = []
if len(sys.argv) == 5:
    ext4_nora_name, ext4_nora_dir = sys.argv[4].split(":", 1)
    fs_name_dict["ext4-nora"] = ext4_nora_name
    fs_dir_dict["ext4-nora"] = ext4_nora_dir
    # for now, only these two jobs are interesting...
    ext4_nora_jobs = ['RDPS', 'RDSS']
    draw_nora = True


def get_default_benchmarks():
    benchmarks = [
        'RMPR',
        'RMSR',
        'RDPR',
        'RDSR',
        'RMPS',
        'RMSS',
        'RDPS',
        'RDSS',
        'WMPR',
        'WMSR',
        'WDPR',
        'WDSR',
        'WMPS',
        'WMSS',
        'WDPS',
        'WDSS',
        'AMPS',
        'AMSS',
        'ADPS',
        'ADSS',
        'S1MP',
        'S1MS',
        'SaMP',
        'SaMS',
        'LsMP',
        'LsMS',
        'CMP',
        'CMS',
        'UMP',
        'UMS',
        'RMP',
        'RMS',
    ]
    return benchmarks


def get_bench_job_title(job_name):
    titles = {
        'RMPR': 'RandRead-Mem-P',
        'RMSR': 'RandRead-Mem-S',
        'RDPR': 'RandRead-Disk-P',
        'RDSR': 'RandRead-Disk-S',
        'RMPS': 'SeqRead-Mem-P',
        'RMSS': 'SeqRead-Mem-S',
        'RDPS': 'SeqRead-Disk-P',
        'RDSS': 'SeqRead-Disk-S',
        'WMPR': 'RandWrite-Mem-P',
        'WMSR': 'RandWrite-Mem-S',
        'WDPR': 'RandWrite-Disk-P',
        'WDSR': 'RandWrite-Disk-S',
        'WMPS': 'SeqWrite-Mem-P',
        'WMSS': 'SeqWrite-Mem-S',
        'WDPS': 'SeqWRite-Disk-P',
        'WDSS': 'SeqWrite-Disk-S',
        'AMPS': 'Append-Mem-P',
        'AMSS': 'Append-Mem-S',
        'ADPS': 'Append-Disk-P',
        'ADSS': 'Append-Disk-S',
        'S1MP': 'Stat1-Mem-P',
        'S1MS': 'Stat1-Mem-S',
        'SaMP': 'StatAll-Mem-P',
        'SaMS': 'StatAll-Mem-S',
        'LsMP': 'Listdir-Mem-P',
        'LsMS': 'Listdir-Mem-S',
        'CMP': 'Create-Mem-P',
        'CMS': 'Create-Mem-S',
        'UMP': 'Unlink-Mem-P',
        'UMS': 'Unlink-Mem-S',
        'RMP': 'Rename-Mem-P',
        'RMS': 'Rename-Mem-S',
    }
    return titles[job_name]


ctype = 'eps'

# every pic is 60 * 40
# 170 = 150 + 20
num_horiz = 8
num_vert = 4
c = canvas(ctype,
           title=plot_name,
           dimensions=[
               num_horiz * 60 + num_horiz * 10, num_vert * 40 + num_vert * 10
           ])

y_label_shift = [3, 0]
x_label_shift = [0, 2]
label_font_size = 5
vertical_label_font_size = 4.5
small_title_font_size = 6  # annotate workload
large_title_font_size = 5.5  # annotate Throughput and Resource Utilization
title_shift = [0, -5]
axis_line_width = 0.6
tp_line_width = 0.7
tic_major_size = 2.0
app_range = [0, 11]
box_dim = [60, 34]


def get_coord(x, y):
    '''
    get the coord for each grid
    change the h_step, w_step for resize grid
    change col_sum, row_sum for more grid or less
    change base if only need more space under grids (like for text, legend)
    '''
    # x should start from 0
    # y should start from 0
    base = [16, 20]
    axis_interval = 10
    h_step = 44
    w_step = 58
    # col_sum = num_horiz
    row_sum = num_vert
    real_x = row_sum - x - 1
    real_y = y
    coord = [
        base[0] + real_y * (w_step + axis_interval),
        base[1] + real_x * (h_step)
    ]
    return coord


p = plotter()

color_dict = {
    "ufs": '0,0,0',
    "ext4": '0.3,0.3,0.3',
    "ext4-nora": '0.6,0.6,0.6',
}

dash_dict = {
    "ufs": 0,
    "ext4": [1, 0.5],
    "ext4-nora": [2, 0.7],
}

# For readability purposes, one may manually set the range of Y-axis
hardcode_yaxis = {
    # "WMPR": 10e6,
    # "WMSR": 1e6,
    # "WMPS": 5e6,
    # "WMSS": 1e6,
    # "AMPS": 5e6,
    # "UMP": 1e6,
    # "UMS": 750e3,
    # "CMP": 500e3,
}

fs_type_list = ['ext4', 'ufs']
additional_fs_type_list = ['ext4-nora']
legend_dict = {fs: legend() for fs in fs_type_list}
for fs_job in color_dict.keys():
    legend_dict[fs_job] = legend()

jobs = get_default_benchmarks()
assert (len(jobs) == num_horiz * num_vert)
pic_idx = 0
tc_dict = {}
drawable_map = {}


def get_round_y(cur_y):
    if cur_y <= 5e3:
        return 5e3
    if cur_y <= 250e3:
        return 250e3
    if cur_y <= 500e3:
        return 500e3
    if cur_y <= 750e3:
        return 750e3
    if cur_y <= 1e6:
        return 1e6
    if cur_y <= 3e6:
        return 3e6
    if cur_y <= 5e6:
        return 5e6
    if cur_y <= 10e6:
        return 10e6
    if cur_y <= 25e6:
        return 25e6
    if cur_y <= 50e6:
        return 50e6
    if cur_y <= 75e6:
        return 75e6
    if cur_y <= 100e6:
        return 100e6
    return ((cur_y + 1) // 50e6) * 50e6


def load_job_data(job, fstp):
    cur_csv_name = '{}/{}.csv'.format(fs_dir_dict[fstp], job)
    tc_dict[(job, fstp)] = table(cur_csv_name)


def get_max(job, fstp, field):
    cur_csv_name = '{}/{}.csv'.format(fs_dir_dict[fstp], job)
    cur_df = pd.read_csv(cur_csv_name)
    cur_max = (cur_df[field]).max()
    return cur_max


def plot_job_per_fs(cur_d, job, fstp, field, has_legend=False):
    if has_legend:
        cur_l = legend_dict[fstp]
    else:
        cur_l = ''
    p.line(drawable=cur_d,
           table=tc_dict[(job, fstp)],
           xfield='num_app',
           yfield=field,
           linecolor=color_dict[fstp],
           linewidth=tp_line_width,
           legend=cur_l,
           legendtext=fs_name_dict[fstp],
           linedash=dash_dict[fstp])


def create_axis(job, pic_idx):
    field = 'iops'
    if job in hardcode_yaxis:
        cur_y_adjust = hardcode_yaxis[job]
    else:
        cur_y_max = max([get_max(job, fstp, field) for fstp in fs_type_list])
        cur_y_adjust = get_round_y(cur_y_max)

    cur_d = drawable(canvas=c,
                     xrange=app_range,
                     yrange=[0, cur_y_adjust],
                     coord=get_coord(pic_idx // num_horiz,
                                     pic_idx % num_horiz),
                     dimensions=box_dim)
    cur_title = '{}'.format(get_bench_job_title(job))
    drawable_map[job] = cur_d

    cur_y_labels = [['0', 0], [numerize.numerize(cur_y_adjust), cur_y_adjust]]
    axis(drawable=cur_d,
         style='y',
         ylabelshift=y_label_shift,
         ylabelfontsize=vertical_label_font_size,
         linewidth=axis_line_width,
         ticmajorsize=tic_major_size,
         ymanual=cur_y_labels)
    axis(drawable=cur_d,
         style='x',
         title=cur_title,
         titlesize=large_title_font_size,
         titleshift=title_shift,
         xauto=[0, 1, 11],
         dolabels=False,
         linewidth=axis_line_width,
         ticmajorsize=tic_major_size)


def plot_job_with_order(job, pic_idx, field, fs_list=fs_type_list):
    cur_d = drawable_map[job]
    for fstp in fs_list:
        plot_job_per_fs(cur_d, job, fstp, field, pic_idx == 0)


for pic_idx, job in enumerate(jobs):
    for fstp in fs_type_list:
        load_job_data(job, fstp)
        create_axis(job, pic_idx)

# draw nora first
if draw_nora:
    additional_legend_added = False
    additional_jobs = ext4_nora_jobs
    for job in additional_jobs:
        pic_idx = get_default_benchmarks().index(job)
        for fstp in additional_fs_type_list:
            load_job_data(job, fstp)
            plot_job_per_fs(drawable_map[job], job, fstp, 'iops',
                            not additional_legend_added)
        additional_legend_added = True

# draw ext4 first, then ufs.
for pic_idx, job in enumerate(jobs):
    plot_job_with_order(job, pic_idx, 'iops', ['ext4', 'ufs'])

small_title_font_size = 7  # annotate workload
legend_base_x = 260
legend_base_y = 10
legend_dict['ufs'].draw(canvas=c,
                        coord=[legend_base_x - 60, legend_base_y],
                        width=10,
                        height=5,
                        fontsize=small_title_font_size,
                        skipnext=1,
                        skipspace=20,
                        hspace=1)
legend_dict['ext4'].draw(canvas=c,
                         coord=[legend_base_x, legend_base_y],
                         width=10,
                         height=5,
                         fontsize=small_title_font_size,
                         skipnext=1,
                         skipspace=20,
                         hspace=1)
if draw_nora:
    legend_dict['ext4-nora'].draw(canvas=c,
                                  coord=[legend_base_x + 65, legend_base_y],
                                  width=10,
                                  height=5,
                                  fontsize=small_title_font_size,
                                  skipnext=1,
                                  skipspace=20,
                                  hspace=1)

# finally, output the graph to a file
c.render()
