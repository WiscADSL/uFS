#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

from bokeh.plotting import figure
from bokeh.palettes import Paired
from bokeh.models import Legend, NumeralTickFormatter


def get_attr_from_item(item, attr_name, deli, tp):
    return tp(item[len(attr_name + deli):])


def print_usage(argv):
    print('Usage {} <dir_name> <num_app> [name_suffix]'.format(argv[0]))


if len(sys.argv) < 3:
    print_usage(sys.argv)
    exit(1)

dir_name = sys.argv[1]
num_app = int(sys.argv[2])

name_suffix = ''
if len(sys.argv) == 4:
    name_suffix = sys.argv[3]

HIGH_NUM = 8
LINE_WIDTH = 2
FONT_SIZE = "12pt"


def plot_series(per_worker_seq_dict):
    legend_it = []
    title_name = 'fsp-cpu-utilization'.format(name_suffix)
    print('size:{}'.format(len(per_worker_seq_dict)))
    p = figure(title=title_name, plot_width=1000, plot_height=500)
    p.title.text_font_size = FONT_SIZE
    p.xaxis.axis_label = 'Time (sec)'
    p.yaxis.axis_label = 'CPU Utilization'
    p.y_range.start = 0
    p.y_range.end = 1.05
    p.toolbar.logo = None
    p.toolbar_location = None
    # p.x_range.end = 9.1
    for wid, seq in per_worker_seq_dict.items():
        print(seq)
        l = p.line(x=seq[0],
                   y=seq[1],
                   color=Paired[10][wid],
                   line_width=LINE_WIDTH)
        if wid == 2:
            p.circle(x=seq[0], y=seq[1], color=Paired[10][wid], size=5)
        # l = p.circle(x=seq[0], y=seq[1], color=Paired[10][wid], size=3)
        legend_it.append(('wid-{}'.format(wid), [l]))
    legend = Legend(items=legend_it, location=(0, 0))
    legend.click_policy = "mute"
    p.add_layout(legend, 'right')
    for cur_axis in [p.xaxis, p.yaxis]:
        cur_axis.axis_label_text_font_size = FONT_SIZE
        cur_axis.major_label_text_font_size = FONT_SIZE
    # export_png(p, filename='{}/{}.png'.format(dir_name, title_name))


def fill_middle_hole(wid, worker_tuple_list):
    # print('fill middle')
    KGAP_SEC = 0.5
    added_dict = {}
    orig_num = len(worker_tuple_list[0])
    # print(worker_tuple_list)
    orig_tuple_list = list(zip(worker_tuple_list[0], worker_tuple_list[1]))
    orig_tuple_list = sorted(orig_tuple_list, key=lambda x: x[0])
    # print(orig_tuple_list)
    for i in range(1, orig_num):
        print('---- wid:{} orig num:{}'.format(wid, orig_num))
        if worker_tuple_list[0][i] - worker_tuple_list[0][i - 1] > KGAP_SEC:
            print(worker_tuple_list[0][i] - worker_tuple_list[0][i - 1])
            added_dict[worker_tuple_list[0][i] - 0.01] = 0
            added_dict[worker_tuple_list[0][i - 1] + 0.01] = 0
            print(added_dict)
    for k, v in added_dict.items():
        orig_tuple_list.append((k, v))
    orig_tuple_list = sorted(orig_tuple_list, key=lambda x: x[0])
    result_list = ([], [])
    for tp in orig_tuple_list:
        result_list[0].append(tp[0])
        result_list[1].append(tp[1])
    if orig_num < len(result_list[1]):
        print('-------- filled ----------')
        print(list(zip(result_list[0], result_list[1])))
    return result_list


def process_fsp_log(fname, sample_k=None):
    per_worker_series_dict = {}
    first_nano = None
    per_worker_idx = {}
    latest_nano = None
    with open(fname) as f:
        for line in f:
            line = line.strip()
            items = line.split()
            if 'invalidateAppShmByName' in line:
                break
            if 'BlkDevSpdk' in line:
                continue
            if '[warning]' in line:
                continue
            if 'FsProc.cc' in line:
                continue
            if 'FsProc_KnowParaLoadMng' in line and 'deactivated' in line:
                wid_item = items[-2]
                wid = get_attr_from_item(wid_item, 'wid', ':', int)
                nano_item = items[-3]
                nano = get_attr_from_item(nano_item, 'nano', ':', int)
                cur_sec = (nano - first_nano) * 1e-9 + 0.001
                per_worker_series_dict[wid][0].append(cur_sec)
                per_worker_series_dict[wid][1].append(0)
                print('wid:{} deactivate sec:{}'.format(wid, cur_sec))
                continue
            if 'wid:' in line and ' activated' in line and 'localvid' in line:
                if latest_nano is None:
                    continue
                wid_item = items[-3]
                wid = get_attr_from_item(wid_item, 'wid', ':', int)
                if wid not in per_worker_series_dict:
                    per_worker_series_dict[wid] = ([], [])
                    per_worker_idx[wid] = 0
                cur_sec = (latest_nano - first_nano) * 1e-9 - 0.001
                per_worker_series_dict[wid][0].append(cur_sec)
                per_worker_series_dict[wid][1].append(0)
                print('wid:{} activate cur_sec:{}'.format(wid, cur_sec))
                continue
            if '[KPLoadStatsSummary]' not in line:
                continue
            if 'cpu_ut:' in line:
                wid_item = items[1]
                nano_item = items[2]
                utilization_item = items[-3]
                # print(line)
                wid = int(wid_item[len('wid:'):])
                nano = int(nano_item[len('real_nano:'):])
                if first_nano is None:
                    first_nano = nano
                latest_nano = nano
                print(line)
                utilization = float(utilization_item[len('cpu_ut:'):])
                if wid not in per_worker_series_dict:
                    per_worker_series_dict[wid] = ([], [])
                    per_worker_idx[wid] = 0

                if sample_k is not None and per_worker_idx[wid] % sample_k != 0:
                    per_worker_idx[wid] += 1
                    continue

                per_worker_series_dict[wid][0].append(
                    (nano - first_nano) * 1e-9)
                per_worker_series_dict[wid][1].append(utilization)
                per_worker_idx[wid] += 1
    # for k, v in per_worker_series_dict.items():
    # per_worker_series_dict[k] = fill_middle_hole(k, v)
    plot_series(per_worker_series_dict)


def process_one_bench_log(fname, sample_k=None):
    time_sec_list = []
    num_op_list = []
    case = None
    total_done = 0
    total_sec = 0
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
                #print('{} {}'.format(line, fname))
                #assert(bucket_us is None)
                bucket_us_item = items[0]
                bucket_us = int(bucket_us_item[len('segment_bucket_us:'):])
            if 'segment_idx:' in line:
                bucket_ops_item = items[-1]
                bucket_done_num_item = items[-2]
                bucket_done_num = get_attr_from_item(bucket_done_num_item,
                                                     'bucket_ops', ':', int)
                total_done += bucket_done_num
                total_sec += bucket_us * 1e-6
                if sample_k is not None and cur_idx % sample_k != 0:
                    cur_idx += 1
                    continue
                bucket_ops = float(bucket_ops_item[len('req_per_sec:'):])
                time_sec_list.append(bucket_us * cur_idx * 1e-6)
                num_op_list.append(bucket_ops)
                cur_idx += 1
    qps = total_done / total_sec
    # out_name = fname+'.qps'
    # with open(out_name, 'w') as f:
    # f.write('{}'.format(qps))
    return time_sec_list, num_op_list, qps


def process_bench_logs(aid_list, log_names, sample_k=None):
    total_time_list = None
    overall_req_psec_dict = {}
    per_app_req_pserc_dict = {}
    cur_max_req_psec = 0
    cur_max_time_ts_num = 0
    aid_qps_dict = {}
    for aid, log_name in zip(aid_list, log_names):
        print(aid)
        print(log_name)
        time_sec_list, req_psec_list, cur_qps = process_one_bench_log(
            log_name, sample_k=sample_k)
        aid_qps_dict[aid] = cur_qps
        if len(time_sec_list) > cur_max_time_ts_num:
            cur_max_time_ts_num = len(time_sec_list)

        if total_time_list is None:
            total_time_list = time_sec_list
        else:
            #assert(total_time_list == time_sec_list)
            pass

        print('cur_len:{} total_len:{}'.format(len(time_sec_list),
                                               len(total_time_list)))
        if len(time_sec_list) < cur_max_time_ts_num:
            for j in range(len(req_psec_list), len(total_time_list)):
                time_sec_list.append(total_time_list[j])
                req_psec_list.append(0)
            pass
        print('cur_len:{} total_len:{}'.format(len(time_sec_list),
                                               len(total_time_list)))

        # print(req_psec_list)
        per_app_req_pserc_dict[aid] = req_psec_list.copy()
        print('------')
    max_req_psec = cur_max_req_psec
    # print(per_app_req_pserc_dict)
    # print(overall_req_psec_list)
    legend_it = []
    title_name = 'app-throughput-{}'.format(name_suffix)
    p = figure(title=title_name, plot_width=1000, plot_height=500)
    p.title.text_font_size = FONT_SIZE
    p.xaxis.axis_label = 'Time (sec)'
    p.yaxis.axis_label = 'Throughput (req/sec)'
    p.yaxis.formatter = NumeralTickFormatter(format="0.0a")
    p.toolbar.logo = None
    p.toolbar_location = None
    p.y_range.start = 0
    # plot
    for aid in aid_list:
        cur_alpha = 0.8
        print('aid:{}'.format(aid))
        print(per_app_req_pserc_dict[aid])
        l = p.line(x=total_time_list,
                   y=per_app_req_pserc_dict[aid],
                   color=Paired[10][aid],
                   line_width=LINE_WIDTH,
                   alpha=cur_alpha)
        legend_it.append(('app-{}'.format(aid), [l]))
    legend = Legend(items=legend_it, location=(0, 0))
    legend.click_policy = "mute"
    p.add_layout(legend, 'right')
    for cur_axis in [p.xaxis, p.yaxis]:
        cur_axis.axis_label_text_font_size = FONT_SIZE
        cur_axis.major_label_text_font_size = FONT_SIZE
    with open('{}/qps'.format(dir_name), 'w') as f:
        aids = aid_qps_dict.keys()
        aids_str = [str(i) for i in aids]
        vals = [str(aid_qps_dict[i]) for i in aids]
        cur_total = sum(aid_qps_dict.values())
        cur_avg = cur_total / len(aids)
        f.write(' '.join(vals))
        f.write('\n')
        f.write(' '.join(aids_str))
        f.write('\n')
        f.write('{} {}'.format(cur_total, cur_avg))
        # for aid,v in aid_qps_dict.items():
        # f.write('{} {}'.format(aid, v))
        # f.write('\n')
    # add thinktime label
    # export_png(p, filename='{}/{}.png'.format(dir_name, title_name))


fsp_log_name = '{}/{}'.format(dir_name, 'fsp_log')
aid_list = [i for i in range(num_app)]
bench_log_name_list = [
    '{}/bench_log_{}'.format(dir_name, aid) for aid in aid_list
]

SAMPLE_K = 10
process_fsp_log(fsp_log_name, sample_k=SAMPLE_K)
cur_list = None
process_bench_logs(aid_list, bench_log_name_list, sample_k=SAMPLE_K)
