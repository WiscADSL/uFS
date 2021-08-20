#!/usr/bin/env python3
# encoding: utf-8

# This script parse microbench logs and generate .csv for each workload

import logging
import pandas as pd
import re
import os
import argparse
import datetime
import statistics


def get_max_num_app():
    return 10


def get_YEAR():
    return datetime.date.today().year


def is_fsp(fstype):
    return 'fsp' in fstype


def get_syncop_num():
    return 4


def bench_has_fg_sync(job):
    job_list = ['ADPS', 'ADSS']
    return job in job_list


def filter_listdir_dir(job, cur_dir_list):
    job_list = ['SaMP', 'SaMS', 'LsMP', 'LsMS']
    result_list = []
    if job in job_list:
        for name in cur_dir_list:
            cur_name = name.split('/')[-1]
            if 'listdir' in cur_name:
                result_list.append(name)
        return result_list
    else:
        return cur_dir_list


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
        'WMPS',
        'WMSS',
        'AMPS',
        'AMSS',
        'WMPR',
        'WMSR',
        'WDPS',
        'WDPR',
        'WDSS',
        'WDSR',
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
        'WMPS': 'SeqWrite-Mem-P',
        'WMSS': 'SeqWrite-Mem-S',
        'AMPS': 'Append-Mem-P',
        'AMSS': 'Append-Mem-S',
        'WMPR': 'RandWrite-Mem-P',
        'WMSR': 'RandWrite-Mem-S',
        'WDPS': 'SeqWRite-Disk-P',
        'WDPR': 'RandWrite-Disk-P',
        'WDSS': 'SeqWrite-Disk-S',
        'WDSR': 'RandWrite-Disk-S',
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


def get_dir_name_list_match_pattern(dir_name, reg_str):
    pattern = re.compile(reg_str)
    match_list = []
    logging.debug('==> try match {} {}'.format(dir_name, reg_str))
    assert (os.path.exists(dir_name))
    for name in os.listdir(dir_name):
        if pattern.match(name):
            match_list.append('{}/{}'.format(dir_name, name))
    return match_list


def process_fsp_out(out_name):
    row_col_name_list = [
        'firstNs', 'lastNs', 'intervalNs', 'bytes', 'numop', 'iops', 'bw'
    ]
    row_list = []
    with open(out_name) as f:
        cur_row_dict = None
        wid = None
        for line in f:
            line = line.strip()
            items = line.split()
            if 'wid:' in line:
                if wid is None:
                    wid = int(items[-2].split(':')[1])
                continue
            if '===> stats ===>' in line:
                if cur_row_dict is not None:
                    row_list.append(
                        [cur_row_dict[k] for k in row_col_name_list])
                    cur_row_dict = {n: 0 for n in row_col_name_list}
                cur_row_dict = {n: 0 for n in row_col_name_list}
            if 'iops:' in line:
                bw_item = items[-1]
                iops_item = items[-2]
                cur_row_dict['bw'] = float(bw_item.split(':')[1])
                cur_row_dict['iops'] = float(iops_item.split(':')[1])
                continue
            if 'firstNs:' in line:
                cur_row_dict['numop'] = int((items[-1]).split(':')[1])
                cur_row_dict['bytes'] = int((items[-2]).split(':')[1])
                cur_row_dict['lastNs'] = float((items[-4]).split(':')[1])
                cur_row_dict['firstNs'] = float((items[-5]).split(':')[1])
                cur_row_dict['intervalNs'] = float((items[-3]).split(':')[1])
                continue
            if 'stats' not in line:
                continue
        # add the last row
        row_list.append([cur_row_dict[k] for k in row_col_name_list])
    df = pd.DataFrame(row_list, columns=row_col_name_list)
    return df


def process_bench_log(log_name):
    row_list = []
    row_col_list = [
        'size', 'numop', 'iops', 'ltc_stddev', 'bw', 'avg_ltc', 'med_ltc',
        'ltc99', 'ltc999'
    ]
    with open(log_name) as f:
        cur_row_dict = None
        for line in f:
            line = line.strip()
            items = line.split()
            if 'Values:' in line:
                if cur_row_dict is not None:
                    row_list.append(
                        [cur_row_dict[k] for k in cur_row_dict.keys()])
                cur_row_dict = {n: 0 for n in row_col_list}
                cur_size = int(items[1])
                cur_row_dict['size'] = cur_size
            if 'Entries:' in line:
                cur_numop = int(items[1])
                cur_row_dict['numop'] = cur_numop
            if 'micros/op' in line:
                assert ('micros/op' in items[3])
                cur_microsop = float(items[2])
                cur_iops = 1e6 / cur_microsop
                cur_row_dict['iops'] = cur_iops
                if 'MB/s' in line:
                    cur_bw = float(items[4])
                    cur_row_dict['bw'] = cur_bw
                else:
                    cur_row_dict['bw'] = 0
            if 'Average:' in line:
                assert ('Average' in items[2])
                cur_avg_ltc = float(items[3])
                cur_ltc_stddev = float(items[5])
                cur_row_dict['avg_ltc'] = cur_avg_ltc
                cur_row_dict['ltc_stddev'] = cur_ltc_stddev
            if 'Median:' in line:
                assert ('Median' in items[2])
                cur_med_ltc = float(items[3])
                cur_row_dict['med_ltc'] = cur_med_ltc
            if '%' in line:
                pct_item = items[6]
                pct = float(pct_item[:-1])
                if pct >= 98.9999999 and cur_row_dict['ltc99'] == 0:
                    cur_row_dict['ltc99'] = float(items[2])
                if pct >= 99.8999999 and cur_row_dict['ltc999'] == 0:
                    cur_row_dict['ltc999'] = float(items[2])
        row_list.append([cur_row_dict[k] for k in cur_row_dict.keys()])
    df = pd.DataFrame(row_list, columns=row_col_list)
    return row_list, row_col_list, df


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
                wid = int(wid_item[len('wid:'):])
                nano = int(nano_item[len('real_nano:'):])
                if first_nano is None:
                    first_nano = nano
                utilization = float(utilization_item[len('cpu_ut:'):])
                if wid not in per_worker_series_dict:
                    per_worker_series_dict[wid] = ([], [])
                    per_worker_idx[wid] = 0

                if sample_k is not None and per_worker_idx[wid] % sample_k != 0:
                    per_worker_idx[wid] += 1
                    continue
                cur_sec = (nano - first_nano) * 1e-9
                if cal_mean_sec_range is not None and cur_sec > cal_mean_sec_range:
                    break
                per_worker_series_dict[wid][0].append(
                    (nano - first_nano) * 1e-9)
                per_worker_series_dict[wid][1].append(utilization)
                per_worker_idx[wid] += 1
    if cal_mean_sec_range is not None:
        num_worker = len(per_worker_series_dict)
        per_worker_avg = {}
        for wid, wid_ut_tp in per_worker_series_dict.items():
            per_worker_avg[wid] = statistics.mean(
                wid_ut_tp[1]) if len(wid_ut_tp[1]) != 0 else 0
        total_ut = sum(list(per_worker_avg.values()))
        mean_ut = total_ut / num_worker
        return mean_ut, total_ut, num_worker
    else:
        return per_worker_series_dict


def merge_rows_from_apps(row_ll, df_list, cur_row_col_list, num_app,
                         size_list):
    """
    merge the row_list generated from each App's benchmark log
    """
    rt_dict = {}
    for i in range(len(size_list)):
        for j in range(num_app):
            cur_app_size_row = row_ll[j][i]
            cur_sz = size_list[i]
            if cur_sz not in rt_dict:
                rt_dict[cur_sz] = {}
            for k in range(len(cur_row_col_list)):
                cur_col_key = cur_row_col_list[k]
                # if cur_col_key == 'size':
                #  continue
                if cur_col_key not in rt_dict[cur_sz]:
                    rt_dict[cur_sz][cur_col_key] = []
                rt_dict[cur_sz][cur_col_key].append(cur_app_size_row[k])

    def from_row_dict_to_row(row_dict, cur_sz):
        cur_row = [0 for i in range(len(cur_row_col_list))]
        for k, v in row_dict.items():
            assert (len(v) == num_app)
            if k == 'size':
                cur_row[cur_row_col_list.index('size')] = cur_sz
            elif k == 'numop':
                cur_row[cur_row_col_list.index(k)] = sum(v)
            elif k == 'iops':
                cur_row[cur_row_col_list.index(k)] = sum(v)
            elif k == 'bw':
                cur_row[cur_row_col_list.index(k)] = sum(v)
            elif k == 'avg_ltc':
                cur_row[cur_row_col_list.index(k)] = statistics.mean(v)
            else:
                # latency we use median (p99, med)
                cur_row[cur_row_col_list.index(k)] = statistics.median(v)
        return cur_row

    row_list = [from_row_dict_to_row(rt_dict[sz], sz) for sz in size_list]

    df = pd.DataFrame(row_list, columns=cur_row_col_list)

    return df


def process_one_expr_dir(dir_name,
                         num_app,
                         num_fsp_worker,
                         is_fsp=True,
                         cpu_ut=False,
                         sz=None):
    if sz is not None:
        bench_log_names = [
            f'{dir_name}/bench_log_{sz}_{i}' for i in range(num_app)
        ]
    else:
        bench_log_names = [f'{dir_name}/bench_log_{i}' for i in range(num_app)]

    cpu_ut_out = None
    row_ll = []
    df_list = []
    for bench_log_name in bench_log_names:
        cur_row_list, cur_row_colname_list, cur_df = process_bench_log(
            bench_log_name)
        row_ll.append(cur_row_list)
        df_list.append(cur_df)
    app_summary_df = merge_rows_from_apps(row_ll, df_list,
                                          cur_row_colname_list, num_app,
                                          list(cur_df['size']))
    if is_fsp:
        fsp_out_df = None
        for cur_size in sorted(list(cur_df['size'])):
            wid_list = range(num_fsp_worker)
            worker_out_names = [
                f'{dir_name}/fsp_out_size{cur_size}/worker-{wid}-logger.out'
                for wid in wid_list
            ]

            for i in range(len(wid_list)):
                worker_out_name = worker_out_names[i]
                if not os.path.exists(worker_out_name):
                    continue

                wid = wid_list[i]
                cur_df = process_fsp_out(worker_out_name)
                cur_df['size'] = int(cur_size)
                cur_df['wid'] = int(wid)
                if fsp_out_df is None:
                    fsp_out_df = cur_df
                else:
                    fsp_out_df = pd.concat([cur_df, fsp_out_df])
        # display(fsp_out_df)
        if cpu_ut:
            if sz is not None:
                fsp_log_name = f'{dir_name}/fsp_log_{sz}'
            else:
                fsp_log_name = f'{dir_name}/fsp_log'
            mean_ut, total_ut, num_worker = process_fsp_log_cpu(
                fsp_log_name, cal_mean_sec_range=1.5)
            cpu_ut_out = mean_ut
    else:
        fsp_out_df = None
    return app_summary_df, fsp_out_df, cpu_ut_out


def gen_csv_for_dir(fstype, dirname, rno, cpu_ut, jobs, sz_list):
    if jobs is None:
        jobs = get_default_benchmarks()
    else:
        jobs = jobs.split(',')

    if sz_list is None:
        sz_list = [None]
    else:
        sz_list = sz_list.split(',')
    fs_prefix = fstype
    if fs_prefix.endswith("nj"):
        fs_prefix = fs_prefix[:-2]
    app_num_list = range(1, get_max_num_app() + 1)
    for job in jobs:
        for sz in sz_list:
            cur_expr_dir = f"{dirname}/{fs_prefix}_{job}_run_{rno}"
            logging.info(f'process {cur_expr_dir}')
            app_out_df_list = []
            for app_num in app_num_list:
                if bench_has_fg_sync(job):
                    app_reg_str = r'log_{}_[a-z|1]+_app_{}_sync-{}$'.format(
                        fs_prefix, app_num, get_syncop_num())
                else:
                    app_reg_str = r'log_{}_[a-z|1]+_app_{}$'.format(
                        fs_prefix, app_num)
                tmp_list = get_dir_name_list_match_pattern(
                    cur_expr_dir, app_reg_str)
                assert (len(tmp_list) == 1)
                cur_app_dir = tmp_list[0]
                logging.info(f'===> process app dir -- {cur_app_dir}')
                date_str = f'log{get_YEAR()}'
                dname_list = [
                    l for l in os.listdir(cur_app_dir) if date_str in l
                ]
                assert len(dname_list) == 1
                date_dir_name = dname_list[0]
                assert (date_str in date_dir_name)
                num_worker_list = [app_num]
                for num_worker in num_worker_list:
                    cur_reg = r'.*_isFsp-{}_clearPc-True_pinCpu-True-numFsWk-[0-9]|10$'.format(
                        is_fsp(fstype), num_worker)
                    cur_target_expr_dir_list = get_dir_name_list_match_pattern(
                        '{}/{}'.format(cur_app_dir, date_dir_name), cur_reg)
                    cur_target_expr_dir_list = filter_listdir_dir(
                        job, cur_target_expr_dir_list)
                    if len(cur_target_expr_dir_list) != 1:
                        logging.warn("More than one candidate path detected!")
                        logging.warn(f"cur_reg: {cur_reg}")
                        logging.warn(
                            f"cur_target_expr_dir_list: {cur_target_expr_dir_list}"
                        )
                        cur_target_expr_dir_list = [
                            d for d in cur_target_expr_dir_list
                            if d.endswith(f"numFsWk-{app_num}")
                        ]
                        logging.warn(f"Pick => {cur_target_expr_dir_list}")
                    assert (len(cur_target_expr_dir_list) == 1)
                    cur_target_expr_dir = cur_target_expr_dir_list[0]
                    app_out_df, fsp_out_df, cpu_ut_mean = process_one_expr_dir(
                        cur_target_expr_dir,
                        app_num,
                        num_worker,
                        is_fsp=True,
                        cpu_ut=cpu_ut,
                        sz=sz)
                    # add some fields
                    app_out_df['num_app'] = app_num
                    app_out_df['num_fs_wk'] = num_worker
                    app_out_df['fs_type'] = fstype
                    if cpu_ut:
                        app_out_df['cpu_ut'] = cpu_ut_mean
                    if fsp_out_df is not None:
                        fsp_out_df['num_app'] = app_num
                        fsp_out_df['num_fs_wk'] = num_worker
                        fsp_out_df['fs_type'] = fstype

                    app_out_df_list.append(app_out_df)
            cur_df = pd.concat(app_out_df_list)
            if sz is None:
                csv_filename = f'{dirname}/{job}.csv'
            else:
                csv_filename = f'{dirname}/{job}_{sz}.csv'
            cur_df.to_csv(csv_filename, index=False)


def main(args, loglevel):
    logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)
    gen_csv_for_dir(args.fs, args.dir, args.rno, args.cpu_ut, args.jobs,
                    args.sz)


def parse_cmd_args():
    parser = argparse.ArgumentParser(
        description="Parse FSP microbenchmark log")
    parser.add_argument('--fs',
                        help='file system type [fsp|ext4|ext4nj]',
                        required=True)
    parser.add_argument('--dir', help='dir name of the logs', required=True)
    parser.add_argument('--rno',
                        help='run number of this expr',
                        type=int,
                        default=0)
    parser.add_argument('--cpu_ut',
                        help='collect cpu utilization',
                        action='store_true')
    # e.g. --jobs RDPR --sz 4096,16384,32768,65536
    parser.add_argument('--jobs',
                        help='which specific benchmarks to process',
                        default=None)
    parser.add_argument('--sz',
                        help='which specific size to process',
                        default=None)
    return (parser.parse_args())


if __name__ == '__main__':
    loglevel = logging.DEBUG
    args = parse_cmd_args()
    main(args, loglevel)
