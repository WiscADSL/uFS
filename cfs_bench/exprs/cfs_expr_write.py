#!/usr/bin/env python3
# encoding: utf-8

from sarge import run, Capture
import time
import os

from cfs_test_common import get_ts_dir_name
from cfs_test_common import check_root
from cfs_test_common import clear_page_cache
from cfs_test_common import get_kfs_data_dir
from cfs_test_common import start_bench_coordinator

from cfs_test_common import get_expr_user
from cfs_test_common import expr_mkfs
from cfs_test_common import expr_mkfs_for_kfs
from cfs_test_common import start_fs_proc
from cfs_test_common import shutdown_fs
from cfs_test_common import get_default_bench_args
from cfs_test_common import get_microbench_bin
from cfs_test_common import write_file
from cfs_test_common import dump_expr_config
from cfs_test_common import get_div_str
from cfs_test_common import get_proj_log_dir
from cfs_test_common import mk_accessible_dir


# NOTE, do not call mkfs in read experiments

# 1 fsp threads, 1 bench thread
def expr_write_1fsp_1t(log_dir_name, clear_pgcache=False, pin_cpu=True,
                       is_seq=True, cur_numop=100, cur_value_size=4096,
                       is_value_random_size=False,
                       is_fsp=True, is_mkfs=False, cfg_update_dict=None):
    # is_mkfs is used to specify if file is pre-allocated or not
    if is_mkfs and is_fsp:
        expr_mkfs()
    if is_mkfs and not is_fsp:
        expr_mkfs_for_kfs()

    if clear_pgcache:
        clear_page_cache(is_slient=False)

    print(log_dir_name)
    bench_log_name = '{}/bench_log'.format(log_dir_name)
    fsp_log_name = '{}/fsp_log'.format(log_dir_name)
    expr_cfg_name = '{}/expr_cfg'.format(log_dir_name)

    # start FSP
    if is_fsp:
        p_fs, exit_signal_fname, ready_fname, fs_cmd = start_fs_proc()

    microbench_bin = get_microbench_bin(is_fsp)

    if not is_seq:
        bench_args = get_default_bench_args('rwrite')
    else:
        bench_args = get_default_bench_args('seqwrite')

    if cfg_update_dict is not None:
        bench_args.update(cfg_update_dict)

    bench_args['--numop='] = cur_numop
    bench_args['--value_size='] = cur_value_size
    bench_args['--histogram='] = 1
    if is_value_random_size:
        bench_args['--value_random_size='] = 1
    if not is_fsp:
        bench_args['--dir='] = get_kfs_data_dir()
    if pin_cpu:
        bench_args['--core_ids='] = '3'
    bench_r_cmd = '{} {}'. \
        format(microbench_bin,
               ' '.join(k + str(v) for k, v in bench_args.items()))

    print(bench_r_cmd)

    # coordinator
    if not is_fsp:
        start_bench_coordinator()
        print('coordinator started')

    # p_bench_r = run(bench_r_cmd)
    p_bench_r = run(bench_r_cmd, stdout=Capture())

    if is_fsp:
        shutdown_fs(exit_signal_fname, p_fs)

    print(get_div_str('DONE!'))

    if is_fsp:
        if os.path.exists(ready_fname):
            print('readFile:{} exists'.format(ready_fname))
            os.remove(ready_fname)
        write_file(fsp_log_name, p_fs.stdout.text)
    write_file(bench_log_name, p_bench_r.stdout.text)
    bench_args['clear_page_cache'] = clear_pgcache
    bench_args['pin_cpu'] = pin_cpu
    bench_args['is_seq'] = is_seq
    bench_args['is_mkfs'] = is_mkfs
    dump_expr_config(expr_cfg_name, bench_args)


def expr_run_1(log_dir, is_fsp=True, cfg_update_dict=None):
    case_name = 'seqwrite'
    case_log_dir = '{}/{}'.format(log_dir, case_name)
    #clear_pc_list = [True, False]
    clear_pc_list = [True]
    pin_cpu_list = [True, False]
    # num_op_list = [100, 1000, 10000, 100000]
    # value_size_list = [64, 1024, 4096, 16384]
    # value_random_size_list = [True, False]
    value_random_size_list = [False]
    value_sz_op_num_dict = {
        64: 1000000,
        1024: 1000000,
        4096: 100000,
        16384: 100000,
    }
    for cp in clear_pc_list:
        for pc in pin_cpu_list:
            for vs, nop in value_sz_op_num_dict.items():
                for vr in value_random_size_list:
                    cur_run_log_dir = \
                        '{}_isFsp-{}_clearPc-{}_pinCpu-{}-valRandSz-{}'. \
                        format(case_log_dir, str(is_fsp), str(cp),
                               str(pc), str(vr))
                    mk_accessible_dir(cur_run_log_dir)
                    expr_write_1fsp_1t(cur_run_log_dir, clear_pgcache=cp,
                                       pin_cpu=pc, is_seq=True,
                                       cur_numop=nop,
                                       cur_value_size=vs,
                                       is_value_random_size=vr,
                                       is_fsp=is_fsp,
                                       is_mkfs=False,
                                       cfg_update_dict=cfg_update_dict)
                # make sure ts log name will not be overlapped
                time.sleep(2)


def expr_run_2(log_dir, is_fsp=True, cfg_update_dict=None):
    case_name = 'rwrite'
    case_log_dir = '{}/{}'.format(log_dir, case_name)
    clear_pc_list = [True, False]
    pin_cpu_list = [True, False]
    num_op_list = [100, 1000, 10000, 100000]
    value_size_list = [64, 1024, 4096, 16384]
    value_random_size_list = [True, False]
    for cp in clear_pc_list:
        for pc in pin_cpu_list:
            for nop in num_op_list:
                for vs in value_size_list:
                    for vr in value_random_size_list:
                        cur_run_log_dir = \
                            '{}_isFsp-{}_clearPc-{}_pinCpu-{}_valRandsz-{}'. \
                            format(case_log_dir, str(is_fsp), str(cp),
                                   str(pc), str(vr))
                        mk_accessible_dir(cur_run_log_dir)
                        expr_write_1fsp_1t(cur_run_log_dir, clear_pgcache=cp,
                                           pin_cpu=pc, is_seq=False,
                                           cur_numop=nop,
                                           cur_value_size=vs,
                                           is_value_random_size=vr,
                                           is_fsp=is_fsp,
                                           cfg_update_dict=cfg_update_dict)
                        time.sleep(2)


def expr_run_append(log_dir, is_fsp=True, cfg_update_dict=None):
    case_name = 'append'
    case_log_dir = '{}/{}'.format(log_dir, case_name)
    # clear_pc_list = [True, False]
    clear_pc_list = [True]
    pin_cpu_list = [True, False]
    value_sz_op_num_dict = {
        64: 1000000,
        1024: 1000000,
        4096: 100000,
        16384: 100000,
    }
    for cp in clear_pc_list:
        for pc in pin_cpu_list:
            for sz, op_num in value_sz_op_num_dict.items():
                cur_run_log_dir = \
                    '{}_isFsp-{}_clearPc-{}_pinCpu-{}-valRandSz-{}'.format(
                        case_log_dir,
                        str(is_fsp),
                        str(cp), str(pc),
                        str(False))
                mk_accessible_dir(cur_run_log_dir)
                expr_write_1fsp_1t(cur_run_log_dir, clear_pgcache=cp,
                                   pin_cpu=pc, is_seq=True, cur_numop=op_num,
                                   cur_value_size=sz, is_fsp=is_fsp,
                                   is_mkfs=True,
                                   cfg_update_dict=cfg_update_dict)
                time.sleep(2)


def main():
    # by default, each script will not do experiments
    # seq write (pre-allocated)
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_1(cur_log_dir, is_fsp=True)
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_1(cur_log_dir, is_fsp=False)

    # random write (pre-allocated)
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_2(cur_log_dir, is_fsp=True)
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_2(cur_log_dir, is_fsp=False)

    # aligned random write
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_2(cur_log_dir, is_fsp=True, cfg_update_dict={
    #     '--rw_align_bytes=': 4096})
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_2(cur_log_dir, is_fsp=False, cfg_update_dict={
    #     '--rw_align_bytes=': 4096})

    # append (file size start from 0, basically do mkfs everytime)
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_append(cur_log_dir, is_fsp=True)
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_append(cur_log_dir, is_fsp=False)
    pass


if __name__ == '__main__':
    is_root = check_root()
    if not is_root:
        print('Run as root required!')
        exit(1)
    main()
