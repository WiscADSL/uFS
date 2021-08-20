#!/usr/bin/env python3
# encoding: utf-8

from sarge import run, Capture
import time

from cfs_test_common import get_ts_dir_name
from cfs_test_common import check_root
from cfs_test_common import clear_page_cache
from cfs_test_common import get_kfs_data_dir

from cfs_test_common import get_expr_user
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

def do_warmup_seqread(is_fsp, cfg_update_dict):
    microbench_bin = get_microbench_bin(is_fsp)
    # sequential read 512 M at max
    _max_warm_up_bytes = 1024 * 1024 * 512
    warm_up_args = get_default_bench_args('seqread')
    if cfg_update_dict is None \
            or '--max_file_size=' not in cfg_update_dict.keys():
        warm_up_args['--max_file_size='] = _max_warm_up_bytes
    else:
        warm_up_args['--max_file_size='] = cfg_update_dict[
            '--max_file_size=']
    if not is_fsp:
        warm_up_args['--dir='] = get_kfs_data_dir()
    if warm_up_args['--max_file_size='] > _max_warm_up_bytes:
        warm_up_args['--max_file_size='] = _max_warm_up_bytes
    warm_up_args['--value_size='] = int(8 * 1024)
    warm_up_args['--numop='] = int(
        warm_up_args['--max_file_size='] / warm_up_args['--value_size='])
    warm_up_cmd = '{} {}'. \
        format(microbench_bin,
               ' '.join(k + str(v) for k, v in warm_up_args.items()))
    print('run warmup command: {}'.format(warm_up_cmd))
    p_warmup = run(warm_up_cmd)
    print(p_warmup.stdout)
    # warm up done
    print(get_div_str('Warmed'))


# 1 fsp threads, 1 bench thread
def expr_read_1fsp_1t(log_dir_name, clear_pgcache=False, pin_cpu=True,
                      is_seq=True, cur_numop=100, cur_value_size=4096,
                      is_fsp=True, warm_up=False, cfg_update_dict=None):
    if clear_pgcache:
        clear_page_cache(is_slient=False)

    print(log_dir_name)
    bench_log_name = '{}/bench_log'.format(log_dir_name)
    fsp_log_name = '{}/fsp_log'.format(log_dir_name)
    expr_cfg_name = '{}/expr_cfg'.format(log_dir_name)

    mk_accessible_dir(log_dir_name)

    # start FSP
    if is_fsp:
        p_fs, exit_signal_fname = start_fs_proc()

    if warm_up:
        do_warmup_seqread(is_fsp, cfg_update_dict)

    microbench_bin = get_microbench_bin(is_fsp)

    if not is_seq:
        bench_args = get_default_bench_args('rread')
        bench_args['--rand_no_overlap='] = 1
    else:
        bench_args = get_default_bench_args('seqread')

    if cfg_update_dict is not None:
        bench_args.update(cfg_update_dict)

    bench_args['--numop='] = cur_numop
    bench_args['--value_size='] = cur_value_size
    bench_args['--histogram='] = 1
    if not is_fsp:
        bench_args['--dir='] = get_kfs_data_dir()
    if pin_cpu:
        bench_args['--core_ids='] = '3'
    bench_r_cmd = '{} {}'. \
        format(microbench_bin,
               ' '.join(k + str(v) for k, v in bench_args.items()))

    print(bench_r_cmd)

    # p_bench_r = run(bench_r_cmd)
    p_bench_r = run(bench_r_cmd, stdout=Capture())

    if is_fsp:
        shutdown_fs(exit_signal_fname, p_fs)

    print(get_div_str('DONE!'))

    if is_fsp:
        write_file(fsp_log_name, p_fs.stdout.text)
    write_file(bench_log_name, p_bench_r.stdout.text)
    bench_args['clear_page_cache'] = clear_pgcache
    bench_args['pin_cpu'] = pin_cpu
    bench_args['is_seq'] = is_seq
    bench_args['warm_up'] = warm_up
    dump_expr_config(expr_cfg_name, bench_args)


def expr_run_1(log_dir, is_fsp=True, warm_up=False, cfg_update_dict=None):
    case_name = 'seqread'
    case_log_dir = '{}/{}'.format(log_dir, case_name)
    clear_pc_list = [True, False]
    pin_cpu_list = [True, False]
    num_op_list = [100, 1000, 10000, 100000, 999990]
    num_op_list = [100000, 999990]
    value_size_list = [64, 1024, 4096, 16384]
    for cp in clear_pc_list:
        for pc in pin_cpu_list:
            for nop in num_op_list:
                for vs in value_size_list:
                    cur_run_log_dir = '{}_isFsp-{}_clearPc-{}_pinCpu-{}'.format(
                        case_log_dir, str(is_fsp), str(cp), str(pc))
                    mk_accessible_dir(cur_run_log_dir)
                    expr_read_1fsp_1t(cur_run_log_dir, clear_pgcache=cp,
                                      pin_cpu=pc, is_seq=True, cur_numop=nop,
                                      cur_value_size=vs, is_fsp=is_fsp,
                                      warm_up=warm_up,
                                      cfg_update_dict=cfg_update_dict)
                    # make sure ts log name will not be overlapped
                    time.sleep(2)


def expr_run_2(log_dir, is_fsp=True, cfg_update_dict=None):
    case_name = 'rrand'
    case_log_dir = '{}/{}'.format(log_dir, case_name)
    # clear_pc_list = [True, False]
    clear_pc_list = [True]
    pin_cpu_list = [True, False]
    value_sz_op_num_dict = {
        64: 100000,
        1024: 100000,
        4096: 100000,
        16384: 100000,
    }
    warm_up_list = [True, False]
    for cp in clear_pc_list:
        for pc in pin_cpu_list:
            for vs, nop in value_sz_op_num_dict.items():
                for wm in warm_up_list:
                    cur_run_log_dir = \
                        '{}_isFsp-{}_clPc-{}_pinCpu-{}-Warm-{}'. \
                        format(case_log_dir, str(is_fsp), str(cp),
                               str(pc), str(wm))
                    mk_accessible_dir(cur_run_log_dir)
                    expr_read_1fsp_1t(cur_run_log_dir, clear_pgcache=cp,
                                      pin_cpu=pc, is_seq=False,
                                      cur_numop=nop,
                                      cur_value_size=vs, is_fsp=is_fsp,
                                      warm_up=wm,
                                      cfg_update_dict=cfg_update_dict)
                    time.sleep(2)


def main():
    # by default, each script will not do experiments
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_1(cur_log_dir, is_fsp=True)
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_2(cur_log_dir, is_fsp=True)
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_1(cur_log_dir, is_fsp=False)
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_2(cur_log_dir, is_fsp=False)

    # let offset aligned
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_2(cur_log_dir, is_fsp=True, cfg_update_dict={
    #     '--rw_align_bytes=': 4096})
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_2(cur_log_dir, is_fsp=False, cfg_update_dict={
    #     '--rw_align_bytes=': 4096})

    # cached random read
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_2(cur_log_dir, is_fsp=True, cfg_update_dict={
    #     '--max_file_size=': 1048576, '--rand_no_overlap=': 0})
    # cur_log_dir = get_proj_log_dir(get_expr_user(), suffix=get_ts_dir_name())
    # expr_run_2(cur_log_dir, is_fsp=False, cfg_update_dict={
    #     '--max_file_size=': 1048576, '--rand_no_overlap=': 0})
    pass


if __name__ == '__main__':
    is_root = check_root()
    if not is_root:
        print('Run as root required!')
        exit(1)
    main()
