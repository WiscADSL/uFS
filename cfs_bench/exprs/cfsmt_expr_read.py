#!/usr/bin/env python3
# encoding: utf-8

import sys
import os
import time
import psutil

from sarge import run, Capture
import cfs_test_common as cfs_tc
from cfs_test_common import start_bench_coordinator

'''
NOTE: assume data is initialized by "init_mt_bench_file.py*
'''


def expr_write_one_log(cmd, log_name, content):
    cfs_tc.write_file(log_name, cmd)
    cfs_tc.write_file(log_name, '\n{}\n'.format(
        cfs_tc.get_div_str('')))
    cfs_tc.write_file(log_name, content)


# read experiment run with multi-threading FSP and multi-app
def expr_read_mtfsp_multiapp(
        log_dir_name,
        num_fsp_worker,
        num_app_proc,
        bench_cfg_dict,
        is_fsp=True,
        clear_pgcache=False,
        pin_cpu=False,
        per_app_fname=None,
        per_app_flist=None,
        per_app_name_prefix=None,
        per_app_dir_name=None,
        per_app_dir2_name=None,
        per_app_block_no=None,
        dump_mpstat=False,
        dump_iostat=False,
        perf_cmd=None,
        log_no_save=False,
        is_share=False,
        per_app_cfg_dict=None,
        bypass_exit_sync=False):
    """
    :per_app_name_prefix: only used for mkdir/create
    """
    if is_fsp and dump_mpstat:
        print('Error dump_mpstat is only supported for kernel FS')
        return
    if is_fsp and dump_iostat:
        print('Error dump_iostat is only supported for kernel FS')
        return

    # clear page cache
    if clear_pgcache:
        cfs_tc.clear_page_cache(is_slient=False)

    print(log_dir_name)
    bench_log_name = '{}/bench_log'.format(log_dir_name)
    fsp_log_name = '{}/fsp_log'.format(log_dir_name)
    expr_cfg_name = '{}/expr_cfg'.format(log_dir_name)

    # make sure essential fields are passed in via the dict
    cfg_essential_fields = [
        '--benchmarks=',
        '--numop=',
        '--value_size=',
    ]

    for k in cfg_essential_fields:
        if bench_cfg_dict is not None:
            if k not in bench_cfg_dict:
                print('ERROR must-have field:{} missing'.format(k))
                sys.exit(1)
    bench_args = {
        '--threads=': 1,
        '--histogram=': 1,
    }
    if bench_cfg_dict is not None:
        bench_args.update(bench_cfg_dict)

    if per_app_name_prefix is not None:
        assert (bench_args['--benchmarks=']
                in ['create', 'mkdir', 'unlink', 'rename']) or ('dynamic' in bench_args['--benchmarks='])

    # update args to pin benchmarking process to specific core
    if pin_cpu:
        per_app_core_id = {}
        if is_fsp:
            for i in range(num_app_proc):
                per_app_core_id[i] = 11 + i
        else:
            for i in range(num_app_proc):
                per_app_core_id[i] = 1 + i
        print('App core pinning:{}'.format(per_app_core_id))

    # update the targeting files
    if per_app_fname is not None:
        for i in range(num_app_proc):
            if i not in per_app_fname:
                print('fname not provided for app:{}', i)
                sys.exit(1)
    else:
        per_app_fname = {}
        for i in range(num_app_proc):
            per_app_fname[i] = 'bench_f_{}'.format(i)

    if dump_mpstat:
        # config for mpstat
        mpstat_log_name = '{}/size-{}_mpstat_log'.format(
            log_dir_name, bench_args['--value_size='])
        print(mpstat_log_name)
        # number of sec to report mpstat
        mpstat_interval_sec = 1
        # number of mpstat report
        mpstat_report_cnt = 10

    if dump_iostat:
        # config for iostat
        iostat_log_name = '{}/size-{}_iostat_log'.format(
            log_dir_name, bench_args['--value_size='], )
        print(iostat_log_name)
        # number of sec to report iostat
        iostat_interval_sec = 1
        # number of iostat report
        iostat_report_cnt = 10

    # update parameters for kernel FS benchmarking
    if not is_fsp:
        if '--dir=' not in bench_args:
            if per_app_dir_name is None:
                bench_args['--dir='] = cfs_tc.get_kfs_data_dir()
        if dump_mpstat:
            # start mpstat
            # mpstat_cmd = 'mpstat -P ALL {} {}'.format(mpstat_interval_sec,
            #                                          mpstat_report_cnt)
            # mpstat_cmd ='mpstat -p ${}'
            # print(mpstat_cmd)
            # p_mpstat = run(mpstat_cmd, stdout=Capture(), async_=True)
            start_cpu_pct = psutil.cpu_percent(interval=None, percpu=True)
            start_cpu_ts = time.time()
            # start_cpu_times = psutil.cpu_times(percpu=True)
            start_cpu_times = psutil.cpu_times()
        if dump_iostat:
            # start iostat
            # iostat_cmd = 'iostat -m -p nvme1n1 {} {}'.format(
            # iostat_cmd = 'iostat -x {} {}'.format(
            #    iostat_interval_sec,
            #    iostat_report_cnt)
            # print(iostat_cmd)
            #p_iostat = run(iostat_cmd, stdout=Capture(), async_=True)
            disks_before = psutil.disk_io_counters(perdisk=True)

    # start coordinator here
    if not is_fsp:
        start_bench_coordinator(num_app_proc=num_app_proc)
        print('cordinator started num_app_proc:{}'.format(num_app_proc))

    #
    # generate benchmarking command for each client
    #

    # client bin name
    microbench_bin = cfs_tc.get_microbench_bin(is_fsp)

    numactl_cmd = ''
    #numactl_cmd = 'numactl --cpunodebind=0 --membind=0'
    bench_app_cmd_dict = {}
    if per_app_cfg_dict is None:
        for i in range(num_app_proc):
            bench_app_cmd_dict[i] = '{} {} {}'. \
                format(numactl_cmd, microbench_bin,
                       ' '.join(k + str(v) for k, v in bench_args.items()))
        if is_fsp and num_fsp_worker > 1 and (not is_share):
            cur_wid = 0
            for aid in range(num_app_proc):
                bench_app_cmd_dict[aid] = '{} --wid={}'.format(bench_app_cmd_dict[aid], cur_wid)
                cur_wid = (cur_wid + 1) % num_fsp_worker
    else:
        assert(len(per_app_cfg_dict) == num_app_proc)
        for i in range(num_app_proc):
            bench_app_cmd_dict[i] = '{} {} {}'. \
                format(numactl_cmd, microbench_bin,
                       ' '.join(k + str(v) for k, v in per_app_cfg_dict[i].items()))
    if is_fsp:
        # start FSP
        app_key_dict, p_fs, exit_signal_fname, ready_fname, fsp_cmd = cfs_tc.start_mtfsp(
            num_fsp_worker, num_app_proc, bypass_exit_sync=bypass_exit_sync)

        # update benchmarking cmds according to the returned shmkey
        for i in range(num_app_proc):
            bench_app_cmd_dict[i] = '{} --fs_worker_key_list={} ' \
                                    '--fname={}'. \
                format(bench_app_cmd_dict[i], app_key_dict[i],
                       per_app_fname[i])
    else:
        # only update fname for benchmarking kernelFS
        for i in range(num_app_proc):
            bench_app_cmd_dict[i] = '{} --fname={}'. \
                format(bench_app_cmd_dict[i],
                       per_app_fname[i])
    if pin_cpu:
        for i in range(num_app_proc):
            bench_app_cmd_dict[i] = '{} --core_ids={},'. \
                format(bench_app_cmd_dict[i], per_app_core_id[i])

    if per_app_name_prefix is not None:
        for i in range(num_app_proc):
            if i in per_app_name_prefix:
                bench_app_cmd_dict[i] = '{} --dirfile_name_prefix={}'. \
                    format(bench_app_cmd_dict[i], per_app_name_prefix[i])

    if per_app_dir_name is not None:
        for i in range(num_app_proc):
            cur_app_dir_name = per_app_dir_name[i]
            if not is_fsp:
                cur_app_dir_name = '{}/{}'.format(cfs_tc.get_kfs_data_dir(),
                                                  cur_app_dir_name)
            bench_app_cmd_dict[i] = '{} --dir={}'. \
                format(bench_app_cmd_dict[i], cur_app_dir_name)

    if per_app_dir2_name is not None:
        for i in range(num_app_proc):
            cur_app_dir2_name = per_app_dir2_name[i]
            if not is_fsp:
                cur_app_dir2_name = '{}/{}'.format(cfs_tc.get_kfs_data_dir(),
                                                   cur_app_dir2_name)
            bench_app_cmd_dict[i] = '{} --dir2={}'. \
                format(bench_app_cmd_dict[i], cur_app_dir2_name)
    if per_app_block_no is not None:
        for i in range(num_app_proc):
            bench_app_cmd_dict[i] = '{} --block_no={}'.format(
                bench_app_cmd_dict[i], per_app_block_no[i])

    if perf_cmd is not None:
        if 'ftrace' in perf_cmd:
            for i in bench_app_cmd_dict.keys():
                bench_app_cmd_dict[i] = '{} {}'.format(
                    perf_cmd, bench_app_cmd_dict[i])
        else:
            for i in bench_app_cmd_dict.keys():
                bench_app_cmd_dict[i] = '{} -o {}/perf_out_app{} {}'.format(
                    perf_cmd, log_dir_name, i, bench_app_cmd_dict[i])

    if per_app_flist is not None:
        for i in per_app_flist:
            if i in per_app_flist:
                bench_app_cmd_dict[i] = '{} --flist={}'.format(
                    bench_app_cmd_dict[i], per_app_flist[i]
                )

    print(bench_app_cmd_dict)

    # start benchmarking clients
    p_bench_r_dict = {}
    for i in range(num_app_proc):
        # print(bench_app_cmd_dict[i])
        p_bench_r_dict[i] = run(bench_app_cmd_dict[i], stdout=Capture(),
                                async_=True)

    # wait for clients finishing
    for pr in p_bench_r_dict.values():
        pr.wait()

    if dump_mpstat:
        end_cpu_pct = psutil.cpu_percent(interval=None, percpu=True)
        # the output of percpu=True is somewhat noisy
        # end_cpu_times = psutil.cpu_times(percpu=True)
        end_cpu_times = psutil.cpu_times()
        end_cpu_ts = time.time()

    if dump_iostat:
        disks_after = psutil.disk_io_counters(perdisk=True)

    time.sleep(2)

    if is_fsp:
        # shutdown FSP
        cfs_tc.shutdown_fs(exit_signal_fname, p_fs)
        p_fs.wait()
    else:
        if dump_mpstat:
            # wait for mpstat finishing
            # p_mpstat.wait()
            pass
        if dump_iostat:
            # wait for iostat finishing
            # p_iostat.wait()
            pass

    print(cfs_tc.get_div_str('DONE!'))

    if log_no_save:
        print('THIS RUN does not save log!')
        return

    if is_fsp:
        # cleanup readySignal generated by FSP
        if os.path.exists(ready_fname):
            print('readyFile:{} exists. Do REMOVE'.format(ready_fname))
            time.sleep(1)
            os.remove(ready_fname)
        # collect FSP output
        expr_write_one_log(fsp_cmd, fsp_log_name, p_fs.stdout.text)
    else:
        if dump_mpstat:
            # collect mpstat output
            start_ps_str = str(start_cpu_pct)
            end_ps_str = str(end_cpu_pct)
            mp_text = 'cpu-start:\n-{}\n-{}\n cpu-end:\n-{}\n-{}\n interval_sec:{}'.\
                format(start_ps_str, str(start_cpu_times), end_ps_str,
                       str(end_cpu_times), end_cpu_ts - start_cpu_ts)
            expr_write_one_log('psutil', mpstat_log_name, mp_text)
            # expr_write_one_log(mpstat_cmd, mpstat_log_name,
            #                   p_mpstat.stdout.text)
        if dump_iostat:
            # collect iostat output
            # expr_write_one_log(iostat_cmd, iostat_log_name,
            #                   p_iostat.stdout.text)
            def gen_disk_text(stat_before, stat_after):
                result_dict = {}
                for k, v in stat_before.items():
                    if 'nvme' not in k:
                        continue
                    if len(k) != 7:
                        # it's likely that nvme device is named in this way:
                        # 'nvme{i}n1'
                        continue
                    v_after = stat_after[k]
                    result_dict[k] = {}
                    result_dict[k]['read_count'] = v_after.read_count - \
                        v.read_count
                    result_dict[k]['write_count'] = v_after.write_count - \
                        v.write_count
                    result_dict[k]['read_bytes'] = v_after.read_bytes - \
                        v.read_bytes
                    result_dict[k]['write_bytes'] = v_after.write_bytes - \
                        v.write_bytes
                    result_dict[k]['read_time'] = v_after.read_time - \
                        v.read_time
                    result_dict[k]['write_time'] = v_after.write_time - \
                        v.write_time
                    result_dict[k]['read_merged_count'] = v_after.read_merged_count - \
                        v.read_merged_count
                    result_dict[k]['write_merged_count'] = v_after.write_merged_count - \
                        v.write_merged_count
                return result_dict
            cur_result = gen_disk_text(disks_before, disks_after)
            mp_text = str(cur_result)
            expr_write_one_log('psutil', iostat_log_name, mp_text)

    # collect benchmark output
    for k, v in p_bench_r_dict.items():
        cur_fname = '{}_{}'.format(bench_log_name, k)
        expr_write_one_log(str(bench_app_cmd_dict[k]), cur_fname,
                           v.stdout.text)
    cfs_tc.dump_expr_config(expr_cfg_name, bench_args)

    if is_fsp:
        # collect FSP's per-worker output log
        out_save_dir_name = '{}/fsp_out_size{}/'.format(log_dir_name,
                                                        bench_args[
                                                            '--value_size='])
        os.mkdir('{}'.format(out_save_dir_name))
        os.system('mv logs/* {}'.format(out_save_dir_name))
        os.system('rm -rf logs/*')


def bench_rand_read(
        log_dir,
        num_app_proc=1,
        is_fsp=True,
        is_seq=False,
        is_share=False,
        num_fsp_worker_list=None,
        strict_no_overlap=False,
        per_app_fname=None,
        dump_mpstat=False,
        dump_iostat=False,
        cfs_update_dict=None):
    """
    :param log_dir:
    :param num_app_proc:
    :param is_fsp:
    :param num_fsp_worker_list: E.g., [1, 2, 3, 4], will run for each #
    :param strict_no_overlap: will aligned to 4K size IO and enforce each IO
    has no overlap, thus each request ==> disk access
    :param per_app_fname: assign a for each app
    :param dump_mpstat: kernel FS only
    :param dump_iostat: kerenfel FS only
    :param cfs_update_dict: more customization
    :return:
    """
    if is_seq:
        case_name = 'seqread'
        bench_cfg_dict = {
            '--benchmarks=': 'seqread',
        }
    else:
        case_name = 'randread'
        # bench_cfg_dict = {
        # '--bnchmarks=': 'drreadm',
        # '--value_size=': 4096,
        # }
        bench_cfg_dict = {
            '--benchmarks=': 'rread',
        }
    case_log_dir = '{}/{}'.format(log_dir, case_name)

    if strict_no_overlap:
        bench_cfg_dict['--rw_align_bytes='] = 4096
        bench_cfg_dict['--rand_no_overlap='] = 1
    else:
        # avoid the cross-block reading
        # because KFS will have read-ahead to benefit from
        bench_cfg_dict['--rw_align_bytes='] = 4096

    if cfs_update_dict is not None:
        bench_cfg_dict.update(cfs_update_dict)

    # note for rand-read, one strict-no-overlap is set, 64 needs same size
    # as 4K
    value_sz_op_num_dict = {
        # 64: 500000,
        # 1024: 500000,
        #4096: 500000,
        4096: int(2*1024*1024/4),
        # 16384: int(2*1024*1024/16),
        # 32768: int(2*1024*1024/32),
        # 65536: int(2*1024*1024/64),
    }
    if is_share:
        # value_sz_op_num_dict = {
        #    4096: 0,
        # }
        for sz in value_sz_op_num_dict.keys():
            value_sz_op_num_dict[sz] = int(
                int((5 * 1024 * 1024) / 4) / num_app_proc) - 2
            if value_sz_op_num_dict[sz] > 500000:
                value_sz_op_num_dict[sz] = 500000
    # pin_cpu_list = [False, True]
    pin_cpu_list = [True]
    clear_pc_list = [True]
    if num_fsp_worker_list is None:
        num_fsp_worker_list = [1]
    for cp in clear_pc_list:
        for pc in pin_cpu_list:
            for vs, nop in value_sz_op_num_dict.items():
                for nfswk in num_fsp_worker_list:
                    cur_run_log_dir = \
                        '{}_isFsp-{}_clearPc-{}_pinCpu-{}-numFsWk-{}'.format(
                            case_log_dir, str(is_fsp), str(cp), str(pc),
                            str(nfswk))
                    bench_cfg_dict['--value_size='] = vs
                    if vs > 4096:
                        bench_cfg_dict['--rw_align_bytes='] = 4096 * \
                            (int((vs - 1) / 4096) + 1)
                    bench_cfg_dict['--numop='] = nop
                    cfs_tc.mk_accessible_dir(cur_run_log_dir)
                    expr_read_mtfsp_multiapp(cur_run_log_dir, nfswk,
                                             num_app_proc, bench_cfg_dict,
                                             is_fsp=is_fsp, clear_pgcache=True,
                                             pin_cpu=pc,
                                             per_app_fname=per_app_fname,
                                             is_share=is_share,
                                             dump_mpstat=dump_mpstat,
                                             dump_iostat=dump_iostat)
                    time.sleep(2)


def bench_cached_read(log_dir, num_app_proc=1, is_fsp=True,
                      num_fsp_worker_list=None, block_no=-1,
                      per_app_fname=None,
                      is_seq=False,
                      perf_cmd=None,
                      dump_mpstat=False, dump_iostat=False,
                      is_share=False,
                      cfs_update_dict=None):
    case_name = 'crread'
    case_log_dir = '{}/{}'.format(log_dir, case_name)
    cur_align_bytes = 4096
    bench_cfg_dict = {
        '--benchmarks=': 'crread',
    }
    if is_seq:
        case_name = 'csread'
        bench_cfg_dict = {
            '--benchmarks=': 'csread',
        }

    if block_no >= 0:
        bench_cfg_dict['--block_no='] = block_no
        bench_cfg_dict['--in_mem_file_size='] = 64 * 1024

    if cfs_update_dict is not None:
        bench_cfg_dict.update(cfs_update_dict)

    # note for rand-read, one strict-no-overlap is set, 64 needs same size
    # as 4K
    value_sz_op_num_dict = {
        # 64: 10000000,
        # 1024: 10000000,
        4096: 10000000,
        # 16384: 1000000,
    }
    if block_no >= 0:
        del(value_sz_op_num_dict[64])

    # pin_cpu_list = [True, False]
    pin_cpu_list = [True]
    clear_pc_list = [True]
    if num_fsp_worker_list is None:
        num_fsp_worker_list = [1]
    for cp in clear_pc_list:
        for pc in pin_cpu_list:
            for vs, nop in value_sz_op_num_dict.items():
                for nfswk in num_fsp_worker_list:
                    cur_run_log_dir = \
                        '{}_isFsp-{}_clearPc-{}_pinCpu-{}-numFsWk-{}'.format(
                            case_log_dir, str(is_fsp), str(cp), str(pc),
                            str(nfswk))
                    bench_cfg_dict['--value_size='] = vs
                    bench_cfg_dict['--numop='] = nop
                    if vs > cur_align_bytes:
                        bench_cfg_dict['--rw_align_bytes='] = 4096 * \
                            (int((vs - 1) / 4096) + 1)
                    else:
                        bench_cfg_dict['--rw_align_bytes='] = cur_align_bytes

                    cfs_tc.mk_accessible_dir(cur_run_log_dir)
                    expr_read_mtfsp_multiapp(cur_run_log_dir, nfswk,
                                             num_app_proc, bench_cfg_dict,
                                             is_fsp=is_fsp, clear_pgcache=cp,
                                             pin_cpu=pc,
                                             per_app_fname=per_app_fname,
                                             dump_mpstat=dump_mpstat,
                                             dump_iostat=dump_iostat,
                                             is_share=is_share,
                                             perf_cmd=perf_cmd)
                    time.sleep(2)


def main():
    # random read
    # fsp
    # cur_log_dir = cfs_tc.get_proj_log_dir(cfs_tc.get_expr_user(),
    #                                       suffix=cfs_tc.get_ts_dir_name(),
    #                                       do_mkdir=True)
    # bench_rand_read(cur_log_dir, num_app_proc=3, is_fsp=True)
    # ext4
    # cur_log_dir = cfs_tc.get_proj_log_dir(cfs_tc.get_expr_user(),
    #                                       suffix=cfs_tc.get_ts_dir_name(),
    #                                       do_mkdir=True)
    # bench_rand_read(cur_log_dir, num_app_proc=3, is_fsp=False,
    #                 dump_mpstat=True, dump_iostat=False)
    pass


if __name__ == '__main__':
    is_root = cfs_tc.check_root()
    if not is_root:
        print('Run as root required!')
        exit(1)
    main()
