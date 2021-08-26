#!/usr/bin/env python3
# encoding: utf-8

import sys
import logging
import os
import argparse
import subprocess
import cfs_test_common as cfs_common

# FSP whole set of microbenchmark

DEV_NAME = "/dev/" + os.environ["SSD_NAME"]
PCIE_ADDR = os.environ["SSD_PICE_ADDR"]

def get_host_name():
    return os.uname().nodename


def get_commit_id():
    commit_id = (subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'],
        cwd=cfs_common.get_cfs_root_dir())).decode('UTF-8').strip()
    return commit_id


def get_spdk_setup_bin():
    setup_bin = '{}/{}'.format(cfs_common.get_cfs_root_dir(),
                               'cfs/lib/spdk/scripts/setup.sh')
    return setup_bin


# This way to get NUMA number is not very clean... `lscpu` is really for human,
# not for machine, but it works on all the machines we used...
# Maybe update it in the future
def get_numa_node_num():
    check_numa_out = subprocess.check_output(
        "lscpu | grep -i NUMA", shell=True, encoding='utf-8').strip().split("\n")
    # first line should be like "NUMA node(s):         2"
    # rest of lines should be mapping of each NUMA nodes to core ids
    numa_node_num = int(check_numa_out[0].split()[-1])
    assert numa_node_num == len(check_numa_out) - 1
    return numa_node_num


def setup_spdk(numa_node_num=None):
    setup_bin = '{}/{}'.format(cfs_common.get_cfs_root_dir(),
                               'cfs/lib/spdk/scripts/setup.sh')
    if numa_node_num is None:
        numa_node_num = get_numa_node_num()
    print(f"SSD PCIe Address: {PCIE_ADDR}")
    numa_node_list = range(numa_node_num)
    for no in numa_node_list:
        ret = subprocess.call(
            'HUGEMEM=16384 PCI_WHITELIST="{}" HUGENODE={} {}'.format(
                PCIE_ADDR, no, setup_bin), shell=True)
        if ret != 0:
            logging.warn('cannot pin mem into node {}'.format(no))
    ret = subprocess.call(
        'echo 0 | sudo tee /proc/sys/kernel/randomize_va_space', shell=True)
    if ret == 0:
        logging.info('ASLR (address space layout randomization disabled')
    logging.info('setup spdk done')


def reset_spdk():
    setup_bin = '{}/{}'.format(cfs_common.get_cfs_root_dir(),
                               'cfs/lib/spdk/scripts/setup.sh')
    ret = subprocess.call('{} reset'.format(setup_bin), shell=True)
    if ret == 0:
        logging.info('reset spdk done')


def setup_ext4(has_journal=True, readahead_kb=None, delay_allocate=True):
    cmd = 'mkfs -F -t ext4 {}'.format(DEV_NAME)
    logging.debug(cmd)
    ret = subprocess.call(cmd, shell=True)
    if ret != 0:
        logging.error("cannot mkfs")
        return
    dir_name = cfs_common.get_kfs_mount_dir()
    subprocess.call('sudo mkdir -p {}'.format(dir_name), shell=True)
    if not has_journal:
        ret = subprocess.call(
            'tune2fs -O ^has_journal {}'.format(DEV_NAME), shell=True)
        if ret != 0:
            logging.error("Cannot disable journal")
    # mount
    dealloc_opt = ''
    if delay_allocate is False:
        # To verify: `# mount`
        dealloc_opt = '-o nodelalloc'
    ret = subprocess.call('sudo mount {} {} {}'.format(
        dealloc_opt, DEV_NAME, dir_name), shell=True)
    if ret != 0:
        logging.error(
            "dev:{} cannot mount to dir:{}".format(DEV_NAME, dir_name))
    else:
        logging.info("ext4 mounted")

    # set readahead
    if readahead_kb is not None:
        ret = subprocess.call(
            'blockdev --setra {} {}'.format(readahead_kb, DEV_NAME), shell=True)
        if ret == 0:
            logging.info("readahead set to {}KB".format(readahead_kb))

    # create bench data dir
    ret = subprocess.call(
        'sudo mkdir -p {}/{}'.format(dir_name, 'bench'), shell=True)
    if ret != 0:
        logging.error('cannot create bench dir')


def reset_ext4():
    subprocess.call('sudo umount {}'.format(
        cfs_common.get_kfs_mount_dir()), shell=True)
    logging.info("ext4 unmounted")

    # reset readahead
    ret = subprocess.call(
        'blockdev --setra {} {}'.format(128, DEV_NAME), shell=True)


def verify_dev_options():
    logging.info(' --- mount options ---')
    logging.info(' ==> readahead')
    result = subprocess.check_output(
        'blockdev --getra {}'.format(DEV_NAME),
        shell=True,
        stderr=subprocess.STDOUT)
    print(result.decode("utf-8"))
    logging.info(' ==> dealloc')
    result = subprocess.check_output(
        'mount | grep nvme',
        shell=True,
        stderr=subprocess.STDOUT)
    print(result.decode("utf-8"))


def get_benchmark_script(bench_code):
    bench_code_mappings = [
        (['RMPR'], 'bench_mt_randread.py {} cached'),
        (['RDPR'], 'bench_mt_randread.py {}'),
        (['RMSR'], 'bench_mt_randread.py {} cached share'),
        (['RDSR'], 'bench_mt_randread.py {} share'),
        (['RMPS'], 'bench_mt_seqread.py {} cached'),
        (['RDPS'], 'bench_mt_seqread.py {}'),
        (['RMSS'], 'bench_mt_seqread.py {} share cached'),
        (['RDSS'], 'bench_mt_seqread.py {} share'),
        (['WMPS'], 'bench_mt_write_noflush.py {}'),
        (['WMSS'], 'bench_mt_write_noflush.py {} share'),
        (['AMPS'], 'bench_mt_write_noflush.py {} append'),
        (['AMSS'], 'bench_mt_write_noflush.py {} append share'),
        (['WMPR'], 'bench_mt_randwrite.py {} cached'),
        (['WMSR'], 'bench_mt_randwrite.py {} cached share'),
        (['WDPS'], 'bench_mt_write_noflush.py {}'),
        (['WDPR'], 'bench_mt_randwrite.py {}'),
        (['WDSS'], 'bench_mt_write_noflush.py {} share'),
        (['WDSR'], 'bench_mt_randwrite.py {} share'),
        (['ADPS'], 'bench_mt_write_sync.py {} append'),
        (['ADSS'], 'bench_mt_write_sync.py {} append share'),
        (['S1MP'], 'bench_mt_stat.py {}'),
        (['S1MS'], 'bench_mt_stat.py {} share'),
        (['SaMP'], 'bench_mt_statall.py {} 1000'),
        (['SaMS'], 'bench_mt_statall.py {} 1000 share'),
        (['LsMP'], 'bench_mt_listdir.py {} 1000'),
        (['LsMS'], 'bench_mt_listdir.py {} 1000 share'),
        (['CMP'], 'bench_mt_mkdir.py {} create'),
        (['CMS'], 'bench_mt_mkdir.py {} create share'),
        (['UMP'], 'bench_mt_unlink.py {}'),
        (['UMS'], 'bench_mt_unlink.py {} share'),
        (['RMP'], 'bench_mt_rename.py {}'),
        (['RMS'], 'bench_mt_rename.py {} share'),
    ]
    for mapping in bench_code_mappings:
        if bench_code in mapping[0] and mapping[1] is not None:
            return mapping[1]
    return None


def get_default_benchmarks():
    benchmarks = [
        'RMPR', 'RMSR', 'RDPR', 'RDSR',
        'RMPS', 'RMSS', 'RDPS', 'RDSS',
        'WMPS', 'WMSS', 'AMPS', 'AMSS',
        'WMPR', 'WMSR',
        'WDPS', 'WDPR', 'WDSS', 'WDSR',
        'ADPS', 'ADSS',
        'S1MP', 'S1MS',
        'SaMP', 'SaMS',
        'LsMP', 'LsMS',
        'CMP', 'CMS',
        'UMP', 'UMS',
        'RMP', 'RMS',
    ]
    return benchmarks


def get_data_plane_benchmarks():
    benchmarks = [
        'RMPR', 'RMSR', 'RDPR', 'RDSR',
        'RMPS', 'RMSS', 'RDPS', 'RDSS',
        'WMPS', 'WMSS', 'AMPS', 'AMSS',
        'WMPR', 'WMSR',
        'WDPS', 'WDPR', 'WDSS', 'WDSR',
        'ADPS', 'ADSS',
    ]
    return benchmarks


def get_meta_benchmarks():
    jobs = get_default_benchmarks()
    dp_jobs = get_data_plane_benchmarks()
    for dj in dp_jobs:
        jobs.remove(dj)
    return jobs


def bench_needs_dataprep(bench_code):
    need_prep_list = [
        # all read
        'RMPR', 'RMSR', 'RDPR', 'RDSR', 'RMPS', 'RMSS', 'RDPS', 'RDSS',
        # overwrite
        'WMPS', 'WMSS', 'WMPR', 'WMSR', 'WDPS', 'WDPR', 'WDSS', 'WDSR'
    ]
    if bench_code in need_prep_list:
        return True
    return False


def bench_split_policy_no(bench_code):
    if bench_code in ['SaMS']:
        return 103
        #return 102
        #return 0
    else:
        return 0


def is_bench_stress_bg_flush(bench_code):
    if bench_code in ['WDPS', 'WDPR', 'WDSS', 'WDSR']:
        return True
    return False


def bench_fs_dirty_flush_ratio(bench_code):
    if is_bench_stress_bg_flush(bench_code):
        # invoke after 26 blocks dirty (1G buffer)
        return 0.00005
    else:
        return 0.9


def bench_kfs_bg_dirty_flush_bytes(bench_code):
    if is_bench_stress_bg_flush(bench_code):
        return int(1024 * 1024)
    else:
        return 0


def bench_kfs_bg_dirty_flush_ratio(bench_code):
    if is_bench_stress_bg_flush(bench_code):
        return 0
    return 10


def set_kfs_bg_dirty_flush(bg_byte, bg_ratio):
    logging.info(
        'set kernel fs bg_dirty flush byte-{} ratio-{}'.format(bg_byte, bg_ratio))
    if bg_byte != 0:
        cmd = 'echo {} > /proc/sys/vm/dirty_background_bytes'.format(
            bg_byte)
        ret = subprocess.call(cmd, shell=True)
        if ret != 0:
            sys.exit(1)
    if bg_ratio != 0:
        cmd = 'echo {} > /proc/sys/vm/dirty_background_ratio'.format(
            bg_ratio)
        ret = subprocess.call(cmd, shell=True)
        if ret != 0:
            sys.exit(1)


def reset_kfs_bg_dirty_flush():
    set_kfs_bg_dirty_flush(0, 10)


def is_fsp(fs_str):
    if 'fsp' in fs_str:
        return True
    else:
        return False


class Benchmark(object):
    def __init__(self, fs, benchmarks, num_app):
        assert(isinstance(benchmarks, list))
        self.fs = fs
        self.data_file_prepared = False
        self.benchmarks = benchmarks
        self.max_num_app = num_app
        self.repeat_num = 1

    def is_fsp(self):
        return 'fsp' in self.fs

    def prep_data_file(self):
        if not self.data_file_prepared:
            print(self.fs)
            if self.is_fsp():
                cfs_common.expr_mkfs()
            else:
                cfs_common.expr_mkfs_for_kfs()
            init_cmd = '{}/cfs_bench/exprs/init_mt_bench_file.py {} {}'. format(
                cfs_common.get_cfs_root_dir(), self.fs, self.max_num_app)
            print(init_cmd)
            ret = subprocess.call(init_cmd, shell=True)
            if ret == 0:
                self.data_file_prepared = True

    def run_single_bench(self, code):
        script_name = get_benchmark_script(code)
        if script_name is None:
            raise RuntimeError("benchmark:{} not supported".format(code))
        if bench_needs_dataprep(code):
            self.prep_data_file()
        logging.info('-------{}-------'.format(code))
        cmd = script_name.format(self.fs)
        cmd = '{}/cfs_bench/exprs/{} numapp={}'.format(
            cfs_common.get_cfs_root_dir(), cmd, self.max_num_app)
        if not self.is_fsp():  # if ext4, must add "mpstat" to collect CPU utilization info
            cmd = '{} {}'.format(cmd, 'mpstat')
        print('--- run benchmark:{} ---'.format(code))
        need_reset_kfs_bg_flush = False
        if not is_fsp(self.fs):
            if is_bench_stress_bg_flush(code):
                print('----- rest bg_flush is set to true')
                need_reset_kfs_bg_flush = True
                set_kfs_bg_dirty_flush(bench_kfs_bg_dirty_flush_bytes(code),
                                       bench_kfs_bg_dirty_flush_ratio(code))
        print(need_reset_kfs_bg_flush)

        for rpt in range(self.repeat_num):
            if is_fsp(self.fs):
                cfs_common.write_fsp_cfs_config_file(
                    split_policy=bench_split_policy_no(code),
                    dirtyFlushRatio=bench_fs_dirty_flush_ratio(code))
            ret = subprocess.call(cmd, shell=True)
            if ret != 0:
                sys.exit(1)
            self.save_expr_result(code, rpt)
            print('save result done')

        if need_reset_kfs_bg_flush:
            reset_kfs_bg_dirty_flush()

        logging.info('----------------')

    def save_expr_result(self, code, rept_no):
        save_dir = '{}_{}_run_{}'.format(self.fs, code, rept_no)
        if not os.path.exists(save_dir):
            os.mkdir(save_dir)
        os.system('mv log_{}_* {}'.format(self.fs, save_dir))

    def run(self):
        # divide the benchmarks into two, and one needs prep data, the other
        # does not
        need_data_prep_list = []
        no_data_prep_list = []
        for bench in self.benchmarks:
            if bench_needs_dataprep(bench):
                need_data_prep_list.append(bench)
            else:
                no_data_prep_list.append(bench)

        # run benchmarks
        for bench in need_data_prep_list:
            self.run_single_bench(bench)
        for bench in no_data_prep_list:
            self.run_single_bench(bench)


def main(args, loglevel):
    logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)
    logging.info('run microbenchmark-commit:{}'.format(get_commit_id()))
    if args.single:
        os.environ["CFS_BENCH_USE_SINGLE_WORKER"] = "true"
    else:
        os.environ["CFS_BENCH_USE_SINGLE_WORKER"] = "false"
    logging.info('Use single worker: {}'.format(cfs_common.use_single_worker()))
    if str(args.fs) == 'fsp':
        if not args.nomount:
            setup_spdk()
        if not args.devonly:
            if args.jobs is None:
                jobs = get_default_benchmarks()
            else:
                jobs = args.jobs.split(',')
                all_benchmarks = get_default_benchmarks()
                for j in jobs:
                    assert j in all_benchmarks
            logging.info(f"RUN jobs: {jobs}")
            b = Benchmark('fsp', jobs, args.numapp)
            b.run()
        if not args.nomount and not args.devonly:
            reset_spdk()
    elif args.fs == 'ext4' or args.fs == 'ext4nj':
        cur_has_journal = (args.fs == 'ext4')
        if not args.nomount:
            setup_ext4(
                has_journal=cur_has_journal,
                readahead_kb=args.ra,
                delay_allocate=(not args.nodalloc))
        verify_dev_options()
        if not args.devonly:
            if args.jobs is None:
                jobs = get_default_benchmarks()
            else:
                jobs = args.jobs.split(',')
                all_benchmarks = get_default_benchmarks()
                for j in jobs:
                    assert j in all_benchmarks
            logging.info(f"RUN jobs: {jobs}")
            b = Benchmark('ext4', jobs, args.numapp)
            b.run()
        if not args.nomount and not args.devonly:
            reset_ext4()
    else:
        logging.error("fs not supported")


def parse_cmd_args():
    # e.g.
    # ./fsp_microbench_suite.py --fs fsp
    parser = argparse.ArgumentParser(
        description="Run FSP microbenchmark")
    parser.add_argument('--fs', required=True,
                        help='file system type [fsp|ext4|ext4nj]')
    parser.add_argument('--ra', type=int, default=None,
                        help='readahead size in bytes')
    parser.add_argument('--nodalloc', action='store_true',
                        help='disable delayed allocation')
    parser.add_argument('--numapp', type=int, default=1,
                        help='number of applications (max)')
    parser.add_argument('--nomount', action='store_true',
                        help='will not mount device')
    parser.add_argument('--single', action='store_true',
                        help='use single worker')
    # e.g. --jobs RMPR,RMSR,RDPR
    parser.add_argument('--jobs', default=None,
                        help='specific benchmarks to run (split by comma)')
    parser.add_argument(
        '--devonly',
        action='store_true',
        help='will not run any benchmark, but mount the device')
    return(parser.parse_args())


if __name__ == '__main__':
    loglevel = logging.INFO
    args = parse_cmd_args()
    main(args, loglevel)
