#!/usr/bin/env python3
# encoding: utf-8

from sarge import run, Capture
import datetime
import sys
import psutil

import os
import time
import shutil


def compute_avg(l):
    if len(l) == 0:
        return 0
    return sum(l) / len(l)


def get_expr_user():
    # just for fun ..., in case elsewhere this function is used
    return 'simba'


def use_exact_num_app():
    if "USE_EXACT_NUM_APP" in os.environ:
        if os.environ["USE_EXACT_NUM_APP"].lower() == "true":
            return True
        elif os.environ["USE_EXACT_NUM_APP"].lower() == "false":
            return False
        else:
            raise RuntimeError('Invalid USE_EXACT_NUM_APP: Only accept "true" OR "false"')
    return False


def use_single_worker():
    use_single = os.environ.get('CFS_BENCH_USE_SINGLE_WORKER')
    if use_single is None:
        raise RuntimeError('CFS_BENCH_USE_SINGLE_WORKER not set')
    if use_single.lower() == "true":
        return True
    if use_single.lower() == "false":
        return False
    raise RuntimeError('Invalid CFS_BENCH_USE_SINGLE_WORKER: Only accept "true" OR "false"')


def get_year_str():
    now = datetime.datetime.now()
    return str(now.year)


def get_cfs_root_dir():
    cfs_root_dir = os.environ.get('CFS_ROOT_DIR')
    if cfs_root_dir is None:
        raise RuntimeError('CFS_ROOT_DIR not set')
    return cfs_root_dir


def get_kfs_mount_dir():
    kfs_mount_dir = os.environ.get('KFS_MOUNT_PATH')
    if kfs_mount_dir is None:
        raise RuntimeError('KFS_MOUNT_PATH not set')
    return kfs_mount_dir


def get_kfs_data_dir():
    """
    :return: the data directory for a kernel file system to R/W
    """
    kfs_data_dir = os.environ.get('KFS_DATA_DIR')
    if kfs_data_dir is None:
        raise RuntimeError('KFS_DATA_DIR not set')
    return kfs_data_dir


def get_kfs_dev_name():
    kfs_dev_name = os.environ.get('SSD_NAME')
    if kfs_dev_name is None:
        raise RuntimeError('SSD_NAME not set')
    return kfs_dev_name


def get_cfs_main_binname():
    cfs_main_binname = os.environ.get('CFS_MAIN_BIN_NAME')
    if cfs_main_binname is None:
        raise RuntimeError('CFS_MAIN_BIN_NAME not set')
    return cfs_main_binname


def get_cfs_mkfs_binname():
    cfs_mkfs_binname = os.environ.get('CFS_MKFS_BIN_NAME')
    if cfs_mkfs_binname is None:
        raise RuntimeError('CFS_MKFS_BIN_NAME not set')
    return cfs_mkfs_binname


def get_ts_dir_name():
    s = '{date:%Y-%m-%d-%H-%M-%S}'.format(date=datetime.datetime.now())
    return s


def check_root():
    cuid = os.geteuid()
    return cuid == 0


def clear_page_cache(is_slient=True):
    os.system('sudo sh -c "sync; echo 1 > /proc/sys/vm/drop_caches"')
    os.system('sudo sh -c "sync; echo 2 > /proc/sys/vm/drop_caches"')
    os.system('sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"')
    if is_slient is False:
        print('page cache cleared')


def dump_kernel_dirty_flush_config(dirname):
    os.system(
        "cat /proc/sys/vm/dirty_background_bytes > {}/dirty_flush_bytes".format(dirname))
    os.system(
        "cat /proc/sys/vm/dirty_background_ratio > {}/dirty_flush_ratio".format(dirname))


def check_if_process_running(process_name):
    """
    Check if there is any running process that contains the  process_name
    """
    # Iterate over the all the running process
    for proc in psutil.process_iter():
        try:
            # Check if process name contains the given name string.
            if process_name.lower() in proc.name().lower():
                return True
        except (
                psutil.NoSuchProcess, psutil.AccessDenied,
                psutil.ZombieProcess):
            pass
    return False


def save_mt_fsp_worker_logs_to_dir(src, dest):
    try:
        shutil.copytree(src, dest)
    # Directories are the same
    except shutil.Error as e:
        print('Directory not copied. Error: %s' % e)
    # Any error saying that the directory doesn't exist
    except OSError as e:
        print('Directory not copied. Error: %s' % e)


def get_proj_log_dir(user=None, suffix=None, do_mkdir=True):
    rs = ''
    if suffix is not None:
        rs = suffix
    log_dir = '{}/cfs_bench/exprs/log{}/'.format(get_cfs_root_dir(), rs)
    if do_mkdir:
        mk_accessible_dir(log_dir)
    return log_dir


def get_fsmain_bin(is_posix_block_dev=False):
    # bin_name = '{}/{}{}'.format(get_cfs_root_dir(), 'cfs/build/fsMain',
    #                            'Posix' if is_posix_block_dev else ''i)
    bin_name = get_cfs_main_binname()
    assert (os.path.exists(bin_name))
    return bin_name


def get_fsmkfs_bin():
    bin_name = get_cfs_mkfs_binname()
    assert (os.path.exists(bin_name))
    return bin_name


def get_div_str(case_str):
    case_str_len = len(case_str)
    half_add_len = int(10 - case_str_len / 2)
    return '=' * half_add_len + case_str + '=' * half_add_len


def get_default_bench_args(bench_name):
    args = {
        "--benchmarks=": bench_name,
        "--numop=": 100000,
        "--value_size=": 1024,
    }
    return args


def write_file(log_name, txt):
    if os.path.exists(log_name):
        write_mode = 'a'
    else:
        write_mode = 'w'
    with open(log_name, write_mode, encoding="utf-8") as f:
        f.write(txt)
        f.flush()


def mk_accessible_dir(dir_name):
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    os.chmod(dir_name, 0o777)


def dump_expr_config(cfg_name, cfg_dict):
    with open(cfg_name, 'w+') as f:
        for k, v in cfg_dict.items():
            f.write("%s,%s\n" % (k, v))
        f.flush()


def expr_mkfs():
    is_root = check_root()
    if is_root:
        print(get_div_str('mkfs'))
        # mkfscmd = '{} mkfs -z 100000'.format(get_fsmkfs_bin())
        mkfscmd = '{} mkfs'.format(get_fsmkfs_bin())
        mkfsproc = run(mkfscmd, stdout=Capture())
        print(mkfsproc.stdout.text)
        print(get_div_str(''))
    else:
        print('must be run as root')


def expr_mkfs_for_kfs():
    print(get_div_str('mkfs for kernel fs'))
    os.system('rm -rf {}/*'.format(get_kfs_data_dir()))
    print(get_div_str('kernel fs benchmark directory content:'))
    print(os.listdir(get_kfs_data_dir()))


def fsp_do_clean_sock():
    os.system('rm -f /ufs-*')
    print('sock file cleaned')

def fsp_do_offline_checkpoint():
    bin_name = get_offline_checkpointer_bin()
    if os.path.exists(bin_name):
        print(get_div_str('do-checkpoint'))
        ckptproc = run(bin_name)
        print(get_div_str(''))


def get_default_cfs_config_name():
    return '/tmp/fsp.conf'


def save_default_cfg_config(dst_dir):
    cfg_name = get_default_cfs_config_name()
    if os.path.exists(cfg_name) and os.path.isdir(dst_dir):
        os.system('mv {} {}'.format(cfg_name, dst_dir))

# write the config that is used for FSP


def write_fsp_cfs_config_file(
        config_name=None,
        split_policy=0,
        dirtyFlushRatio=0.9,
        raNumBlock=16,
        serverCorePolicy=0,
        cgst_ql=0,
        percore_ut=0):
    print(get_div_str('fsp-cfg'))
    print(get_div_str(''))
    if config_name is None:
        config_name = get_default_cfs_config_name()
    tpl_str = "# Generated config\n" \
              "# config for using FSP\n" \
              "splitPolicyNum = \"{}\";\n" \
              "serverCorePolicyNo = \"{}\";\n" \
              "lb_cgst_ql = \"{}\";\n" \
              "nc_percore_ut = \"{}\";\n" \
              "dirtyFlushRatio = \"{}\";\n" \
        "raNumBlock = \"{}\";\n".format(
            split_policy, serverCorePolicy, cgst_ql, percore_ut, dirtyFlushRatio, raNumBlock)
    with open(config_name, 'w') as f:
        f.write(tpl_str)
    print(get_div_str(''))


def get_default_spdk_config_fname():
    return '/tmp/spdk.conf'

# write the config that is used for SPDK


def write_fsp_dev_config_file(config_name=None, core_mask='0x2'):
    print(get_div_str('dev-cfg'))
    if config_name is None:
        config_name = get_default_spdk_config_fname()
    if os.path.exists(config_name):
        os.remove(config_name)
    # by default, core mask is 0x1
    tpl_str = "# Generated config\n" \
        "# config for using spdk\n" \
        "dev_name = \"spdkSSD\";\n" \
              "core_mask = \"{}\";\n" \
              "shm_id = \"9\";".format(core_mask)
    print(tpl_str)
    with open(config_name, 'w') as f:
        f.write(tpl_str)
    print(get_div_str(''))


def gen_expr_mtfsp_shm_offset(n_fsp_worker, n_app_proc):
    kMAX_APP_NUM = 10
    kMAX_FSP_WORKER = 10
    # kMAX_FSP_WORKER = 6

    assert (n_fsp_worker <= kMAX_FSP_WORKER)
    assert (n_app_proc <= kMAX_APP_NUM)

    fsp_wk_shmkeys = [(1 + x * kMAX_APP_NUM) for x in range(kMAX_FSP_WORKER)]
    rt_fsp_wk_shmkey_str = ','.join(
        [str(fsp_wk_shmkeys[i]) for i in range(n_fsp_worker)])

    appid_keystr_dict = {}
    for appid in range(n_app_proc):
        cur_app_shmkey_str = ','.join(
            [str(fsp_wk_shmkeys[i] + appid) for i in range(n_fsp_worker)])
        appid_keystr_dict[appid] = cur_app_shmkey_str
    return rt_fsp_wk_shmkey_str, appid_keystr_dict


def start_bench_coordinator(num_app_proc=1):
    # NOTE: starting a coordinator to coordinate the running of
    # cfs_bench clients.
    # Ideally, we should have a function that accepts the command to
    # lauch the fsp + all clients. That function should also call
    # a coordinator. However, since right now we spawn all cfs_bench
    # clients in their own experiment functions, we will start the
    # coordinator here and hope everything works well.
    # When there is a failure, make sure all coordinators have
    # been deleted and the shm file is closed.
    if os.path.exists("/dev/shm/coordinator"):
        print("/dev/shm/coordinator exists, maybe a coordinator is running?")
        print("Run ps -ef | grep cfs_bench_coordinator and kill any existing")
        print("Delete the /dev/shm/coordinator")
        raise Exception("Don't trust this benchmark. Re-run again.")

    coord_bin = get_coordinator_bin()
    coord_cmd = '{} -n {}'.format(coord_bin, num_app_proc)
    run(coord_cmd, async_=True, stdout=Capture())


def start_fs_proc(
        num_workers=1,
        num_app_proc=1,
        shm_offset=1,
        exit_signal_file_name=None,
        config_name=get_default_spdk_config_fname(),
        ready_fname=None,
        worker_cores=None,
        bypass_exit_sync=False):
    fs_main_bin = get_fsmain_bin()
    if check_if_process_running(fs_main_bin.split('/')[-1]):
        print('ERROR. FSP ({}) is already running. EXIT ...'.format(
            fs_main_bin.split('/')[-1]))
        sys.exit(1)
    if not os.path.exists(fs_main_bin):
        raise RuntimeError('{} not exists'.format(fs_main_bin))
    if exit_signal_file_name is None:
        exit_signal_file_name = '/tmp/cfs_exit_{}'.format(get_ts_dir_name())
    if ready_fname is None:
        ready_fname = '/tmp/cfs_ready'
    if os.path.exists(ready_fname):
        os.remove(ready_fname)

    if not os.path.exists(config_name):
        write_fsp_dev_config_file(config_name)

    if os.path.exists(get_default_cfs_config_name()):
        fsp_config_name = get_default_cfs_config_name()
    else:
        fsp_config_name = ''

    if num_workers <= 10:
        core_ids = [str(i + 1) for i in range(num_workers)]
    else:
        core_ids_1 = [str(i + 1) for i in range(10)]
        core_ids_2 = [str(41 - i) for i in range(0, num_workers - 10)]
        core_ids = core_ids_1 + core_ids_2
    fs_cmd = '{} {} {} {} {} {} {} {}'.format(
        fs_main_bin,
        num_workers,
        num_app_proc,
        shm_offset,
        exit_signal_file_name,
        config_name,
        ','.join(core_ids),
        fsp_config_name,)

    if worker_cores is not None:
        assert isinstance(worker_cores, list)
        assert len(worker_cores) == num_workers
        worker_cores_str = ','.join(map(str, worker_cores))
        fs_cmd += ' {}'.format(worker_cores_str)

    #fs_cmd = '{} {}'.format('numactl --cpunodebind=0 --membind=0', fs_cmd)

    print(get_div_str('start fs with ready_fname:{}'.format(ready_fname)))
    print(fs_cmd)
    env = {**os.environ, "READY_FILE_NAME": f"{ready_fname}"}
    if bypass_exit_sync:
        env.update({"FSP_BYPASS_SHUTDOWN_NOSYNC": "YES"})
    p_fs = None
    p_fs = run(fs_cmd, stdout=Capture(), async_=True, env=env)
    # wait until initialization finished
    while not os.path.exists(ready_fname):
        # print(get_div_str('wait for ready_file'))
        time.sleep(1e-8)
    print(get_div_str('fs started'))

    start_bench_coordinator(num_app_proc=num_app_proc)

    return p_fs, exit_signal_file_name, ready_fname, fs_cmd


def start_mtfsp(
        num_fsp_worker,
        num_app_proc,
        ready_fname=None,
        bypass_exit_sync=False):
    fsp_shmkeys, app_key_dict = gen_expr_mtfsp_shm_offset(num_fsp_worker,
                                                          num_app_proc)
    print(fsp_shmkeys)
    print(app_key_dict)
    fsp_do_clean_sock()
    # we run checkpoint first
    fsp_do_offline_checkpoint()
    # start fsp
    p_fs, exit_signal_file_name, real_ready_fname, fs_cmd = start_fs_proc(
        num_fsp_worker,
        num_app_proc,
        shm_offset=fsp_shmkeys, ready_fname=ready_fname, bypass_exit_sync=bypass_exit_sync)
    return app_key_dict, p_fs, exit_signal_file_name, real_ready_fname, fs_cmd


def shutdown_fs(exit_signal_file_name, p_fs):
    with open(exit_signal_file_name, 'w+') as f:
        f.write('Apparate')
        f.flush()
        f.close()
    p_fs.wait()


def get_microbench_bin(is_fsp=True):
    if is_fsp:
        cfs_bin = '{}/{}'.format(get_cfs_root_dir(),
                                 'cfs_bench/build/bins/cfs_bench')
    else:
        cfs_bin = '{}/{}'.format(get_cfs_root_dir(),
                                 'cfs_bench/build/bins/cfs_bench_posix')
    return cfs_bin


def get_coordinator_bin():
    return '{}/{}'.format(get_cfs_root_dir(),
                          'cfs_bench/build/bins/cfs_bench_coordinator')


def get_offline_checkpointer_bin():
    return '{}/{}'.format(get_cfs_root_dir(),
                          'cfs/build/test/fsproc/fsProcOfflineCheckpointer')


def compare_two_crc_log(fw_name, fr_name):
    print(get_div_str('compare crc'))
    fw = open(fw_name)
    fr = open(fr_name)
    fwlines = fw.readlines()
    frlines = fr.readlines()
    fw_idx = 0
    fr_idx = 0
    fw_started = False
    fr_started = False
    while True:
        if fw_idx >= len(fwlines) or fr_idx >= len(frlines):
            break
        if not fw_started and 'CRC32 START' not in fwlines[fw_idx]:
            fw_idx += 1
        if 'CRC32 START' in fwlines[fw_idx] and not fw_started:
            fw_started = True
        if not fr_started and 'CRC32 START' not in frlines[fr_idx]:
            fr_idx += 1
        if 'CRC32 START' in frlines[fr_idx] and not fr_started:
            fr_started = True
        if fr_started and fw_started:
            if fwlines[fw_idx].strip() != frlines[fr_idx].strip():
                print(
                    'ERROR cannot match line:{} line:{}'.format(
                        fw_idx, fr_idx))
                return
            else:
                fw_idx += 1
                fr_idx += 1
    print('CRC matches')


def print_env_variables():
    env_var_list = ['USER', 'CFS_ROOT_DIR']
    for ev in env_var_list:
        print('${} is set to: {}'.format(ev, os.environ.get(ev)))


if __name__ == '__main__':
    print_env_variables()
    pass
