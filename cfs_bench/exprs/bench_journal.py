#!/usr/bin/env python

import argparse
import datetime
import json
import os
import subprocess
import traceback
import time

from pathlib import Path

MAX_WORKERS = MAX_APPS = 10
# TODO read CFS_* environ variables
# REPO = Path("/home/arebello/workspace/ApparateFS/")
SPDK_CONF = Path("/home/arebello/.config/fsp/spdk.conf")
FSP_CONF = Path("/home/arebello/.config/fsp/fsp.conf")
READY_FILE = Path("/tmp/readyFile")
EXIT_FILE = Path("/tmp/cfs_exit")

# Use the dpdk cpu_layout.py functin to figure out the cores you want to use
# for oats
# socket 0 : 0 - 19
# socket 1 : 20 - 39
# We use values for cores starting from 1, so adding 1 to them..
CPU_TOPO = [ list(range(1, 21)), list(range(21, 41)) ]
DEFAULT_SOCKET_IDX = 0
FSP_CORES = CPU_TOPO[DEFAULT_SOCKET_IDX][:MAX_WORKERS]
print(f"FSP_CORES={FSP_CORES}")
APP_CORES = CPU_TOPO[DEFAULT_SOCKET_IDX][MAX_WORKERS:MAX_WORKERS + MAX_APPS]
print(f"APP_CORES={APP_CORES}")
assert len(FSP_CORES) == MAX_WORKERS
assert len(APP_CORES) == MAX_APPS

def get_shm_offsets(num_workers, num_apps):
    assert num_workers <= MAX_WORKERS
    assert num_apps <= MAX_APPS
    return [(1 + i * MAX_APPS ) for i in range(num_workers)]

def get_client_worker_key_list(wid, num_workers, num_apps):
    return [(wid + i) for i in get_shm_offsets(num_workers, num_apps) ]

def get_fsmain_cmd(repo, num_workers, num_apps):
    fsMain = repo / "cfs/build/fsMain"
    assert(fsMain.exists())
    cmd = [
        fsMain, num_workers, num_apps,
        ','.join(map(str, get_shm_offsets(num_workers, num_apps))),
        EXIT_FILE,
        SPDK_CONF,
        ",".join(map(str, FSP_CORES[:num_workers])),
        FSP_CONF,
    ]
    return cmd

def get_mkfs_cmd(repo):
    cmd = [
        repo / "cfs/build/test/fsproc/testRWFsUtil",
        "mkfs",
    ]
    return cmd

def get_client_cmd(repo, wid, num_workers, num_apps, **kwargs):
    # NOTE: Only works for --threads=1
    cmd = [
        repo / "cfs_bench/build/bins/cfs_bench",
        "--threads=1", "--histogram=1", "--benchmarks=seqwrite",
    ]

    worker_key_list = get_client_worker_key_list(wid, num_workers, num_apps)
    worker_key_list = ','.join(map(str, worker_key_list))
    fname = f"bench_f_{wid + 1}"
    core_id = APP_CORES[wid]

    cmd.extend([
        f"--fs_worker_key_list={worker_key_list}",
        f"--fname={fname}",
        f"--core_ids={core_id}",
        f"--wid={wid % num_workers}",
    ])
    cmd.extend([f"--{k}={v}" for k,v in kwargs.items()])
    return cmd

def get_coordinator_cmd(repo, num_workers, num_apps):
    candidates = FSP_CORES[num_workers:] + APP_CORES[num_apps:]
    if len(candidates) == 0:
        # choose from other socket?
        candidates = CPU_TOPO[DEFAULT_SOCKET_IDX + 1]

    core = candidates[0]
    return [ repo / "cfs_bench/build/bins/cfs_bench_coordinator", "-n", num_apps, "-c", core]

class Experiment(object):
    TIMEOUT = 180 # 3 minutes
    def __init__(self, outdir, num_workers, num_apps, repo=None, **apps_kwargs):
        if repo is None:
            # TODO take repo from CFS_ROOT_DIR?
            assert False, "repo cannot be None for now..."

        self.repo = Path(repo)
        assert self.repo.exists()

        self.outdir = Path(outdir)
        self.outdir.mkdir(parents=True, exist_ok=True)

        self.root = self.outdir / f'fsp_{num_workers}_apps_{num_apps}'
        if self.root.exists():
            # FIXME remove this
            raise Exception("Skipping this.. already exists")
            # rename existing one with a timestamp
            ts = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
            dst = self.outdir / f'{self.root.name}__{ts}'
            self.root.rename(dst)

        self.root.mkdir()
        self.num_workers = num_workers
        self.num_apps = num_apps
        self.apps_kwargs = apps_kwargs

        self.fsp_stdout = None
        self.fsp_stderr = None
        self.fsp_command = None
        self.fsp_process = None

        self.co_stdout = None
        self.co_stderr = None
        self.co_command = None
        self.co_process = None

        self.app_stdout_list = [None] * self.num_apps
        self.app_stderr_list = [None] * self.num_apps
        self.app_command_list = [None] * self.num_apps
        self.app_process_list = [None] * self.num_apps

    def prepare(self):
        cmd = get_mkfs_cmd(self.repo)
        assert cmd[0].exists()
        scmd = [str(i) for i in cmd]
        print("running mkfs")
        with open(self.root / "mkfs", "wb") as fp:
            subprocess.check_call(scmd, stdout=fp, stderr=fp)

        subprocess.check_call("rm -rf /dev/shm/*", shell=True)
        subprocess.check_call("ipcrm --all", shell=True)

    def start_fsp(self):
        self.fsp_stdout = open(self.root / "fsp_stdout", "wb")
        self.fsp_stderr = open(self.root / "fsp_stderr", "wb")

        cmd = get_fsmain_cmd(self.repo, self.num_workers, self.num_apps)
        assert cmd[0].exists()
        scmd = [str(i) for i in cmd]

        EXIT_FILE.unlink(missing_ok=True)
        READY_FILE.unlink(missing_ok=True)

        env = {**os.environ, "READY_FILE_NAME": f"{READY_FILE}"}
        self.fsp_command = ' '.join(scmd)
        print(self.fsp_command)
        self.fsp_process = subprocess.Popen(scmd, stdout=self.fsp_stdout, stderr=self.fsp_stderr, env=env)

    def start_coordinator(self):
        self.co_stdout = open(self.root / "co_stdout", "wb")
        self.co_stderr = open(self.root / "co_stderr", "wb")

        cmd = get_coordinator_cmd(self.repo, self.num_workers, self.num_apps)
        assert cmd[0].exists()
        scmd = [str(i) for i in cmd]

        self.co_command = ' '.join(scmd)
        print(self.co_command)
        self.co_process = subprocess.Popen(scmd, stdout=self.co_stdout, stderr=self.co_stderr)

    def start_app(self, app_idx):
        app_stdout = open(self.root / f"app{app_idx}_stdout", "wb")
        app_stderr = open(self.root / f"app{app_idx}_stderr", "wb")

        cmd = get_client_cmd(self.repo, app_idx, self.num_workers, self.num_apps, **self.apps_kwargs)
        assert cmd[0].exists()
        scmd = [str(i) for i in cmd]
        app_command = ' '.join(scmd)
        print(app_command)
        app_process = subprocess.Popen(scmd, stdout=app_stdout, stderr=app_stderr)

        self.app_stdout_list[app_idx] = app_stdout
        self.app_stderr_list[app_idx] = app_stderr
        self.app_command_list[app_idx] = app_command
        self.app_process_list[app_idx] = app_process

    def wait_for_fsp_ready(self):
        print("waiting for ready file")
        while not READY_FILE.exists():
            time.sleep(1)

    def stop_fsp(self):
        with open(EXIT_FILE, "w") as fp:
            pass

    def wait_for_apps(self):
        for i, app in enumerate(self.app_process_list):
            print(f"Waiting for app {i}")
            if app is not None:
                app.wait(timeout=self.TIMEOUT)
                print(f"App {i} returned: {app.returncode}")

    def kill_all(self):
        print("killing all processes")
        procs = [
            self.fsp_process, self.co_process
        ]
        procs.extend(self.app_process_list)

        for proc in procs:
            if proc is not None:
                proc.kill()

    def poll_till_any_completes(self):
        procs = [self.fsp_process, self.co_process]
        procs.extend(self.app_process_list)
        while True:
            time.sleep(10)
            for proc in procs:
                if proc is None:
                    continue

                if proc.poll() is not None:
                    # something finished
                    print(f"Completed first: {proc.args}")
                    return

    def _run(self):
        print(f"Running experiment: {self.outdir} {self.num_workers} workers, {self.num_apps} apps")
        print(f"Repo: {self.repo}")
        self.prepare()

        self.start_fsp()
        self.wait_for_fsp_ready()
        self.start_coordinator()
        time.sleep(1)
        for i in range(self.num_apps):
            self.start_app(i)

        self.poll_till_any_completes()

        # the rest should finish within some timeout or something is stuck...
        self.wait_for_apps()
        print("Waiting for coordinator")
        self.co_process.wait(timeout=self.TIMEOUT)
        print(f"Coordinator returned: {self.co_process.returncode}")
        self.stop_fsp()
        print(f"Waiting for fsp to stop")
        self.fsp_process.wait(timeout=self.TIMEOUT)
        print(f"Fsp returned: {self.fsp_process.returncode}")

        self.summarize()
        self.cleanup()
        print("Finished experiment")

    def run(self):
        try:
            self._run()
        except:
            traceback.print_exc()
            self.kill_all()
            raise

    def summarize(self):
        summary = {
            "num_workers": self.num_workers,
            "num_apps": self.num_apps,
            "fsp_command": self.fsp_command,
            "co_command": self.co_command,
            "app_commands": self.app_command_list,
            "returncodes": {
                "fsp": self.fsp_process.returncode,
                "co": self.co_process.returncode,
                "apps": [i.returncode for i in self.app_process_list],
            },
        }
        with open(self.root / "summary.json", "w") as fp:
            json.dump(summary, fp, indent=2)

    def cleanup(self):
        fds = [self.fsp_stdout, self.fsp_stderr, self.co_stdout, self.co_stderr]
        fds.extend(self.app_stdout_list)
        fds.extend(self.app_stderr_list)
        for fd in fds:
            if fd is not None:
                fd.close()

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--workers", default=1, type=int)
    parser.add_argument("-a", "--apps", default=1, type=int)
    parser.add_argument("-d", "--outdir", default=os.getcwd())
    parser.add_argument("--value-size", default=64, type=int)
    parser.add_argument("--num-ops", default=10, type=int)
    parser.add_argument("--sync-after-n-writes", default=1, type=int)
    parser.add_argument("--repo", default="/home/arebello/workspace/ApparateFS/")
    return parser.parse_args()

if __name__ == '__main__':
    args = get_args()
    app_args = {
        "value_size": args.value_size,
        "numop": args.num_ops,
        "sync_numop": args.sync_after_n_writes,
    }

    e = Experiment(args.outdir, args.workers, args.apps, repo=args.repo, **app_args)
    e.run()
