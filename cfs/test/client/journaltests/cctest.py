#!/usr/bin/env python

"""
Usage:

    python cctest.py --repo /home/anthony/workspace/ApparateFS -n 4 --prepare
    python cctest.py --repo /home/anthony/workspace/ApparateFS --prepare-output-dir prepare --create-crash-states
    python cctest.py --repo /home/anthony/workspace/ApparateFS -n 4 --run-crash-state --crash-state-dir crash_states/crash_state_000
"""
import argparse
import datetime
import json
import os
import subprocess
import traceback
import time

from pathlib import Path

# TODO read CFS_* environ variables
# REPO = Path("/home/arebello/workspace/ApparateFS/")
SPDK_CONF = Path("/home/arebello/.config/fsp/spdk.conf")
FSP_CONF = Path("/home/arebello/.config/fsp/fsp.conf")
READY_FILE = Path("/tmp/readyFile")
EXIT_FILE = Path("/tmp/cfs_exit")

def _get_shm_offsets(num_workers):
    pattern = [1, 11, 22, 33, 44, 55, 66, 77, 88, 99]
    return pattern[:num_workers]

def get_fsmain_cmd(repo, num_workers, num_apps):
    fsMain = repo / "cfs/build/fsMain"
    assert(fsMain.exists())
    cmd = [
        fsMain, num_workers, num_apps,
        ','.join(map(str, _get_shm_offsets(num_workers))),
        EXIT_FILE,
        SPDK_CONF,
        ",".join(map(str, range(1, num_workers + 1))),
        FSP_CONF,
    ]
    return cmd

def get_mkfs_cmd(repo):
    cmd = [
        repo / "cfs/build/test/fsproc/testRWFsUtil",
        "mkfs",
    ]
    return cmd

def get_workload_cmd_for_worker(repo, i, num_workers):
    cmd = [
        repo / "cfs/build/test/client/journaltests/cctest_workload",
        "-a", str(i),
        "-n", str(num_workers),
        "-w", str(i),
    ]
    return cmd

def get_postworkload_cmd(repo, num_workers):
    cmd = [
        repo / "cfs/build/test/client/journaltests/cctest_postworkload",
        "-n", str(num_workers),
    ]
    return cmd

def get_offline_chkpt_cmd(repo, export=False, import_dir=None):
    cmd = [
        repo / "cfs/build/test/fsproc/fsProcOfflineCheckpointer"
    ]
    if export:
        cmd.append("-x")

    if import_dir is not None:
        assert isinstance(import_dir, Path)
        assert import_dir.exists() and import_dir.is_dir()
        cmd.extend([
            "-m", str(import_dir.absolute()),
        ])

    return cmd


class Experiment(object):
    def __init__(self, repo):
        self.repo = Path(repo)
        assert self.repo.exists()

        self.root = None # set by callers

        self.fsp_stdout = None
        self.fsp_stderr = None
        self.fsp_command = None
        self.fsp_process = None

    def initial_prep(self):
        cmd = get_mkfs_cmd(self.repo)
        assert cmd[0].exists()
        scmd = [str(i) for i in cmd]
        print("running mkfs")
        with open(self.root / "mkfs", "wb") as fp:
            subprocess.check_call(scmd, stdout=fp, stderr=fp)

        subprocess.check_call("rm -rf /dev/shm/*", shell=True)
        subprocess.check_call("ipcrm --all", shell=True)

    def start_fsp(self, num_workers, num_apps):
        self.fsp_stdout = open(self.root / "fsp_stdout", "wb")
        self.fsp_stderr = open(self.root / "fsp_stderr", "wb")

        cmd = get_fsmain_cmd(self.repo, num_workers, num_apps)
        assert cmd[0].exists()
        scmd = [str(i) for i in cmd]

        EXIT_FILE.unlink(missing_ok=True)
        READY_FILE.unlink(missing_ok=True)

        env = {**os.environ, "READY_FILE_NAME": f"{READY_FILE}"}
        self.fsp_command = ' '.join(scmd)
        print(self.fsp_command)
        self.fsp_process = subprocess.Popen(scmd, stdout=self.fsp_stdout, stderr=self.fsp_stderr, env=env, cwd=self.root)

    def wait_for_fsp_ready(self):
        print("waiting for ready file")
        while not READY_FILE.exists():
            time.sleep(1)

    def stop_fsp(self):
        if self.fsp_process is None:
            return

        with open(EXIT_FILE, "w") as fp:
            pass

        print("waiting for fsp to shutdown")
        self.fsp_process.wait(timeout=180) # 30 seconds wait
        assert self.fsp_process.returncode == 0
        self.fsp_process = None

    def run_workloads(self, args):
        scmds = []
        stdout_fps = []
        stderr_fps = []

        print("running workloads")
        for i in range(args.num_files):
            cmd = get_workload_cmd_for_worker(self.repo, i, args.num_files)
            assert cmd[0].exists()
            scmd = [str(i) for i in cmd]
            scmds.append(scmd)

            stdout_file = self.root / f"app{i}_stdout"
            stderr_file = self.root / f"app{i}_stderr"
            stdout_fps.append(open(stdout_file, "wb"))
            stderr_fps.append(open(stderr_file, "wb"))

        procs = []
        for i, cmd in enumerate(scmds):
            proc = subprocess.Popen(cmd, stdout=stdout_fps[i], stderr=stderr_fps[i], cwd=self.root)
            procs.append(proc)

        for i, proc in enumerate(procs):
            proc.wait(timeout=10) # 10 seconds
            assert proc.returncode == 0
            stdout_fps[i].close()
            stderr_fps[i].close()

    def prepare(self, args):
        self.initial_prep()
        self.start_fsp(args.num_files, args.num_files)
        self.wait_for_fsp_ready()
        self.run_workloads(args)
        self.stop_fsp()
        self.run_fsp_checkpointer(export=True)
        self.start_fsp(1, 1)
        self.wait_for_fsp_ready()
        self.run_postworkload(args.num_files)
        self.stop_fsp()

    def run_fsp_checkpointer(self, **cmd_args):
        cmd = get_offline_chkpt_cmd(self.repo, **cmd_args)
        assert cmd[0].exists()
        scmd = list(map(str, cmd))
        stdout = open(self.root / "chkpt_stdout", "wb")
        stderr = open(self.root / "chkpt_stderr", "wb")
        print("running checkpointer: ", scmd)
        subprocess.check_call(scmd, stdout=stdout, stderr=stderr, cwd=self.root)

    def run_postworkload(self, num_files):
        cmd = get_postworkload_cmd(self.repo, num_files)
        assert cmd[0].exists()
        scmd = list(map(str, cmd))
        stdout = open(self.root / "postworkload_stdout", "wb")
        stderr = open(self.root / "postworkload_stderr", "wb")
        print("running postworkload: ", scmd)
        subprocess.check_call(scmd, stdout=stdout, stderr=stderr, cwd=self.root)

    def run_crash_state(self, import_dir, num_files):
        assert import_dir.exists()
        self.run_fsp_checkpointer(export=True, import_dir=import_dir)
        self.start_fsp(1,1)
        self.wait_for_fsp_ready()
        self.run_postworkload(num_files)
        self.stop_fsp()

    def create_crash_states(self, prepare_dir):
        assert prepare_dir is not None
        prepare_dir = Path(prepare_dir)
        assert prepare_dir.exists()
        jentries = []

        # yes, I know, inefficient, but I'm running out of dev time
        def _copy_all(src_dir, dst_dir):
            dst_dir.mkdir(parents=True, exist_ok=True)
            subprocess.check_call(f"cp -r {src_dir}/* {dst_dir}/", shell=True)

        def _remove_jentry(src_dir, jentry):
            assert (src_dir / f'jentry_{jentry}').exists()
            assert (src_dir / f'jentry_{jentry}_commit').exists()
            # only remove the journal commit..not the journal entry
            # subprocess.check_call(f"mv {src_dir}/jentry_{jentry} {src_dir}/jentry_{jentry}_removed", shell=True)
            subprocess.check_call(f"mv {src_dir}/jentry_{jentry}_commit {src_dir}/jentry_{jentry}_commit_removed", shell=True)

        def _get_idx(f):
            with open(f) as fp:
                return json.load(fp)["idx"]

        def _get_inodes(f):
            with open(f) as fp:
                return set(json.load(fp)["inodes_in_entry"])

        jentries = list(map(_get_idx, prepare_dir.glob("chkpt_export/original/jentry_*_commit")))
        jentries.sort()

        jentry_to_inodes = {}
        inode_to_jentries = {}
        for jentry in jentries:
            fname = prepare_dir / f"chkpt_export/original/jentry_{jentry}_commit"
            inodes = _get_inodes(fname)
            jentry_to_inodes[jentry] = inodes
            for inode in inodes:
                inode_to_jentries.setdefault(inode, []).append(jentry)

        # failing one by one
        for i, to_fail_jidx in enumerate(jentries):
            crash_dir = self.root / f"crash_state_{i:03}"
            crash_dir.mkdir(parents=True, exist_ok=True)
            _copy_all(prepare_dir / "chkpt_export/original", crash_dir / "input")

            # remove the one that we intend to fail and anything else after that that has the same inodes
            inodes = jentry_to_inodes[to_fail_jidx]
            all_jentries_for_inodes = set()
            for inode in inodes:
                all_jentries_for_inodes.update(inode_to_jentries[inode])

            all_jentries_related = sorted(all_jentries_for_inodes)
            remove_from = all_jentries_related.index(to_fail_jidx)
            jentries_to_remove = all_jentries_related[remove_from:]
            for jentry in jentries_to_remove:
                _remove_jentry(crash_dir / "input", jentry)

            desc = { "to_fail": to_fail_jidx, "removing": jentries_to_remove }
            with open(crash_dir / "input/description", "w") as fp:
                json.dump(desc, fp, indent=2)

def get_args():
    # TODO better structure for args / subcommands
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("-n", "--num-files", default=1, type=int)
    parser.add_argument("--original-export-dir", default=None)
    parser.add_argument("--crash-state-dir", default=None)

    flag_kwargs = {"action": "store_true", "default": False}
    parser.add_argument("--prepare", **flag_kwargs)
    parser.add_argument("--create-crash-states", **flag_kwargs)
    parser.add_argument("--prepare-output-dir", default=False)
    parser.add_argument("--run-crash-state", **flag_kwargs)
    parser.add_argument("--check-crash-state", **flag_kwargs)
    return parser.parse_args()

def main():
    args = get_args()
    exp = Experiment(args.repo)
    exp.root = None
    if args.prepare:
        exp.root = Path(".").absolute() / "prepare"
        exp.root.mkdir(parents=True, exist_ok=True)
        exp.prepare(args)

    if args.create_crash_states:
        exp.root = Path(".").absolute() / "crash_states"
        exp.root.mkdir(parents=True, exist_ok=True)
        exp.create_crash_states(args.prepare_output_dir)

    if args.run_crash_state:
        assert args.crash_state_dir is not None
        p = Path(args.crash_state_dir)
        assert p.exists()

        exp.root = p.absolute() / "output"
        exp.root.mkdir(parents=True, exist_ok=True)
        exp.run_crash_state(p / "input", args.num_files)

    if args.check_crash_state:
        pass #check_crash_state(args)

if __name__ == '__main__':
    main()
