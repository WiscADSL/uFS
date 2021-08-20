# uFS Artifact Evaluation

## Overview

The entry point of the artifact evaluation is the script `artifact_eval.sh`. It could even be run independently (e.g. download this script alone without the whole codebase), in which case it will pull the codebase by itself. It typically involves these steps:

1. Initialization: It will set up environments to run this script, including install dependencies and changes some system configurations. The entry point of this step is using `init` and `init-after-reboot` as the first argument of `artifact_eval.sh` (see details below).

2. Compilation: It compiles uFS and/or benchmark code. Note that, the code must be compiled with the corresponding configurations before running any experiments. The entry point is `cmpl`.

3. Run: It runs the experiments and generates data. The entry point is `run`. Each experiment will generate data in its own directory, and the script will then symbolically link these data directories in `./AE_DATA` for easy access. If the experiments get killed in the middle, the data directories should be empty.

4. Plot: It reads data produced by `run` (from `./AE_DATA`) and plots them. The entry point is `plot`. Make sure your working directory is the same as the one when calling `run`.

Below are step-by-step instructions. Enjoy!

## Initialization

To begin with, run `artifact_eval.sh` with the current machine's type. Currently only support two types: `cloudlab` and `adsl`. If running on CloudLab, the machine must be of hardware type c6525-100g. If running on CloudLab:

```bash
bash artifact_eval.sh init cloudlab
```

If running on a machine managed by ADSL, use `adsl` instead.

The initialization takes three steps:

- `mount`: only on CloudLab machines; mount `/dev/nvme0n1p4` so that we could have enough disk space. It generates `~/.ae_mount_done` so that the next time running `artifact_eval.sh`, it will skip this step.

- install: install dependencies and set some environment variables; once done, it generates `~/.ae_install_done` for similar purposes.

- `config`: set some configurations e.g. lift memory limit so that SPDK could allocate enough huge page memory. It generates `~/.ae_config_done`.

It will also create `~/.ae_env.sh` with some environment variables required by the following benchmarking. When starting a new shell, `~/.ae_env.sh` will be loaded automatically. Currently, only `zsh` and `bash` are supported. After this step, please reboot the machine to ensure the necessary settings take effect. After reboot, you should be able to use `ae` as a shortcut/alias of `bash artifact_eval.sh`. Note that it will always run the `artifact_eval.sh` in `$HOME/workspace/uFS/cfs_bench/exprs/artifact_eval`, regardless whether you have `artifact_eval.sh` in your current working directory or not.

After rebooting the machine, run

```bash
ae init-after-reboot cloudlab
```

This command will set up environments for later benchmarking e.g. disabling hyperthreading and reserving huge pages for SPDK (which should be done immediately after rebooting so that the kernel still have hugepage memory available). This finally completes the environment initialized. If for any reason, the machine reboots again, `init-after-reboot` should be rerun before any benchmarking.

## Microbenchmark

Microbenchmark consists of two experiments: one is a quick comparison between uFS and ext4, where both have their journaling disabled and uFS only uses one thread; the other is a more sophisticated comparison where uFS and ext4 are both in their default settings (both with journalling and uFS is multithreaded).

### Microbenchmark: single-threaded uFS vs. ext4

To compare single-threaded uFS and ext4, both no journaling (fig. 5 in uFS paper):

```bash
# compile no-journal uFS
ae cmpl microbench ufsnj
# ext4 doesn't require additional compilation for microbenchmark

# run single-threaded uFS
ae run microbench ufs-single
# The results could be accessed through the symbolic link `./AE_DATA/DATA_ufs-single`

# run no-journal ext4
ae run microbench ext4nj
# The results could be accessed through the symbolic link `./AE_DATA/DATA_ext4nj`

# plot the results
ae plot microbench single
```

### Microbenchmark: multithreaded uFS vs. ext4

To compare multithreaded uFS and ext4 (fig. 6 in uFS paper):

```bash
# compile uFS with journaling (default)
ae cmpl microbench ufs

# run multi-threaded uFS (default)
ae run microbench ufs
# the results could be accessed through the symbolic link `./AE_DATA/DATA_ufs`

# run journaling ext4 (default)
ae run microbench ext4
# the results could be accessed through the symbolic link `./AE_DATA/DATA_ext4`

# plot the results
ae plot microbench multi
```

Note that the microbenchmark includes 32 workloads, and each will be run cases with 1-10 applications. It would take a few hours to complete one `run`. You may want to use `tmux` to make your life easier :)

## Filebench


## Load Management

Load management includes three experiments: load balancing benchmark, core allocation benchmark, and a demonstration of uFS behavior under the dynamic workload. To begin with, compile uFS with configurations for load management benchmarks:

```bash
ae cmpl loadmng
```

### Load Management: Load Balance

To compare uFS load balancing performance with other alternative settings (fig. 9 in uFS paper):

```bash
# run load balancing benchmarks (three policies: ufs, ufs_max, ufs_rr)
ae run loadmng ldbal
# the results could be accessed through the symbolic link `./AE_DATA/DATA_loadmng_ufs`, `./AE_DATA/DATA_loadmng_max`, and `./AE_DATA/DATA_loadmng_rr`

ae plot loadmng ldbal
```

### Load Management: Core Allocation

To compare uFS core allocation performance with other alternative settings (fig. 10 in uFS paper):

```bash
# run core allocation benchmarks (two policies: ufs and ufs_max; two workload changing patterns: gradual and bursty)
ae run loadmng calloc
# the results could be accessed through the symbolic link `./AE_DATA/DATA_calloc`

ae plot loadmng calloc
```

### Load Management: Dynamic Behavior

To demonstrate uFS dynamic behavior under varying workload (fig. 11 in uFS paper):

```bash
# run the demonstration of uFS under dynamic workload
ae run loadmng dynamic
# the results could be accessed through the symbolic link `./AE_DATA/DATA_dynamic`

ae plot loadmng dynamic
```

## LevelDB


## Advanced Usage

`artifact_eval.sh` could take some environment variables to customize its behavior:

- `AE_BRANCH`: customize which branch of uFS repository to use

- `AE_REPO_URL`: URL to pull uFS repository

- `AE_BENCH_BRANCH`: customize which branch of uFS-bench repository to use

- `AE_BENCH_REPO_URL`: URL to pull uFS-bench repository
