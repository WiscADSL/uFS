# uFS Artifact Evaluation

## Overview

The entry point of the artifact evaluation is the script `artifact_eval.sh`. It could even be run independently (e.g. download this script alone without the whole codebase), in which case it will pull the codebase by itself. It typically involves these steps:

1. Initialization: It will set up environments to run this script, including install dependencies and changes some system configurations. The entry point of this step is using `init` and `init-after-reboot` as the first argument of `artifact_eval.sh` (see details below).

2. Compilation: It compiles uFS and/or benchmark code. Note that, the code must be compiled with the corresponding configurations before running any experiments. The entry point is `cmpl`.

3. Run: It runs the experiments and generates data. The entry point is `run`. Each experiment will generate data in its own directory, and the script will then symbolically link these data directories in `./AE_DATA` for easy access. If the experiments get killed in the middle, the data directories should be empty.

4. Plot: It reads data produced by `run` (from `./AE_DATA`) and plots them. The entry point is `plot`. Make sure your working directory is the same as the one when calling `run`.

Below are step-by-step instructions. Enjoy!

## Requirements

The experiments require a machine with at least 20 physical cores (on one NUMA node) and one NVMe SSD. To fully reproduce the results presented in our paper, an Intel Optane SSD is preferred.

These scripts are tested on Ubuntu 20.04 LTS (CloudLab machines) and Ubuntu 18.04 LTS (ADSL machines), both with kernel version 5.4. We use `gcc-10` and `g++-10` as compilers (will be installed by `artifact_eval.sh`). SPDK code included in this repository is based on SPDK 18.04.

## Initialization

To begin with, run `artifact_eval.sh` with the current machine's type. Currently only support three types: `cloudlab`, `adsl`, and `other`. If running on CloudLab, the machine must be of hardware type c6525-100g. If running on CloudLab:

```bash
wget https://raw.githubusercontent.com/WiscADSL/uFS/main/cfs_bench/exprs/artifact_eval/artifact_eval.sh
bash artifact_eval.sh init cloudlab
```

If running on a machine managed by ADSL, use `adsl` instead. If running on other machines, use `other` and must provide two environment variables: `AE_SSD_NAME` for the name of NVMe SSD and `AE_SSD_PICE_ADDR` for its PCIe address. For example

```bash
export AE_SSD_NAME='nvme0n1'
export AE_SSD_PICE_ADDR='0000:3b:00.0'
bash artifact_eval.sh init other
```

If you are not sure about your SSD's name, try `lsblk`. If you are not sure about PICe address of the SSD, try [this script](../../../cfs/lib/spdk/scripts/gen_nvme.sh).

The initialization takes three steps:

- `mount`: only on CloudLab machines; mount `/dev/nvme0n1p4` so that we could have enough disk space. It generates `~/.ae_mount_done` so that the next time running `artifact_eval.sh`, it will skip this step.

- `install`: install dependencies and set some environment variables; once done, it generates `~/.ae_install_done` for similar purposes.

- `config`: set some configurations e.g. lift memory limit so that SPDK could allocate enough huge page memory. It generates `~/.ae_config_done`.

It will also create `~/.ae_env.sh` with some environment variables required by the following benchmarking. When starting a new shell, `~/.ae_env.sh` will be loaded automatically. Currently, only `zsh` and `bash` are supported. After this step, please reboot the machine to ensure the necessary settings take effect. After reboot, you should be able to use `ae` as a shortcut/alias of `bash artifact_eval.sh`. Note that it will always run the `artifact_eval.sh` in `$HOME/workspace/uFS/cfs_bench/exprs/artifact_eval`, regardless whether you have `artifact_eval.sh` in your current working directory or not.

After rebooting the machine, run

```bash
ae init-after-reboot cloudlab # OR adsl/other
```

This command will set up benchmarking environments that don't survive from a reboot e.g. disabling hyperthreading and reserving huge pages for SPDK (which should be done immediately after rebooting so that the kernel still has hugepage memory available). This finally completes the environment initialized. If for any reason, the machine reboots again, `init-after-reboot` should be rerun before any benchmarking. For stable results, we do recommend rebooting machines between experiments.

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
# results could be accessed through the symbolic link `./AE_DATA/DATA_microbench_ufs-single`

# run no-journal ext4
ae run microbench ext4nj
# results in `./AE_DATA/DATA_microbench_ext4nj`

# plot the results
ae plot microbench single
```

This will create `microbench_single.eps` in the current working directory.

### Microbenchmark: multithreaded uFS vs. ext4

To compare multithreaded uFS and ext4 (fig. 6 in uFS paper):

```bash
# compile uFS with journaling (default)
ae cmpl microbench ufs

# run multi-threaded uFS (default)
ae run microbench ufs
# results in `./AE_DATA/DATA_microbench_ufs`

# run journaling ext4 (default)
ae run microbench ext4
# results in `./AE_DATA/DATA_microbench_ext4`

# plot the results
ae plot microbench multi
```

This will create `microbench_multi.eps` in the current working directory.

Note that the microbenchmark includes 32 workloads, and each will be run cases with 1-10 applications. It would take around 8 hours to complete all experiments for one figure. You may want to use `tmux` to make your life easier :)

## Filebench

Filebench consists of two experiments: one running Varmail workload to show the effect of uFS threads number, and the other running Webserver workload to show the effect of the client-side cache hit rate. Note the filebench experiments depend on uFS-bench repository, which should be automatically pulled during `init`.

### Filebench: Varmail

To run Varmail workload on multithreaded uFS and ext4 (fig. 8-left in uFS paper):

```bash
# compile filebench with uFS APIs and varmail's configuration
ae cmpl filebench varmail ufs
ae run filebench varmail ufs
# results in `./AE_DATA/DATA_filebench_varmail_ufs`

# then ext4 (also need recompile filebench)
ae cmpl filebench varmail ext4
ae run filebench varmail ext4
# results in `./AE_DATA/DATA_filebench_varmail_ext4`

# plot
ae plot filebench varmail
```

The application-side throughput results would be printed to `stdout` and saved to `varmail.data`. It will also generate a plot named `filebench_varmail.eps` in the current working directory. As a side note, the `plot` command only generates the case when uFS uses 1 to 4 threads, though the `run` command does run the cases of 1 to 10 threads. Empirically, we observe uFS using 5 to 10 threads behave very similar to uFS-4, so their curves mostly overlap and make the figure less readable. As a result, we decide not to put them on the figure, but their performance results would still be printed to `stdout` for reference.

### Filebench: Webserver

To run Webserver workload on uFS with different client cache hit rate and ext4 (fig. 8-middle in uFS paper):

```bash
# compile filebench with uFS APIs and webserver's configuration
ae cmpl filebench webserver ufs
ae run filebench webserver ufs
ae cmpl filebench webserver ext4
ae run filebench webserver ext4
# results in `./AE_DATA/DATA_filebench_webserver_ufs` and `./AE_DATA/DATA_filebench_webserver_ext4`

# plot
ae plot filebench webserver
```

The results would be printed to `stdout` and saved to `webserver.data`. A plot named `filebench_webserver.eps` would also be generated.

Make sure to always run `cmpl` before `run` in filebench experiments. Also note that filebench uses a customized branch of uFS in the experiments (checkout by the scripts automatically), because filebench is a stress test so that we have to lift some limits inside uFS (e.g. lift the limit on the number of file descriptors).

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
# results in `./AE_DATA/DATA_loadmng_ufs`, `./AE_DATA/DATA_loadmng_max`, and `./AE_DATA/DATA_loadmng_rr`

ae plot loadmng ldbal
```

The plot script will parse the logs and collect data. The final results would be printed to `stdout`, as well as saved to `loadmng_ldbal_results.txt` in the current working directory. Note that this `plot` command doesn't really produce a figure. The amount of data in the final results is small, in which case a table (in `txt` form) should be as clear as a bar graph.

### Load Management: Core Allocation

To compare uFS core allocation performance with other alternative settings (fig. 10 in uFS paper):

```bash
# run core allocation benchmarks (two policies: ufs and ufs_max; two workload changing patterns: gradual and bursty)
ae run loadmng calloc
# results in `./AE_DATA/DATA_loadmng_calloc`

# this plot may take a while as there is a large number of logs to process
ae plot loadmng calloc
```

This `plot` doesn't produce a figure, either. The final results would be printed to `stdout` and saved to `loadmng_calloc_results.txt`. 

### Load Management: Dynamic Behavior

To demonstrate uFS dynamic behavior under varying workload (fig. 11 in uFS paper):

```bash
# run the demonstration of uFS under dynamic workload
ae run loadmng dynamic
# results in `./AE_DATA/DATA_loadmng_dynamic`

ae plot loadmng dynamic
```

One round of experiment would produce two figures: one shows applications' throughput and the other shows uServer's CPU utilization. To alleviate the effect of randomness, the script will repeat the experiments for three rounds, and the output would suffix with `rptX` where `X` is the round number. Thus, this `plot` command would generate 6 figures in the current working directory: `dynamic-behavior-app-throughput-rptX.png` and `ufs-cpu-utilization-rptX.png` for `X` being `0`, `1`, `2`.

## LevelDB

The version of LevelDB used in this experiment is `leveldb-1.22`. We ported its filesystem calls to uFS's APIs, and the codebase would be pulled during `init`.

We use 6 YCSB traces, named as `ycsb-X` for `X` being `a` to `f`. One could run them one-by-one, as well as simply run `all` (fig. 12 in uFS paper):

```bash
ae cmpl leveldb ufs
ae run leveldb all ufs
# results in `./AE_DATA/DATA_leveldb_ycsb-X_ufs` for `X` being `a` to `f`

ae cmpl leveldb ext4
ae run leveldb all ext4
# results in `./AE_DATA/DATA_leveldb_ycsb-X_ext4` for `X` being `a` to `f`

# plot them all
for X in 'a' 'b' 'c' 'd' 'e' 'f'
do
	ae plot leveldb ycsb-$X
done
```

The results would be printed to `stdout` and saved as `ycsb-X.data`. We recommend using `all` instead of manully running these six cases one-by-one, as the option `all` enables the script to reuse environments and skip preparation, which largely speeds up the experiments.

## Advanced Usage

`artifact_eval.sh` could take some environment variables to customize its behavior:

- `AE_BRANCH`: customize which branch of uFS repository to use

- `AE_REPO_URL`: URL to pull uFS repository

- `AE_BENCH_BRANCH`: customize which branch of uFS-bench repository to use

- `AE_BENCH_REPO_URL`: URL to pull uFS-bench repository

- `AE_UFS_FILEBENCH_BRANCH`: customize which branch of uFS repository to use for filebench; filebench may need some customize configures (e.g. lift the limit of the number of fd)

- `AE_EXT4_WAIT_AFTER_MOUNT`: customize how long (unit: second) to wait after mounting ext4. Ext4's mount includes some lazy operations, which would affect its performance. Thus, we wait for 300 seconds after mounting before further experiments.

Since most of the experiments take hours to run, `sudo`'s authentication cache may expire, and `ae` would then pause somewhere and ask for `sudo` password. To alleviate this problem, we provide a `sudo` version of `ae` alias named `sudo-ae`, which only asks for `sudo` password once. Note that we don't encourage using `sudo` everywhere, so if running on CloudLab, where the password is not required for `sudo`, or you could manage to extend `sudo` authentication cache timeout, please use `ae` instead of `sudo-ae`. If you run `sudo-ae` once, you may need to always use `sudo-ae` because `ae` may not have permissions to clean up some files left by `sudo-ae`.

If you are using another script to run `ae`, you may want to add `shopt -s expand_aliases` at the beginning of your script so that `ae`, as an alias instead of an executable binary, would be expanded in a non-interactive shell.

## Fun Facts

1. You may notice the source code directory is named as `cfs`. Who is cfs? Well, uFS has experienced several interactions, and the version you see is the third one. R.I.P. for our dear version a and b.

2. Actually, version a and version b were not named as afs and bfs. They were called vsfs (very simple filesystem, later also commented by someone as "very stupid filesystem") and uffs (user-level fast filesystem). When the third iteration started, the poor author ran out of the idea of naming, so she decided to name future versions as cfs, dfs, efs, etc., and wished we won't have a ufs version of uFS. Fortunately, cfs survived.
