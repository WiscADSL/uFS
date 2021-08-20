#! /bin/bash
# This script has no assumption on the current working directory. Instead, it
# always find path based on the environment variables provided.

set -e  # exit if any fails
set -u  # all env vars must be set

function print_usage_and_exit() {
	echo "Usage: $0 [ ufs | ufs-single | ext4 | ext4nj ]"
	echo "  Specify microbench to compile for"
	echo "    ufs:        multithreaded uFS (depends on cmpl, may or may not use journaling)"
	echo "    ufs-single: single-threaded uFS (depends on cmpl, may or may not use journaling)"
	echo "    ext4:       ext4 with journaling (default of ext4)"
	echo "    ext4nj:     ext4 without journaling"
	exit 1
}

if [ $# -lt 1 ]; then print_usage_and_exit; fi
if [ ! "$1" = "ufs" ] && [ ! "$1" = "ufs-single" ] && \
	[ ! "$1" = "ext4" ] && [ ! "$1" = "ext4nj" ]
then print_usage_and_exit; fi

source "$AE_SCRIPT_DIR/common.sh"
cd $AE_REPO_DIR/cfs_bench/exprs

# Some pre-run cleaning
set +e
## Clean up data directories
lost_found_dir=$AE_REPO_DIR/cfs_bench/exprs/DATA_lost_found/DATA_arkv_$(date +%m-%d-%H-%M-%S)
mkdir -p "$lost_found_dir"

# These lines may return error if no match found: so wrap it into a subshell
sudo mv ext4_*_run_0 "$lost_found_dir"
sudo mv fsp_*_run_0 "$lost_found_dir"
sudo mv log$(date +%Y)-* "$lost_found_dir"
sudo mv log_ext4* "$lost_found_dir"
sudo mv log_fsp* "$lost_found_dir"
set -e

# Create directory for this round and make a symbolic links to it
data_dir="$(mk-data-dir microbench_$1)"

if [ "$1" = "ufs" ]; then
	cleanup-ufs-config  # fsp_microbench_suite.py will generate its own config
	sudo -E python3 fsp_microbench_suite.py --fs fsp --numapp=10 "${@:2}"
	sudo mv fsp_*_run_0 "$data_dir"
	cleanup-ufs-config
elif [ "$1" = "ufs-single" ]; then
	cleanup-ufs-config
	sudo -E python3 fsp_microbench_suite.py --fs fsp --numapp=10 --single "${@:2}"
	sudo mv fsp_*_run_0 "$data_dir"
	cleanup-ufs-config
elif [ "$1" = "ext4" ]; then
	reset-spdk
	sudo -E python3 fsp_microbench_suite.py --fs ext4 --numapp=10 "${@:2}"
	sudo mv ext4_*_run_0 "$data_dir"
elif [ "$1" = "ext4nj" ]; then
	reset-spdk
	sudo -E python3 fsp_microbench_suite.py --fs ext4nj --numapp=10 "${@:2}"
	sudo mv ext4_*_run_0 "$data_dir"
fi
