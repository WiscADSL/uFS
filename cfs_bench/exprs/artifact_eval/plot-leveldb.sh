#! /bin/bash
# This script has no assumption on the current working directory. Instead, it
# always find path based on the environment variables provided.

set -e  # exit if any fails
set -u  # all env vars must be set
set -o pipefail  # exit if any pipe broken

function print_usage_and_exit() {
	echo "Usage: $0 [ ycsb-a | ycsb-b | ycsb-c | ycsb-d | ycsb-e | ycsb-f ]"
	echo "  Specify which workload to plot"
	exit 1
}

if [ ! $# = "1" ]; then print_usage_and_exit; fi
if [ ! "$1" = "ycsb-a" ] && [ ! "$1" = "ycsb-b" ] && [ ! "$1" = "ycsb-c" ] && \
	[ ! "$1" = "ycsb-d" ] && [ ! "$1" = "ycsb-e" ] && [ ! "$1" = "ycsb-f" ]
then print_usage_and_exit; fi

workload="$1"

source "$AE_SCRIPT_DIR/common.sh"

LEVELDB_PLOT_DIR="$AE_REPO_DIR/cfs_bench/exprs/leveldb_plot"

UFS_DATA_DIR="$AE_DATA_DIR/DATA_leveldb_${workload}_ufs"
EXT4_DATA_DIR="$AE_DATA_DIR/DATA_leveldb_${workload}_ext4"

python3 "$LEVELDB_PLOT_DIR/parse_log.py" "$workload" "$UFS_DATA_DIR" "$EXT4_DATA_DIR" | tee ./${workload}.data
