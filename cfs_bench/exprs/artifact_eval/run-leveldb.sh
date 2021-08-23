#! /bin/bash
# This script has no assumption on the current working directory. Instead, it
# always find path based on the environment variables provided.

set -e  # exit if any fails
set -u  # all env vars must be set

function print_usage_and_exit() {
	echo "Usage: $0 [ ycsb-a | ycsb-b | ycsb-c | ycsb-d | ycsb-e | ycsb-f | all ] [ ufs | ext4 ]"
	echo "  Specify which LevelDB workload to run on which filesystem"
	echo "    workload: 6 workloads available (shown above) OR run them all"
	echo "    ufs:  run uFS for given workload from 1 app to 10 apps"
	echo "    ext4: run ext4 for given workload from 1 app to 10 apps"
	exit 1
}

if [ ! $# = "2" ]; then print_usage_and_exit; fi
if [ ! "$1" = "ycsb-a" ] && [ ! "$1" = "ycsb-b" ] && [ ! "$1" = "ycsb-c" ] && \
	[ ! "$1" = "ycsb-d" ] && [ ! "$1" = "ycsb-e" ] && [ ! "$1" = "ycsb-f" ] && \
	[ ! "$1" = "all"  ]
then print_usage_and_exit; fi
if [ ! "$2" = "ufs" ] && [ ! "$2" = "ext4" ]; then print_usage_and_exit; fi

# Finish checking, now execute
source "$AE_SCRIPT_DIR/common.sh"

# Go to destination to run
cd "$AE_BENCH_REPO_DIR/leveldb-1.22"

REUSE_DATA_DIR="/ssd-data/1"

# provide workload name as the first arguments
function run_one_workload_ufs() {
	workload=$1
	data_dir="$(mk-data-dir leveldb_${workload}_ufs)"
	echo "Run LevelDB: $workload"
	sudo -E python3 scripts/run_ldb_ufs.py "$workload" "$data_dir" "${@:2}"
}

function run_one_workload_ext4() {
	workload=$1
	data_dir="$(mk-data-dir leveldb_${workload}_ext4)"
	echo "Run LevelDB: $workload"
	sudo -E python3 scripts/run_ldb_ext4.py --workload "$workload" --output-dir "$data_dir" --reuse-data-dir $REUSE_DATA_DIR "${@:2}"
}

function load_data_ext4() {
	run_one_workload_ext4 fillseq --num-app-only 10
}

# Running YCSB workload requires an existing LevelDB image, so we need to load
# the data into LevelDB first before running any YCSB workload
# For uFS: loading data is fast, so we mkfs to clear all the data and reload it
#     everytime before running YCSB workload
# For ext4: loading data is slow, so we only run loading once and back up the
#     image into /ssd-data/1/; everytime before running YCSB workload, we copy
#     the backup image into benchmarking directory i.e. /ssd-data/0/
# Thus, it is strongly preferred to run `all` instead of individual `ycsb-X`
# one-by-one, so that the script could resue the image
if [ "$2" = "ufs" ]; then
	# Setup device
	setup-spdk
	# Prep for spdk's config
	cleanup-ufs-config
	echo 'dev_name = "spdkSSD";' >> /tmp/spdk.conf
	echo 'core_mask = "0x2";' >> /tmp/spdk.conf
	echo 'shm_id = "9";' >> /tmp/spdk.conf

	# run_ldb_ufs would do load itself everytime
	if [ "$1" = "all" ]; then
		for job in 'a' 'b' 'c' 'd' 'e' 'f'
		do
			run_one_workload_ufs "ycsb-${job}"
		done
	else
		run_one_workload_ufs "$1"
	fi
elif [ "$2" = "ext4" ]; then
	# Make sure the device can be seen by the kernel
	reset-spdk
	setup-ext4

	sudo rm -rf "/ssd-data/0"
	sudo rm -rf "$REUSE_DATA_DIR"
	sudo mkdir -p "/ssd-data/0"
	sudo mkdir -p "$REUSE_DATA_DIR"

	echo "===================================================================="
	echo "Ext4 mount succeeds. However before further experiments, we will wait for $AE_EXT4_WAIT_AFTER_MOUNT seconds, because ext4's mount contains lazy operations, which would affect performance significantly. To ensure fair comparsion, we will resume experiments $AE_EXT4_WAIT_AFTER_MOUNT seconds later. Go grab a coffee!"
	echo "===================================================================="
	sleep $AE_EXT4_WAIT_AFTER_MOUNT
	echo "Now we resumes..."

	if [ "$1" = "all" ]; then
		load_data_ext4
		for job in 'a' 'b' 'c' 'd' 'e' 'f'
		do
			run_one_workload_ext4 "ycsb-${job}"
		done
	else
		load_data_ext4
		run_one_workload_ext4 "$1"
	fi

	# umount ext4
	reset-ext4
fi
