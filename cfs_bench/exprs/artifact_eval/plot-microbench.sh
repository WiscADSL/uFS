#! /bin/bash
# This script has no assumption on the current working directory. Instead, it
# always find path based on the environment variables provided.

set -e  # exit if any fails
set -u  # all env vars must be set

# The plot script by designed is a little restricted as it involves coloring and
# legend placement. We are using zplot to make the figures, and the color and
# placement are tuned to be reader-friendly. This implies that it might be
# difficult to make this plot script very general.
function print_usage_and_exit() {
	echo "Usage: $0 [ single | multi ]"
	echo "  Specify which figure to plot"
	echo "    single: single-threaded uFS (no journal) vs. ext4 (no journal); fig. 5 in the uFS paper"
	echo "    multi:  multithreaded uFS (with journal) vs. ext4 (with journal); fig. 6 in the uFS paper"
	exit 1
}

if [ ! $# = "1" ]; then print_usage_and_exit; fi
if [ ! "$1" = "single" ] && [ ! "$1" = "multi" ]; then print_usage_and_exit; fi

source "$AE_SCRIPT_DIR/common.sh"

MICROBENCH_PLOT_DIR=$AE_REPO_DIR/cfs_bench/exprs/microbench_plot

if [ "$1" = "single" ]; then # Please make sure this ufs is compiled with nj
	# test data dir exist
	test-data-dir-exist $AE_DATA_DIR/DATA_microbench_ufs-single
	test-data-dir-exist $AE_DATA_DIR/DATA_microbench_ext4nj

	# parse output and generate CSV
	python3 "$MICROBENCH_PLOT_DIR/parse_log.py" --fs fsp --dir $AE_DATA_DIR/DATA_microbench_ufs-single
	python3 "$MICROBENCH_PLOT_DIR/parse_log.py" --fs ext4nj --dir $AE_DATA_DIR/DATA_microbench_ext4nj
	python3 "$MICROBENCH_PLOT_DIR/plot_ufs_ext4_cmp.py" "microbench_single" "uFSnj:$AE_DATA_DIR/DATA_microbench_ufs-single" "ext4nj:$AE_DATA_DIR/DATA_microbench_ext4nj"
elif [ "$1" = "multi" ]; then
	# test data dir exist
	test-data-dir-exist $AE_DATA_DIR/DATA_microbench_ufs
	test-data-dir-exist $AE_DATA_DIR/DATA_microbench_ext4

	python3 "$MICROBENCH_PLOT_DIR/parse_log.py" --fs fsp --dir $AE_DATA_DIR/DATA_microbench_ufs
	python3 "$MICROBENCH_PLOT_DIR/parse_log.py" --fs ext4 --dir $AE_DATA_DIR/DATA_microbench_ext4
	python3 "$MICROBENCH_PLOT_DIR/plot_ufs_ext4_cmp.py" "microbench_multi" "uFS:$AE_DATA_DIR/DATA_microbench_ufs" "ext4:$AE_DATA_DIR/DATA_microbench_ext4"
fi
