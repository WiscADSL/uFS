#! /bin/bash

set -e  # exit if any fails
set -u  # all env vars must be set

function print_usage_and_exit() {
	echo "Usage: $0 <calloc_dir>"
	exit 1
}

if [ ! $# = "1" ]; then print_usage_and_exit; fi

CALLOC_DIR="$1"

log1='log_loadexpr_no-1002_ql-1.1_rpt-0_NW-6_NA-6'
log2='log_loadexpr_no-1003_ql-1.1_rpt-0_NW-6_NA-6'
log3='log_loadexpr_no-1004_ql-1.1_rpt-0_NW-6_NA-6'
log4='log_loadexpr_no-1005_ql-1.2_rpt-0_NW-6_NA-6'

for rpt in 1 2 3 4 5; do
	for workload in "gradual" "burst"; do
		for core_num in "ufs" "max"; do
			wl_core="${workload}_${core_num}"
			for log_dir in $log1 $log2 $log3 $log4; do
				log_abs_path="$CALLOC_DIR/$wl_core/run_$rpt/$log_dir/log-numseg-1"
				python3 $LOADMNG_PLOT_DIR/calloc/plot_calloc.py "$log_abs_path" 6
				if [ "$core_num" = "ufs" ]; then
					python3 $LOADMNG_PLOT_DIR/calloc/get_mean_num_core.py "$log_abs_path"
					cp "$CALLOC_DIR/$wl_core/run_$rpt/$log_dir/err_log" "$log_abs_path"
					python3 $LOADMNG_PLOT_DIR/calloc/plot_inode_traffic.py "$log_abs_path" 6
				fi
			done
		done
	done
done
