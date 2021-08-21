#! /bin/bash
# This script has no assumption on the current working directory. Instead, it
# always find path based on the environment variables provided.

set -e  # exit if any fails
set -u  # all env vars must be set

function print_usage_and_exit() {
	echo "Usage: $0 [ ldbal | calloc | dynamic ]"
	echo "  Specify which figure to plot"
	echo "    ldbal:   load balancing benchmark; fig. 9 in the uFS paper"
	echo "    calloc:  core allocation benchmark; fig. 10 in the uFS paper"
	echo "    dynamic: dynmaic behavior of load management; fig. 11 in the uFS paper"
	exit 1
}

if [ ! $# = "1" ]; then print_usage_and_exit; fi
if [ ! "$1" = "ldbal" ] && [ ! "$1" = "calloc" ] && [ ! "$1" = "dynamic" ]; then print_usage_and_exit; fi

source "$AE_SCRIPT_DIR/common.sh"

# some scripts' helpers may need it
export LOADMNG_PLOT_DIR="$AE_REPO_DIR/cfs_bench/exprs/loadmng_plot"

if [ "$1" = "ldbal" ]; then
	# data_dir
	LDBAL_UFS_DIR="$AE_DATA_DIR/DATA_loadmng_ldbal_ufs"
	LDBAL_RR_DIR="$AE_DATA_DIR/DATA_loadmng_ldbal_rr"
	LDBAL_MAX_DIR="$AE_DATA_DIR/DATA_loadmng_ldbal_max"
	test-data-dir-exist $LDBAL_UFS_DIR
	test-data-dir-exist $LDBAL_RR_DIR
	test-data-dir-exist $LDBAL_MAX_DIR

	# parse and plot
	bash "$LOADMNG_PLOT_DIR/ldbal/parse_ldbal_log.sh" "$LDBAL_UFS_DIR" "$LDBAL_RR_DIR" "$LDBAL_MAX_DIR" > /dev/null
	python3 "$LOADMNG_PLOT_DIR/ldbal/summarize_ldbal.py" "$LDBAL_UFS_DIR" "$LDBAL_RR_DIR" "$LDBAL_MAX_DIR" | tee ./loadmng_ldbal_results.txt
elif [ "$1" = "calloc" ]; then
	# data_dir
	CALLOC_DIR="$AE_DATA_DIR/DATA_loadmng_calloc"
	test-data-dir-exist $CALLOC_DIR

	bash "$LOADMNG_PLOT_DIR/calloc/parse_calloc_log.sh" "$CALLOC_DIR" > /dev/null
	python3 "$LOADMNG_PLOT_DIR/calloc/summarize_calloc.py" "$CALLOC_DIR" | tee ./loadmng_calloc_results.txt
elif [ "$1" = "dynamic" ]; then
	# data_dir
	DYNAMIC_DIR="$AE_DATA_DIR/DATA_loadmng_dynamic"
	test-data-dir-exist $DYNAMIC_DIR

	for RPT in 0 1 2; do
		data_log_dir="$DYNAMIC_DIR/log_loadexpr_no-1001_ql-1.2_rpt-${RPT}_NW-8_NA-8/log-numseg-1"
		python3 "$LOADMNG_PLOT_DIR/dynamic/plot_dynamic.py" "$data_log_dir" 8 "rpt$RPT" > /dev/null
		# copy figures into the current working directory
		cp -f "$data_log_dir/dynamic-behavior-app-throughput-rpt${RPT}.png" ./
		cp -f "$data_log_dir/ufs-cpu-utilization-rpt${RPT}.png" ./
	done
fi
