#! /bin/bash

set -e  # exit if any fails
set -u  # all env vars must be set

# shell script to generate the summary data for sub-directory

function print_usage_and_exit() {
	echo "Usage: $0 <ldbal_ufs_dir> <ldbal_rr_dir> <ldbal_max_dir>"
	exit 1
}

if [ ! $# = "3" ]; then print_usage_and_exit; fi

LDBAL_UFS_DIR="$1"
LDBAL_RR_DIR="$2"
LDBAL_MAX_DIR="$3"

# reading exprs (a,b,c,abc)
python3 "$AE_REPO_DIR/cfs_bench/exprs/fsp_analyze_lb.py" --num_rpt 5 --cgstql 1.1 --log_dir "$LDBAL_UFS_DIR" --num_worker 4 --num_app 6 --summary_log --expr_list [100,101,102,103]
python3 "$AE_REPO_DIR/cfs_bench/exprs/fsp_analyze_lb.py" --num_rpt 5 --cgstql 1.1 --log_dir "$LDBAL_RR_DIR" --num_worker 4 --num_app 6 --summary_log --use_mod --expr_list [100,101,102,103]
python3 "$AE_REPO_DIR/cfs_bench/exprs/fsp_analyze_lb.py" --num_rpt 5 --cgstql 1.1 --log_dir "$LDBAL_MAX_DIR" --num_worker 6 --num_app 6 --summary_log --max_perf --expr_list [100,101,102,103]

mv "$LDBAL_UFS_DIR/perf.csv" "$LDBAL_UFS_DIR/read_perf.csv"
mv "$LDBAL_RR_DIR/perf.csv" "$LDBAL_RR_DIR/read_perf.csv"
mv "$LDBAL_MAX_DIR/perf.csv" "$LDBAL_MAX_DIR/read_perf.csv"
mv "$LDBAL_UFS_DIR/stable_time.csv" "$LDBAL_UFS_DIR/read_stable_time.csv"

# writing exprs (e,f,g,efg,all)
python3 "$AE_REPO_DIR/cfs_bench/exprs/fsp_analyze_lb.py" --num_rpt 5 --cgstql 1.1 --log_dir "$LDBAL_UFS_DIR" --num_worker 4 --num_app 6 --summary_log --expr_list [200,201,202,203,300]
python3 "$AE_REPO_DIR/cfs_bench/exprs/fsp_analyze_lb.py" --num_rpt 5 --cgstql 1.1 --log_dir "$LDBAL_RR_DIR" --num_worker 4 --num_app 6 --summary_log --use_mod --expr_list [200,201,202,203,300]
python3 "$AE_REPO_DIR/cfs_bench/exprs/fsp_analyze_lb.py" --num_rpt 5 --cgstql 1.1 --log_dir "$LDBAL_MAX_DIR" --num_worker 6 --num_app 6 --summary_log --max_perf --expr_list [200,201,202,203,300]
