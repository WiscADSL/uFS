#! /bin/bash

set -e
set -u

# This file indicates which experiment data is prepared for
# Useful if running the same experiment twice, where the second round could
# reuse data prepared in the first round
LM_DATA_PREP_FNAME=".lm_prep_type"

function print_usage_and_exit() {
	echo "Usage: $0 [ ldbal | calloc | dynamic | clean ]"
	echo "  Specify which experiments to run"
	echo "    ldbal:   load balancing experiments"
	echo "    calloc:  core allocation experiments"
	echo "    dynamic: dynamic load management experiments"
	echo "    clean:   clean up prepared data in any load management experiments"
	exit 1
}

if [ $# -lt 1 ]; then print_usage_and_exit; fi

source "$AE_SCRIPT_DIR/common.sh"

# Must in this directory to run the scripts
cd $AE_REPO_DIR/cfs_bench/exprs

export USE_EXACT_NUM_APP="true"

# Remove .lm_prep_type
function clean_data() {
	sudo rm -rf "$LM_DATA_PREP_FNAME"
}

# Usage: prep_data [ lb | nc ]
# Note that "nc" is for "num cores", which is an old name for core allocations
# for compatibility reason, still use "nc" here
function prep_data() {
	PREP_TYPE=$1
	echo "Data preparation for $PREP_TYPE: Start..."

	if [ -f "$LM_DATA_PREP_FNAME" ] && [ "$(cat $LM_DATA_PREP_FNAME)" = "$PREP_TYPE" ]; then
		echo "Detect $PREP_TYPE experiments data prepared already; skip data preparation..."
		return
	else
		echo "$PREP_TYPE experiments data is not detected. Start preparing data..."
	fi

	# remove the existing one first, in case we get killed in the middle
	clean_data

	# create (and maybe write) files
	sudo -E python3 lm_prep_files.py "$PREP_TYPE"
	echo $?
	if [ ! "$?" = "0" ]; then
		echo "Fail to prepare data for $PREP_TYPE!"
		exit 1
	fi
	# write contents to files to make later writing becomes *overwrite*
	sudo -E python3 lm_test_load.py --case dyn_setup_lb_write --numworker 6 --numapp 6
	if [ ! "$?" = "0" ]; then
		echo "Fail to prepare data for $PREP_TYPE!"
		exit 1
	fi

	# if succeed, indicate the data is prepared
	echo "$PREP_TYPE" > $LM_DATA_PREP_FNAME
	echo "Data preparation for $PREP_TYPE: Done!"
}

function run_lb_read() {
	local CUR_ERR_LOG="/dev/null"
	local NUM_RPT=5

	for NO in 100 101 102 103
	do
		echo "Run: NO=$NO"
		sleep 1
		sudo -E python3 fsp_analyze_lb.py --num_rpt $NUM_RPT --cgstql 1.1 --expr_no $NO --max_perf 2>"$CUR_ERR_LOG"
		sudo mv log_loadexpr_no* $data_dir_perf
		sleep 1
		sudo -E python3 fsp_analyze_lb.py --num_rpt $NUM_RPT --cgstql 1.1 --expr_no $NO 2>"$CUR_ERR_LOG"
		sudo mv log_loadexpr_no* $data_dir_dyn
		sleep 1
		sudo -E python3 fsp_analyze_lb.py --num_rpt $NUM_RPT --cgstql 1.1 --expr_no $NO --use_mod 2>"$CUR_ERR_LOG"
		sudo mv log_loadexpr_no* $data_dir_mod
	done
}

function run_lb_write() {
	local CUR_ERR_LOG="/dev/null"
	local NUM_RPT=5

	for NO in 200 201 202 203 300
	do
		# write+sync: small vs. large
		if [ $NO -eq 200 ]; then QL=1.3; fi
		# append vs. write
		if [ $NO -eq 201 ]; then QL=1.3; fi
		# hot vs. cold
		if [ $NO -eq 202 ]; then QL=1.1; fi
		# efg
		if [ $NO -eq 203 ]; then QL=1.3; fi
		if [ $NO -eq 300 ]; then QL=1.5; fi

		echo "Run: NO=$NO, QL=$QL"
		sleep 1
		sudo -E python3 fsp_analyze_lb.py --num_rpt $NUM_RPT --cgstql $QL --expr_no $NO 2>"$CUR_ERR_LOG"
		sudo mv log_loadexpr_no* $data_dir_dyn
		sleep 1
		sudo -E python3 fsp_analyze_lb.py --num_rpt $NUM_RPT --cgstql $QL --expr_no $NO --max_perf 2>"$CUR_ERR_LOG"
		sudo mv log_loadexpr_no* $data_dir_perf
		sleep 1
		sudo -E python3 fsp_analyze_lb.py --num_rpt $NUM_RPT --cgstql $QL --expr_no $NO --use_mod 2>"$CUR_ERR_LOG"
		sudo mv log_loadexpr_no* $data_dir_mod
	done
}

function run_lb() {
	data_dir_dyn="$(mk-data-dir loadmng_ldbal_ufs)"
	data_dir_perf="$(mk-data-dir loadmng_ldbal_max)"
	data_dir_mod="$(mk-data-dir loadmng_ldbal_rr)"

	run_lb_read
	run_lb_write
}

function run_ca_one_cfg() {
	local CUR_ERR_LOG="err_log"
	# local CUR_ERR_LOG="/dev/null"

	local BURST_ARG=$1

	if [ "$BURST_ARG" == "gradual" ]; then
		PY_EXPR_BURST_ARG='--nc'
	elif [ "$BURST_ARG" == "burst" ]; then
		PY_EXPR_BURST_ARG='--burst --nc'
	else
		echo "Error argument not supported arg: $BURST_ARG"
	fi

	MAX_DIR_NAME="ca_${BURST_ARG}_max"
	DYN_DIR_NAME="ca_${BURST_ARG}_ufs"

	mkdir -p "$MAX_DIR_NAME"
	mkdir -p "$DYN_DIR_NAME"

	echo "EXPR_ARG: $PY_EXPR_BURST_ARG"
	echo "Result will save to $MAX_DIR_NAME and $DYN_DIR_NAME"

	for NO in 1002 1003 1004 1005
	do
		echo "Run: NO=$NO"
		if [ $NO -eq 1002 ]; then QL=1.1; fi
		if [ $NO -eq 1003 ]; then QL=1.1; fi
		if [ $NO -eq 1004 ]; then QL=1.1; fi
		if [ $NO -eq 1005 ]; then QL=1.2; fi
		sleep 1
		sudo -E python3 fsp_analyze_lb.py --num_rpt 1 --cgstql $QL --expr_no $NO --max_perf ${PY_EXPR_BURST_ARG} 2> "$CUR_ERR_LOG"
		sudo mv "$CUR_ERR_LOG" "log_loadexpr_no-${NO}_ql-${QL}_rpt-0_NW-6_NA-6"
		sudo mv "log_loadexpr_no-${NO}_ql-${QL}_rpt-0_NW-6_NA-6" "$MAX_DIR_NAME"
		sleep 1
		sudo -E python3 fsp_analyze_lb.py --num_rpt 1 --cgstql $QL --expr_no $NO ${PY_EXPR_BURST_ARG} 2> "$CUR_ERR_LOG"
		sudo mv "$CUR_ERR_LOG" "log_loadexpr_no-${NO}_ql-${QL}_rpt-0_NW-6_NA-6"
		sudo mv "log_loadexpr_no-${NO}_ql-${QL}_rpt-0_NW-6_NA-6" "$DYN_DIR_NAME"
	done
}

function run_ca_gradual() {
	echo "Run core allocation workload: gradual"
	run_ca_one_cfg gradual
}

function run_ca_burst() {
	echo "Run core allocation workload: burst"
	run_ca_one_cfg burst
}

function run_ca() {
	data_dir=$(mk-data-dir loadmng_calloc)
	mkdir -p "$data_dir/gradual_ufs"
	mkdir -p "$data_dir/gradual_max"
	mkdir -p "$data_dir/burst_ufs"
	mkdir -p "$data_dir/burst_max"
	for RPT in 1 2 3 4 5
	do
		# Remove possible conflicted directories
		# likely left by the last run if it gets killed in the middle
		sudo rm -rf ca_gradual_ufs
		sudo rm -rf ca_gradual_max
		sudo rm -rf ca_burst_ufs
		sudo rm -rf ca_burst_max
		run_ca_gradual
		sudo mv ca_gradual_ufs "$data_dir/gradual_ufs/run_$RPT"
		sudo mv ca_gradual_max "$data_dir/gradual_max/run_$RPT"
		run_ca_burst
		sudo mv ca_burst_ufs "$data_dir/burst_ufs/run_$RPT"
		sudo mv ca_burst_max "$data_dir/burst_max/run_$RPT"
		# Note: Only the data that completes the run will be saved to $data_dir
		#       This ensures the data in $data_dir is either empty or correct
		#       and completed data
	done
}

function run_dyn() {
	data_dir=$(mk-data-dir loadmng_dynamic)

	# demo use the same set of data as lb
	prep_data lb

	# run dynmaic workload
	sudo -E python3 fsp_analyze_lb.py --expr_no 1001 --num_worker 8 --num_rpt 3 --cgstql 1.2 --nc

	# save results
	sudo mv log_loadexpr_no* "$data_dir"
}

function pre_run() {
	# clean up data left by other experiments
	sudo rm -rf log_loadexpr_no*
}

# Ensure SPDK take over the SSD
setup-spdk

case "$1" in
	(ldbal)
		prep_data lb
		pre_run
		run_lb
		;;
	(calloc)
		prep_data nc
		pre_run
		run_ca
		;;
	(dynamic)
		pre_run
		run_dyn
		;;
	(clean)
		clean_data
		;;
	(*)
		print_usage_and_exit
		;;
esac
