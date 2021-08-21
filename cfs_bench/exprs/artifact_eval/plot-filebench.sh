#! /bin/bash
# This script has no assumption on the current working directory. Instead, it
# always find path based on the environment variables provided.

set -e  # exit if any fails
set -u  # all env vars must be set
set -o pipefail  # exit if any pipe broken

function print_usage_and_exit() {
	echo "Usage: $0 [ varmail | webserver ]"
	echo "  Specify which figure to plot"
	echo "    varmail:   uFS with different number of threads vs. ext4 on Varmail benchmark; fig. 8-left in the uFS paper"
	echo "    webserver: uFS with different cache hit rate vs. ext4 on Webserver benchmark; fig. 8-middle in the uFS paper"
	exit 1
}

if [ ! $# = "1" ]; then print_usage_and_exit; fi
if [ ! "$1" = "varmail" ] && [ ! "$1" = "webserver" ]; then print_usage_and_exit; fi

source "$AE_SCRIPT_DIR/common.sh"

# some scripts' helpers may need it
FILEBENCH_PLOT_DIR="$AE_REPO_DIR/cfs_bench/exprs/filebench_plot"

if [ "$1" = "varmail" ]; then
	# data_dir
	VARMAIL_UFS_DIR="$AE_DATA_DIR/DATA_filebench_varmail_ufs"
	VARMAIL_EXT4_DIR="$AE_DATA_DIR/DATA_filebench_varmail_ext4"
	test-data-dir-exist $VARMAIL_UFS_DIR
	test-data-dir-exist $VARMAIL_EXT4_DIR

	python3 "$FILEBENCH_PLOT_DIR/varmail/parse_log.py" "ufs" "$VARMAIL_UFS_DIR"
	python3 "$FILEBENCH_PLOT_DIR/varmail/parse_log.py" "ext4" "$VARMAIL_EXT4_DIR"
	python3 "$FILEBENCH_PLOT_DIR/varmail/merge_data.py" "$VARMAIL_UFS_DIR" "$VARMAIL_EXT4_DIR" | tee ./varmail.data
	python3 "$FILEBENCH_PLOT_DIR/varmail/plot_varmail.py" filebench_varmail ./varmail.data
elif [ "$1" = "webserver" ]; then
	# data_dir
	WEBSERVER_UFS_DIR="$AE_DATA_DIR/DATA_filebench_webserver_ufs"
	WEBSERVER_EXT4_DIR="$AE_DATA_DIR/DATA_filebench_webserver_ext4"
	test-data-dir-exist $WEBSERVER_UFS_DIR
	test-data-dir-exist $WEBSERVER_EXT4_DIR

	python3 "$FILEBENCH_PLOT_DIR/webserver/parse_log.py" "ufs" "$WEBSERVER_UFS_DIR"
	python3 "$FILEBENCH_PLOT_DIR/webserver/parse_log.py" "ext4" "$WEBSERVER_EXT4_DIR"
	python3 "$FILEBENCH_PLOT_DIR/webserver/merge_data.py" "$WEBSERVER_UFS_DIR" "$WEBSERVER_EXT4_DIR" | tee ./webserver.data
	python3 "$FILEBENCH_PLOT_DIR/webserver/plot_webserver.py" filebench_webserver ./webserver.data
fi
