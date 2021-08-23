#! /bin/bash
# This script has no assumption on the current working directory. Instead, it
# always find path based on the environment variables provided.

set -e  # exit if any fails
set -u  # all env vars must be set

function print_usage_and_exit() {
	echo "Usage: $0 [ varmail | webserver ] [ ufs | ext4 ]"
	echo "  Specify which filebench to run for which filesystem"
	echo "    varmail:   Varmail workload"
	echo "    webserver: Webserver workload"
	echo "    ufs:  run uFS (for varmail, run 1-10 threads; for webserver, run 0/50/75/100% cache hit rate)"
	echo "    ext4: run ext4 for given workload"
	exit 1
}

if [ $# -lt 2 ]; then print_usage_and_exit; fi
if [ ! "$1" = "varmail" ] && [ ! "$1" = "webserver" ]; then print_usage_and_exit; fi
if [ ! "$2" = "ufs" ] && [ ! "$2" = "ext4" ]; then print_usage_and_exit; fi

source "$AE_SCRIPT_DIR/common.sh"

# Must be careful that the scripts below can only run in this working directory
cd "$AE_BENCH_REPO_DIR/filebench"

# Create directory for data
data_dir="$(mk-data-dir filebench_$1_$2)"

if [ "$2" = "ufs" ]; then
	# Ensure SSD in under SPDK's control
	setup-spdk
	# Ensure no configs left in the last round
	cleanup-ufs-config
	# Prepare spdk.conf
	echo 'dev_name = "spdkSSD";' >> /tmp/spdk.conf
	echo 'core_mask = "0x2";' >> /tmp/spdk.conf
	echo 'shm_id = "9";' >> /tmp/spdk.conf
	# Prepare fsp.conf
	echo 'splitPolicyNum = "5";' >> /tmp/fsp.conf
	echo 'serverCorePolicyNo = "5";' >> /tmp/fsp.conf
	echo 'dirtyFlushRatio = "0.9";' >> /tmp/fsp.conf
	echo 'raNumBlock = "16";' >> /tmp/fsp.conf

	# Now run benchmark script
	if [ "$1" = "varmail" ]; then
		sudo -E python3 scripts/run_varmail_ufs.py "$data_dir"
	elif [ "$1" = "webserver" ]; then
		sudo -E python3 scripts/run_webserver_ufs.py "$data_dir" 0
	fi

	# Clean up config files
	cleanup-ufs-config

elif [ "$2" = "ext4" ]; then
	# Ensure SSD is unbound from SPDK
	reset-spdk
	setup-ext4

	echo "===================================================================="
	echo "Ext4 mount succeeds. However before further experiments, we will wait for $AE_EXT4_WAIT_AFTER_MOUNT seconds, because ext4's mount contains lazy operations, which would affect performance significantly. To ensure fair comparsion, we will resume experiments $AE_EXT4_WAIT_AFTER_MOUNT seconds later. Go grab a coffee!"
	echo "===================================================================="
	sleep $AE_EXT4_WAIT_AFTER_MOUNT
	echo "Now we resumes..."

	# Run benchmark
	if [ "$1" = "varmail" ]; then
		sudo -E python3 scripts/run_varmail_ext4.py "$data_dir"
	elif [ "$1" = "webserver" ]; then
		sudo -E python3 scripts/run_webserver_ext4.py "$data_dir" 0
	fi

	# umount ext4
	reset-ext4
fi
