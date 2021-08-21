#! /bin/bash
# This script has no assumption on the current working directory nor compilation
# cache. Instead, it always find path based on the environment variables
# provided and always clear the cache.


set -e  # exit if any fails
set -u  # all env vars must be set

function print_usage_and_exit() {
	echo "Usage: $0"
	echo "  Only compile for uFS. No additional argument is accepted"
	exit 1
}

if [ ! $# = "0" ]; then print_usage_and_exit; fi

source "$AE_SCRIPT_DIR/common.sh"

cd "$AE_REPO_DIR"
git checkout "$AE_BRANCH"

# Compile uFS with flag to output load balancing information
cmpl-ufs '-DUFS_EXPR_LBNC=ON' '-DCFS_DISK_LAYOUT_LEVELDB=ON'
