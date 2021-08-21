set -e  # exit if any fails
set -u  # all env vars must be set

function print_usage_and_exit() {
	echo "Usage: $0 [ varmail | webserver ] [ ufs | ext4 ]"
	echo "  Specify which filebench to compile for which filesystem"
	echo "    varmail:   Varmail workload"
	echo "    webserver: Webserver workload"
	echo "    ufs:  compile with uFS APIs"
	echo "    ext4: compile with ext4 APIs"
	exit 1
}

if [ ! $# = "2" ]; then print_usage_and_exit; fi
if [ ! "$1" = "varmail" ] && [ ! "$1" = "webserver" ]; then print_usage_and_exit; fi
if [ ! "$2" = "ufs" ] && [ ! "$2" = "ext4" ]; then print_usage_and_exit; fi

source "$AE_SCRIPT_DIR/common.sh"

cd "$AE_REPO_DIR"
git checkout "$AE_UFS_FILEBENCH_BRANCH"

if [ "$1" = "varmail" ]; then
	export VAR_MACRO='-DVAR'
elif [ "$1" = "webserver" ]; then
	export VAR_MACRO=''
fi

if [ "$2" = "ufs" ]; then
	# Ensure uFS is compiled correctly
	cmpl-ufs '-DCFS_DISK_LAYOUT_FILEBENCH=ON'
	export LIB_CFS='-lcfs'
	export CFS_MACRO='-DCFS'
elif [ "$2" = "ext4" ]; then
	# Ensure these two environment variables empty
	export LIB_CFS=''
	export CFS_MACRO=''
fi

cd "$AE_BENCH_REPO_DIR"
git checkout "$AE_BENCH_BRANCH"
cd filebench
libtoolize
aclocal
autoheader
automake --add-missing
autoconf
./configure
make clean
make -j "$AE_CMPL_THREADS"
sudo make install
