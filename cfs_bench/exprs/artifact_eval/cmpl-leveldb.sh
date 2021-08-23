set -e  # exit if any fails
set -u  # all env vars must be set

function print_usage_and_exit() {
	echo "Usage: $0 [ ufs | ext4 ]"
	echo "  Specify compile LevelDB for which filesystem"
	echo "    ufs:  compile with uFS APIs"
	echo "    ext4: compile with ext4 APIs"
	exit 1
}

if [ ! $# = "1" ]; then print_usage_and_exit; fi
if [ ! "$1" = "ufs" ] && [ ! "$1" = "ext4" ]; then print_usage_and_exit; fi

source "$AE_SCRIPT_DIR/common.sh"

if [ "$1" = "ufs" ]; then
	cmpl-ufs '-DCFS_DISK_LAYOUT_LEVELDB=ON'
	LDB_CMAKE_CFS_ARG="-DLEVELDB_JL_LIBCFS=ON"
elif [ "$1" = "ext4" ]; then
	LDB_CMAKE_CFS_ARG="-DLEVELDB_JL_LIBCFS=OFF"
fi

cd "$AE_REPO_DIR"
git checkout "$AE_BRANCH"

# compile leveldb
cd "$AE_BENCH_REPO_DIR"
git checkout "$AE_BENCH_BRANCH"
cd leveldb-1.22
sudo rm -rf build
mkdir build && cd build
cmake ${LDB_CMAKE_CFS_ARG} -DCMAKE_BUILD_TYPE=Release ..
make -j "$AE_CMPL_THREADS"
