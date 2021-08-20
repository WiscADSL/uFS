# This script provides some handy functions for artifact evaluation
# To use them, `source "$AE_SCRIPT_DIR/common.sh"`

## Data management

### Usage: data_dir=$(mk-data-dir exper_name)
function mk-data-dir() {
	if [ ! $# = "1" ]; then
		echo "Usage: mk-data-dir exper_name" >&2
		exit 1
	fi
	exper_name="$1"
	latest_dir="$(pwd)/DATA_${exper_name}_latest"
	ae_data_dir="$AE_DATA_DIR/DATA_${exper_name}"
	sudo rm -rf "$latest_dir"
	sudo rm -rf "$ae_data_dir"
	data_dir="$(pwd)/DATA_${exper_name}_$(date +%m-%d-%H-%M-%S)"
	mkdir "$data_dir"
	ln -s "$data_dir" "$latest_dir"
	ln -s "$data_dir" "$ae_data_dir"  # link it under $AE_DATA_DIR
	echo "$data_dir"
}

function test_data_dir_exist() {
	if [ ! -d "$1" ]; then
		echo "Data directory not found: Expect $1"
		exit 1
	fi
}

## Compilation

### Usage: cmpl-ufs [ CMAKE_MACRO ] ...
function cmpl-ufs() {
	curr_dir=$PWD
	cd $AE_REPO_DIR/cfs
	bash ./tools/clean-cmake-cfs.sh
	cd build

	if [ ! $# = "0" ]; then
		cmake "$@" ..
	fi
	make -j "$AE_CMPL_THREADS"

	rm -rf $AE_REPO_DIR/cfs_bench/build
	mkdir -p $AE_REPO_DIR/cfs_bench/build
	cd $AE_REPO_DIR/cfs_bench/build
	cmake ..
	make -j "$AE_CMPL_THREADS"
	cd $curr_dir
}


## Environment Setup

function setup-spdk() {
	sudo -E python3 $AE_REPO_DIR/cfs_bench/exprs/fsp_microbench_suite.py --fs fsp --devonly
}

function reset-spdk() {
	sudo bash $AE_REPO_DIR/cfs/lib/spdk/scripts/setup.sh reset
}

function cleanup-ufs-config() {
	sudo rm -rf /tmp/spdk.conf
	sudo rm -rf /tmp/fsp.conf
}
