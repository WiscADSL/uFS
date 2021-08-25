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

function test-data-dir-exist() {
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
	sudo rm -rf build
	bash ./tools/clean-cmake-cfs.sh
	cd build

	if [ ! $# = "0" ]; then
		cmake "$@" ..
	fi
	make -j "$AE_CMPL_THREADS"

	sudo rm -rf $AE_REPO_DIR/cfs_bench/build
	mkdir -p $AE_REPO_DIR/cfs_bench/build
	cd $AE_REPO_DIR/cfs_bench/build
	cmake ..
	make -j "$AE_CMPL_THREADS"
	cd $curr_dir
}


## Environment Setup

function setup-spdk() {
	DEV_NAME="/dev/$SSD_NAME"
	if sudo grep -qF "$DEV_NAME $KFS_MOUNT_PATH" /proc/mounts ; then
		echo "WARN: Detect $DEV_NAME has already mounted on $KFS_MOUNT_PATH"
		echo "      Will umount first before setup SPDK"
		sudo umount "$KFS_MOUNT_PATH"
	fi
	sudo -E python3 $AE_REPO_DIR/cfs_bench/exprs/fsp_microbench_suite.py --fs fsp --devonly
}

function reset-spdk() {
	DEV_NAME="/dev/$SSD_NAME"
	if sudo grep -qF "$DEV_NAME $KFS_MOUNT_PATH" /proc/mounts ; then
		echo "WARN: Detect $DEV_NAME has already mounted on $KFS_MOUNT_PATH"
		echo "      Will NOT reset SPDK"
		return
	fi
	sudo bash $AE_REPO_DIR/cfs/lib/spdk/scripts/setup.sh reset
}

# Here is the recommended pattern of using ext4:
# - setup-ext4
# - sleep 300
# - do as many experiments as possible (batch)
# - reset-ext4
# This batching is to avoid redundant time spent on waiting ext4 to finish
# lazy operations (see below)

# NOTE: ext4 have many lazy operations in mount, so it is strongly recommended
#       to wait for ~5 min before further experiments
# Ref: https://askubuntu.com/questions/402785/writes-occurring-to-fresh-ext4-partition-every-second-endlessly-cause-and-solut
# TODO: use non-lazy mount
function setup-ext4() {
	DEV_NAME="/dev/$SSD_NAME"
	# check if already mount; if yes, umount first and print warning
	if sudo grep -qF "$DEV_NAME $KFS_MOUNT_PATH" /proc/mounts ; then
		echo "WARN: Detect $DEV_NAME has already mounted on $KFS_MOUNT_PATH"
		echo "      Will umount first before setup ext4"
		sudo umount "$KFS_MOUNT_PATH"
	fi
	sudo mkfs -F -t ext4 "$DEV_NAME"
	sudo mount "$DEV_NAME" "$KFS_MOUNT_PATH"
}

function reset-ext4() {
	sudo umount "$KFS_MOUNT_PATH"
}

function cleanup-ufs-config() {
	sudo rm -rf /tmp/spdk.conf
	sudo rm -rf /tmp/fsp.conf
}
