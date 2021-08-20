#! /bin/bash
# This is the main script for artifact evaluation
# Do not run as root
set -e

if [ $(id -u) = "0" ]; then
	echo "It is NOT recommended to run this script as root!"
	echo "Permission will be asked on demand"
fi

if [ ! "$1" = "init" ] && [ ! "$1" = "init-after-reboot" ] && [ ! "$1" = "cmpl" ] && [ ! "$1" = "run" ] && [ ! "$1" = "plot" ]; then
	echo "Usage: ae [ init | init-after-reboot | cmpl | run | plot ] [...]"
	echo "  Specify which step to perform"
	echo "    init: initialize environment, install dependency (must reboot after init)"
	echo "    init-after-reboot: further initialize environment (must be done after every rebooting)"
	echo "    cmpl: compile codebase for a specific benchmark"
	echo "    run:  run a specific benchmark"
	echo "    plot: make a plot from the collected data"
	exit 1
fi

# some parameters (could be overwritten)
# a env var that will be exported should have a prefix "AE_" to avoid conflicts
## source code repository
if [ -z "$AE_REPO_URL" ]; then
	export AE_REPO_URL='https://github.com/WiscADSL/uFS.git'
fi
if [ -z "$AE_BRANCH" ]; then
	export AE_BRANCH='main'
fi
## benchmark code repository
if [ -z "$AE_BENCH_REPO_URL" ]; then
	export AE_BENCH_REPO_URL='https://github.com/WiscADSL/uFS-bench.git'
fi
if [ -z "$AE_BENCH_BRANCH" ]; then
	export AE_BENCH_BRANCH='main'
fi

## workspace
export AE_WORK_DIR="$HOME/workspace"
export AE_REPO_DIR="$AE_WORK_DIR/uFS"
export AE_BENCH_REPO_DIR="$AE_WORK_DIR/uFS-bench"
export AE_SCRIPT_DIR="$AE_REPO_DIR/cfs_bench/exprs/artifact_eval"
## number of threads to compile
export AE_CMPL_THREADS="15"  # avoid too many threads causing OOM
## top level directory for data management
export AE_DATA_DIR="$PWD/AE_DATA"

# add a line to a config file only if this line does not present yet
# useful to make the script idempotent
function add-if-not-exist() {
	config_line=$1
	config_file=$2
	if ! sudo grep -qF "$config_line" "$config_file" ; then 
		echo "$config_line" | sudo tee -a "$config_file"
	fi
}

function ae-init-mount() {
	echo "Mount: start..."

	# If on CloudLab c6525-100g machines, use /dev/nvme0n1p4
	DEV_NAME=/dev/nvme0n1p4

	mkdir -p $AE_WORK_DIR
	sudo mkfs -t ext4 $DEV_NAME
	sudo mkdir -p $AE_WORK_DIR
	sudo mount $DEV_NAME $AE_WORK_DIR
	sudo chown -R $USER $AE_WORK_DIR
	sudo chmod 775 -R $AE_WORK_DIR

	add-if-not-exist "$DEV_NAME        $AE_WORK_DIR        ext4    defaults        0       0" /etc/fstab

	echo "Mount: DONE!"
	touch ~/.ae_mount_done
}

function ae-init-install() {
	echo "Install: start..."
	touch ~/.ae_env.sh

	# basic tools
	sudo apt-get update
	sudo apt-get -y install htop shellcheck valgrind cpufrequtils cloc

	# tmux
	sudo apt-get -y install tmux
	add-if-not-exist "export EDITOR='/usr/bin/vim'" ~/.ae_env.sh

	# cmake
	sudo apt-get -y install cmake cmake-curses-gui

	# formatters
	# cpp
	sudo apt-get -y install clang-format
	add-if-not-exist "alias cfgg='clang-format -i -style=Google'" ~/.ae_env.sh

	# python
	# may need proxy if using ADSL machine (protected by CS department network)
	# set PIP_PROXY='--proxy xxx'
	# otherwise, just leave it black
	sudo pip3 ${PIP_PROXY} install autopep8
	add-if-not-exist "alias pyfmt='autopep8 --in-place --aggressive --aggressive'" ~/.ae_env.sh

	# change file limits for vscode
	add-if-not-exist 'fs.inotify.max_user_watches=524288' /etc/sysctl.conf

	# python lib to run exprs and plotting
	sudo pip3 ${PIP_PROXY} install sarge psutil numerize pandas
	# must use a customized version of z-plot with csv support
	git clone https://github.com/jingliu9/z-plot.git
	sudo pip3 install ./z-plot; sudo rm -rf ./z-plot

	# Some useful shell command:
	add-if-not-exist "alias ae='bash $AE_SCRIPT_DIR/artifact_eval.sh'" ~/.ae_env.sh

	# env vars required by benchmark scripts
	if [ "$1" = "cloudlab" ]; then
		add-if-not-exist 'export SSD_NAME="nvme1n1"' ~/.ae_env.sh
	else
		add-if-not-exist 'export SSD_NAME="nvme0n1"' ~/.ae_env.sh
	fi
	add-if-not-exist 'export KFS_MOUNT_PATH="/ssd-data"' ~/.ae_env.sh
	add-if-not-exist 'export KFS_DATA_DIR="${KFS_MOUNT_PATH}/bench"' ~/.ae_env.sh
	add-if-not-exist 'export CFS_ROOT_DIR="${HOME}/workspace/uFS"' ~/.ae_env.sh
	add-if-not-exist 'export SPDK_SRC_DIR="${CFS_ROOT_DIR}/cfs/lib/spdk"' ~/.ae_env.sh
	add-if-not-exist 'export MKFS_SPDK_BIN="${CFS_ROOT_DIR}/cfs/build/test/fsproc/testRWFsUtil"' ~/.ae_env.sh
	add-if-not-exist 'export MKFS_POSIX_DEV_BIN="${CFS_ROOT_DIR}/cfs/build/test/fsproc/testRWFsUtilPosix"' ~/.ae_env.sh
	add-if-not-exist 'export CFS_MKFS_BIN_NAME="${MKFS_SPDK_BIN}"' ~/.ae_env.sh
	add-if-not-exist 'export CFS_MAIN_BIN_NAME="${CFS_ROOT_DIR}/cfs/build/fsMain"' ~/.ae_env.sh

	# Ensure necessary path is okay...
	sudo mkdir -p /ssd-data/bench

	if [ -f ~/.bashrc ]; then
		add-if-not-exist 'source ~/.ae_env.sh' ~/.bashrc
	fi
	if [ -f ~/.zshrc ]; then
		add-if-not-exist 'source ~/.ae_env.sh' ~/.zshrc
	fi
	source ~/.ae_env.sh

	sudo sysctl -p

	# Then build necessary dependencies
	set +e  # allow non-zero return value
	## Init submodules
	cd $AE_REPO_DIR
	git submodule update --init

	## Build folly
	cd $AE_REPO_DIR/cfs/lib
	bash ../tools/folly_install.sh

	## Build fio
	cd $AE_REPO_DIR/cfs/lib
	if [ ! -d $AE_REPO_DIR/cfs/lib/fio ]; then
		git clone https://github.com/axboe/fio
	fi
	cd fio
	make -j $AE_CMPL_THREADS

	## Build spdk
	cd $AE_REPO_DIR/cfs/lib/spdk
	sudo scripts/pkgdep.sh
	./configure --with-fio=$AE_REPO_DIR/cfs/lib/fio
	make -j $AE_CMPL_THREADS
	make -f Makefile.sharedlib

	## Build tbb
	cd $AE_REPO_DIR/cfs/lib/tbb
	make -j $AE_CMPL_THREADS
	### this directory name may vary in different machines, so create a symbolic link for uniform access
	cd build
	ln -s linux_*_release tbb_build_release

	## Build config4cpp
	cd $AE_REPO_DIR/cfs/lib/config4cpp
	make -j $AE_CMPL_THREADS

	set -e

	echo "Install: DONE!"
	touch ~/.ae_install_done
}

function ae-init-config() {
	echo "Config: start..."

	# Ensure memory limit won't stop SPDK
	add-if-not-exist "$USER hard memlock unlimited" /etc/security/limits.conf
	add-if-not-exist "$USER soft memlock unlimited" /etc/security/limits.conf
	add-if-not-exist "$USER hard nofile 1048576" /etc/security/limits.conf
	add-if-not-exist "$USER soft nofile 1048576" /etc/security/limits.conf

	echo "Config: DONE!"
	touch ~/.ae_config_done
}

function ae-init() {
	if [ ! "$1" = "cloudlab" ] && [ ! "$1" = "adsl" ]; then
		echo "Usage: ae init [ cloudlab | adsl ]"
		echo "  Specify which machine this script is running on:"
		echo "    cloudlab: a machine of hardware type c6525-100g in CloudLab"
		echo "    adsl:     a machine managed by ADSL (have some environment prepared already)"
		exit 1
	fi

	echo "=== Welcome to the artifact evaluation of uFS! ==="
	echo "Init: start..."

	# if not mount
	if [ ! -f ~/.ae_mount_done ] && [ "$1" = "cloudlab" ]; then
		ae-init-mount "$@"
	else
		echo "Detect mount has been done; skip..."
	fi

	# Then download codebase to the workspace if the codebase is not found
	if [ ! -d "$AE_REPO_DIR" ]; then
		echo "uFS repository is not detected, start downloading..."
		mkdir -p "$AE_WORK_DIR"; cd "$AE_WORK_DIR"
		git clone "$AE_REPO_URL"
		echo "Download uFS repository finish"
	fi
	if [ ! -d "$AE_BENCH_REPO_DIR" ]; then
		echo "uFS benchmark repository is not detected, start downloading..."
		mkdir -p "$AE_WORK_DIR"; cd "$AE_WORK_DIR"
		git clone "$AE_BENCH_REPO_URL"
		echo "Download uFS benchmark repository finish"
	fi

	# Ensure we are in a correct working directory
	cd "$AE_REPO_DIR"
	git checkout "$AE_BRANCH"

	cd "$AE_BENCH_REPO_DIR"
	git checkout "$AE_BENCH_BRANCH"

	# if not install
	if [ ! -f ~/.ae_install_done ]; then
		ae-init-install "$@"
	else
		echo "Detect install has been done; skip..."
	fi

	# if not config
	if [ ! -f ~/.ae_config_done ]; then
		ae-init-config "$@"
	else
		echo "Detect config has been done; skip..."
	fi

	echo "Init: DONE!"
	echo "Please reboot the machine for some configurations to take effect"
}

function ae-init-after-reboot {
	if [ ! "$1" = "cloudlab" ] && [ ! "$1" = "adsl" ]; then
		echo "Usage: ae init-after-reboot [ cloudlab | adsl ]"
		echo "  Specify which machine this script is running on:"
		echo "    cloudlab: a machine of hardware type c6525-100g in CloudLab"
		echo "    adsl:     a machine managed by ADSL (have some environment prepared already)"
		exit 1
	fi

	# disable hyperthreading
	echo off | sudo tee /sys/devices/system/cpu/smt/control
	# for reading cpu performance counter
	sudo modprobe msr
	sudo sysctl kernel.nmi_watchdog=0

	# reserve hugepage memory for SPDK
	sudo -E python3 $AE_REPO_DIR/cfs_bench/exprs/fsp_microbench_suite.py --fs fsp --devonly

	set +e
	if [ "$1" = "cloudlab" ]; then
		echo "TODO: disable CPU scaling on CloudLab machines..."
	else  # ADSL machines
		TARGET_FREQ="2900000"
		for x in /sys/devices/system/cpu/*/cpufreq/
		do
			# NOTE: This will report error, but while verifying via `cat`, it has its effect there.
			echo "$TARGET_FREQ" | sudo tee "$x/scaling_max_freq"
		done
	fi
	set -e
}

function ae-cmpl() {
	if [ ! "$1" = "microbench" ] && [ ! "$1" = "filebench" ] && [ ! "$1" = "loadmng" ] && [ ! "$1" = "leveldb" ]; then
		echo "Usage: ae cmpl [ microbench | filebench | loadmng | leveldb ]"
		echo "  Specify which benchmark to compile:"
		echo "    microbench: microbenchmark with 32 workload (fig. 5 and 6 in paper)"
		echo "    filebench:  Varmail and Webserver worload in filebench (fig. 8)"
		echo "    loadmng:    Load Management benchmark (fig. 9, 10, and 11)"
		echo "    leveldb:    LevelDB on YCSB workload (fig. 12)"
		exit 1
	fi

	echo "Cmpl: Start..."
	if [ "$1" = "microbench" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/cmpl-microbench.sh "${@:2}"
		ret=$?
	elif [ "$1" = "filebench" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/cmpl-filebench.sh "${@:2}"
		ret=$?
	elif [ "$1" = "loadmng" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/cmpl-loadmng.sh "${@:2}"
		ret=$?
	elif [ "$1" = "leveldb" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/cmpl-leveldb.sh "${@:2}"
		ret=$?
	fi

	if [ "$ret" = "0" ]; then
		echo "Cmpl: DONE!"
	else
		echo "Cmpl: Fail!"
		exit 1
	fi
}

function ae-run() {
	if [ ! "$1" = "microbench" ] && [ ! "$1" = "filebench" ] && [ ! "$1" = "loadmng" ] && [ ! "$1" = "leveldb" ]; then
		echo "Usage: ae run [ microbench | filebench | loadmng | leveldb ]"
		echo "  Specify which benchmark to compile:"
		echo "    microbench: microbenchmark with 32 workload (fig. 5 and 6 in paper)"
		echo "    filebench:  Varmail and Webserver worload in filebench (fig. 8)"
		echo "    loadmng:    Load Management benchmark (fig. 9, 10, and 11)"
		echo "    leveldb:    LevelDB on YCSB workload (fig. 12)"
		exit 1
	fi

	# Create an top level data directory that points to other data directory
	mkdir -p $AE_DATA_DIR
	# Every run script should link its latest data into $AE_DATA_DIR

	## Ensure no processes from the last round left
	echo "Perform some pre-run cleaning: it may report some errors for files/processes not found, but it should be fine..."
	set +e  # allow non-zero return value for file/process not found
	sudo killall fsMain
	sudo killall cfs_bench_coordinator
	sudo rm -rf /ufs-*
	sudo rm -rf /dev/shm/coordinator
	set -e

	if [ "$1" = "microbench" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/run-microbench.sh "${@:2}"
	elif [ "$1" = "filebench" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/run-filebench.sh "${@:2}"
	elif [ "$1" = "loadmng" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/run-loadmng.sh "${@:2}"
	elif [ "$1" = "leveldb" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/run-leveldb.sh "${@:2}"
	fi

	if [ "$?" = "0" ]; then
		echo "Run: DONE!"
	else
		echo "Run: Fail!"
		exit 1
	fi
}

function ae-plot() {
	if [ ! "$1" = "microbench" ] && [ ! "$1" = "filebench" ] && [ ! "$1" = "loadmng" ] && [ ! "$1" = "leveldb" ]; then
		echo "Usage: ae plot [ microbench | filebench | loadmng | leveldb ]"
		echo "  Specify which benchmark to parse output and plot:"
		echo "    microbench: microbenchmark with 32 workload (fig. 5 and 6 in paper)"
		echo "    filebench:  Varmail and Webserver worload in filebench (fig. 8)"
		echo "    loadmng:    Load Management benchmark (fig. 9, 10, and 11)"
		echo "    leveldb:    LevelDB on YCSB workload (fig. 12)"
		exit 1
	fi

	if [ ! -d "$AE_DATA_DIR" ]; then
		echo 'AE_DATA not found!'
		echo '`ae plot` reads from ./AE_DATA, which is created by `ae run`'
		echo 'Make sure run the experiement before plotting'
		exit 1
	fi

	if [ "$1" = "microbench" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/plot-microbench.sh "${@:2}"
	elif [ "$1" = "filebench" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/plot-filebench.sh "${@:2}"
	elif [ "$1" = "loadmng" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/plot-loadmng.sh "${@:2}"
	elif [ "$1" = "leveldb" ]; then
		bash $AE_REPO_DIR/cfs_bench/exprs/artifact_eval/plot-leveldb.sh "${@:2}"
	fi
}

if [ "$1" = "init" ]; then
	ae-init "${@:2}"
elif [ "$1" = "init-after-reboot" ]; then
	ae-init-after-reboot "${@:2}"
elif [ "$1" = "cmpl" ]; then
	ae-cmpl "${@:2}"
elif [ "$1" = "run" ]; then
	ae-run "${@:2}"
elif [ "$1" = "plot" ]; then
	ae-plot "${@:2}"
fi
