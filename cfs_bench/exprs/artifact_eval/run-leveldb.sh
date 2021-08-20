echo "TODO: implement run-leveldb..."

function print_usage_and_exit() {
	echo "Usage: $0 [ fillseq | fillrand | ycsb-a | ycsb-b | ycsb-c | ycsb-d | ycsb-e | ycsb-f ] [ ufs | ext4 ]"
	echo "  Specify which LevelDB workload to run on which filesystem"
	echo "    workload: 8 workloads available (shown above)"
	echo "    ufs:  run uFS for given workload from 1 app to 10 apps"
	echo "    ext4: run ext4 for given workload from 1 app to 10 apps"
	exit 1
}

if [ ! $# = "2" ]; then print_usage_and_exit; fi
if [ ! "$1" = "fillseq" ] && [ ! "$1" = "fillrand" ] && \
	[ ! "$1" = "ycsb-a" ] && [ ! "$1" = "ycsb-b" ] && \
	[ ! "$1" = "ycsb-c" ] && [ ! "$1" = "ycsb-d" ] && \
	[ ! "$1" = "ycsb-e" ] && [ ! "$1" = "ycsb-f" ]
then print_usage_and_exit; fi
if [ ! "$2" = "ufs" ] && [ ! "$2" = "ext4" ]; then print_usage_and_exit; fi
