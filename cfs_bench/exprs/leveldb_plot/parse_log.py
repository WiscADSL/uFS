#! /usr/bin/env python3
import sys


def print_usage_and_exit():
    print(f"Usage: {sys.argv[0]} <workload> <ufs_data_dir> <ext4_data_dir>")
    exit(1)


if len(sys.argv) != 4:
    print_usage_and_exit()

workload = sys.argv[1]
ufs_data_dir = sys.argv[2]
ext4_data_dir = sys.argv[3]
valid_workloads = [f"ycsb-{name}" for name in ["a", "b", "c", "d", "e", "f"]]

NUM_OPS = 100000 if "fill" not in workload \
    else 10000000 if "random" not in workload else 2000000

if workload not in valid_workloads:
    print_usage_and_exit()


def collect_data(data_dir):
    tp_list = []
    for num_app in range(1, 11):
        tp_sum = 0
        for appid in range(1, num_app + 1):
            with open(
                    f"{data_dir}/{workload}_num-app-{num_app}_leveldb/leveldb-{appid}.out",
                    "r") as f:
                for line in f:
                    split = line.split()
                    if len(split
                           ) >= 3 and split[0] == "Timer" and split[1] == "0:":
                        latency = int(split[2])
                        tp_sum += NUM_OPS / latency * 1000000
        tp_list.append(tp_sum)
    return tp_list


ufs_tp = collect_data(ufs_data_dir)
ext4_tp = collect_data(ext4_data_dir)

print("# num_client\tuFS\t\text4")

for i in range(0, 10):
    print(f"{i+1}\t\t{ufs_tp[i]:.2f}\t{ext4_tp[i]:.2f}")
