#! /usr/bin/env python3

# This script merges multiple csv files into a z-plot friendly file

import sys
import csv


def print_usage_and_exit():
    print(f"Usage: {sys.argv[0]} <ufs_data_dir> <ext4_data_dir>", file=sys.stderr)
    exit(1)


if len(sys.argv) != 3:
    print_usage_and_exit()

ufs_data_dir = sys.argv[1]
ext4_data_dir = sys.argv[2]

fs_types = ["ext4"] + [f"ufs_{cache_ratio}" for cache_ratio in [0, 50, 75, 100]]

# the plot use "clients" instead of "apps", so we switch name here...
header = ["#", "num_client"] + fs_types

# map (fs_type, num_app) to iops
iops_map = {}

with open(f"{ext4_data_dir}/ext4_webserver.csv", "rt") as f:
    f_csv = csv.reader(f)
    for line_num, line in enumerate(f_csv):
        if line_num == 0:
            assert line[0] == "num_app" and line[1] == "iops"
            continue
        num_app, iops = int(line[0]), float(line[1])
        assert num_app == line_num
        iops_map[("ext4", num_app)] = iops

for cache_ratio in [0, 50, 75, 100]:
    with open(f"{ufs_data_dir}/ufs-cache-hit-{cache_ratio}_webserver.csv", "rt") as f:
        f_csv = csv.reader(f)
        for line_num, line in enumerate(f_csv):
            if line_num == 0:
                assert line[0] == "num_app" and line[1] == "iops"
                continue
            num_app, iops = int(line[0]), float(line[1])
            assert num_app == line_num
            iops_map[(f"ufs_{cache_ratio}", num_app)] = iops

print("\t".join(header))
for num_app in range(1, 11):
    line = [f"{num_app}"]
    for fs_type in fs_types:
        line.append(str(iops_map[(fs_type, num_app)]))
    print("\t".join(line))
