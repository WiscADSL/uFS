#! /usr/bin/env python3
import sys
import csv


def print_usage_and_exit():
    print(f"Usage: {sys.argv[0]} <ufs|ext4> <data_dir>")
    exit(1)


if len(sys.argv) != 3:
    print_usage_and_exit()

fs_type = sys.argv[1]
data_dir = sys.argv[2]

if fs_type != "ufs" and fs_type != "ext4":
    print_usage_and_exit()


class AverageCounter:
    def __init__(self):
        self.value = 0
        self.count = 0

    def AddValue(self, value, count=1):
        assert value >= 0
        self.value += value * count
        self.count += count

    def Average(self):
        if self.count == 0:
            return 0
        return self.value / self.count

    def Clear(self):
        self.value = 0
        self.count = 0


def collect_op_data(source, op_name, dst):
    if op_name in source[0]:
        ops = int(source[1][:-3])
        lat = float(source[4][:-5])
        dst.AddValue(lat, ops)


def get_header():
    return [
        "num_app", "iops", "l3_hit_ratio", "open_latency", "read_latency",
        "close_latency", "append_latency", "fsync_latency", "create_latency",
        "delete_latency"
    ]


# return a list of data
def parse_one_log(log_name):
    open_counter = AverageCounter()
    read_counter = AverageCounter()
    close_counter = AverageCounter()
    append_counter = AverageCounter()
    fsync_counter = AverageCounter()
    create_counter = AverageCounter()
    delete_counter = AverageCounter()
    iops = None
    l3_hit_rate = None

    with open(log_name, "rt") as f:
        for line in f.readlines():
            split = line.split()
            if len(split) >= 7:
                collect_op_data(split, "openfile", open_counter)
                collect_op_data(split, "readfile", read_counter)
                collect_op_data(split, "closefile", close_counter)
                collect_op_data(split, "appendfilerand", append_counter)
                collect_op_data(split, "fsyncfile", fsync_counter)
                collect_op_data(split, "createfile", create_counter)
                collect_op_data(split, "deletefile", delete_counter)
                if split[2] == "Summary:":
                    iops = float(split[5])
                if split[0] == "L3" and split[1] == "Hit:":
                    l3_hit_rate = float(split[6])

    return [
        iops,
        l3_hit_rate,
        open_counter.Average(),
        read_counter.Average(),
        close_counter.Average(),
        append_counter.Average(),
        fsync_counter.Average(),
        create_counter.Average(),
        delete_counter.Average(),
    ]


# collect data of a fixed number of uFS workers/ext4 under varying number of
# apps, which corresponds to one curve in the final figure
def collect_data_for_one_config(num_worker, csv_name):
    results = []
    for num_app in range(1, 11):
        if num_worker is None:  # for ext4
            log_name = f"{data_dir}/num-app-{num_app}_ext4_filebench.out"
        else:
            log_name = f"{data_dir}/num-app-{num_app}_ufs-{num_worker}_filebench.out"
        line = [num_app]
        line.extend(parse_one_log(log_name))
        results.append(line)

    with open(csv_name, "wt") as f:
        f_csv = csv.writer(f)
        f_csv.writerow(get_header())
        f_csv.writerows(results)


if fs_type == "ufs":
    for num_worker in range(1, 11):
        collect_data_for_one_config(num_worker,
                                    f"{data_dir}/ufs-{num_worker}_varmail.csv")
elif fs_type == "ext4":
    collect_data_for_one_config(None, f"{data_dir}/ext4_varmail.csv")
