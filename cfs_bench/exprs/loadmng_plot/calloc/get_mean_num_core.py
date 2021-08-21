#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Get the number of cores of one fsp log.

Q: how to deal with the tail for lazy join-all?
- I think it should be counted

"""

import sys


def get_attr_from_item(item, attr_name, deli, tp):
    return tp(item[len(attr_name + deli):])


def compute_mean_num_core(nano_core_num_dict):
    nano_list = sorted(nano_core_num_dict.keys())
    first_nano = nano_list[0]
    last_nano = nano_list[-1]
    total_nano = last_nano - first_nano
    agg_mean = 0
    for i in range(1, len(nano_list)):
        last_nano = nano_list[i - 1]
        cur_nano = nano_list[i]
        nano_diff = cur_nano - last_nano
        cur_pct = nano_diff / total_nano
        agg_mean += cur_pct * nano_core_num_dict[last_nano]
    return agg_mean


def test_compute_mean_num_core():
    nano_core_num_dict = {
        0: 4,
        2: 3,
        4: 3,
        6: 2,
        8: 2,
        10: 1,
        12: 1,
        14: 2,
        16: 2,
        18: 0
    }
    nc = compute_mean_num_core(nano_core_num_dict)
    print(nc)


def process_log(fsp_log):
    with open(fsp_log) as f:
        first_nano = None
        cur_nano = None
        nano_core_delta_dict = {}
        nano_core_num_dict = {}
        for line in f:
            line = line.strip()
            items = line.split()
            if '[KPLoadStatsSummary]' in line and 'real_nano:' in line:
                pass
            if 'invalidateAppShmByName' in line:
                break
            if 'Stop file system process' in line:
                break
            try:
                if 'comb_reset_ts' in line and 'recv_ns_ql' in line:
                    nano_item = items[2]
                    nano = get_attr_from_item(nano_item, 'real_nano', ':', int)
                    if first_nano is None:
                        first_nano = nano
                        nano_core_delta_dict[first_nano] = 1
                    cur_nano = nano
                if 'exec lb_nminus_plan' in line:
                    assert cur_nano is not None
                if 'wid:' in line and ' activated' in line and 'localvid' in line:
                    assert cur_nano is not None
                    nano_core_delta_dict[cur_nano] = 1
                if 'deactivate' in line:
                    nano_item = items[4]
                    deactive_nano = get_attr_from_item(nano_item, 'nano', ':',
                                                       int)
                    nano_core_delta_dict[deactive_nano] = -1
            except:
                print('WRONG line'.format(line))
        nano_list = sorted(nano_core_delta_dict)
        assert (len(nano_list) > 1)
        for i in range(len(nano_list)):
            if i == 0:
                last = 0
            else:
                last = nano_core_num_dict[nano_list[i - 1]]
            nano_core_num_dict[nano_list[i]] = last + \
                nano_core_delta_dict[nano_list[i]]
        # print(nano_core_delta_dict)
        print(nano_core_num_dict)
    cur_mean_num_core = compute_mean_num_core(nano_core_num_dict)
    print('mean_nc:{}'.format(cur_mean_num_core))
    return cur_mean_num_core


def print_usage(argv):
    print('Usage: {} <dir_name|test>'.format(argv[0]))


def main(argv):
    if len(argv) != 2:
        print_usage()
        exit(1)
    if argv[1] == 'test':
        test_compute_mean_num_core()
    else:
        log_name = '{}/fsp_log'.format(argv[1])
        cur_mean = process_log(log_name)
        out_name = '{}/mean_num_core'.format(argv[1])
        with open(out_name, 'w') as f:
            f.write(str(cur_mean))


if __name__ == '__main__':
    main(sys.argv)
    pass
