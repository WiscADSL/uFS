#! /usr/bin/env python3

import sys
import pandas as pd

assert(len(sys.argv) == 2)

fname = sys.argv[1]


class Treatment(object):
    def __init__(self, cmd):
        self.parse_cmd(cmd)

    def parse_cmd(self, cmd):
        items = cmd.split()
        self.qd = int(items[2])
        self.size = int(items[4])
        self.num_threads = len(eval(items[10]))

    def add_total_line(self, line):
        assert(self.iops is None)
        items = line.split()
        self.iops = float(items[1])
        self.mbps = float(items[2])
        self.avg_ltc = float(items[3])
        self.min_ltc = float(items[4])
        self.max_ltc = float(items[5])

    def to_column_names():
        return [
            'qd',
            'size',
            'num_threads',
            'iops',
            'mbps',
            'avg_ltc',
            'min_ltc',
            'max_ltc']

    def get_column_attrs(self):
        return [self.__dict__[name] for name in to_column_names()]


with open(fname) as f:
    treatobj = None
    for line in f:
        line = line.strip()
        if 'perf -q' in line:
            treatobj = Treatmemt(line)
        if 'Total' in line:
            treatobj.add_total_line(line)
            print(self.get_column_attrs())
            break
