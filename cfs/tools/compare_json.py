#! /usr/bin/env python3

import json
from jsondiff import diff
import sys

f0 = open(sys.argv[1])
f1 = open(sys.argv[2])

for line_num, (line0, line1) in enumerate(zip(f0, f1)):
    j0 = json.loads(line0)
    j1 = json.loads(line1)
    #print(diff(j0, j1))
    cur_diff = diff(j0, j1, syntax='explicit')
    if len(cur_diff) > 0:
        print('line number:{}'.format(line_num))
        print(cur_diff)
        # print(diff(j1,j0))
        # print(j0)
        # print(j1)

#j0 = json.load(f0)
#j1 = json.load(f1)
