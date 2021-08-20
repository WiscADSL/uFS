#!/usr/bin/env python3
# encoding: utf-8

from sarge import run, Capture
from io import TextIOWrapper
import string
import random
import os
import sys


def gen_src_data(bytes, fname=None):
    res = ''.join(random.choices(string.ascii_uppercase +
                                 string.digits, k=bytes))
    if fname is not None:
        with open(fname, 'w+') as f:
            f.write(res)
    return res


def gen_fix_data(bytes):
    to_expand = 'abcdefghijklmnopqrstuvwxyz'
    res = (to_expand * (int(bytes / len(to_expand)) + 1))[:bytes]
    return res


# NOTE: be sure to call mkfs before invoke this test case
# Besides, need to start FSP before invoke this.
def test_1():
    bytes = 35548
    # src_data = gen_src_data(bytes)
    src_data = gen_fix_data(bytes)
    print(len(src_data))
    cmd_fname = 'test_1.cmd'
    cmd_template = 'open t2\n' \
                   'write 10 {}\n' \
                   'pread 10 {} 0'. \
        format(src_data, bytes)
    with open(cmd_fname, 'w+') as f:
        f.write(cmd_template)
        f.flush()
        f.close()

    cmd_app_bin = '@CMAKE_CURRENT_BINARY_DIR@/testAppCmd'
    if not os.path.exists(cmd_app_bin):
        print('{} does not exist', cmd_app_bin)
        sys.exit(1)

    cur_app_cmd = '{} {} {}'.format(cmd_app_bin, 1, cmd_fname)
    p_app = run(cur_app_cmd, stdout=Capture())
    if p_app.stdout is not None:
        # print(p_app.stdout.text)
        for line in TextIOWrapper(p_app.stdout):
            if 'DATA:' in line:
                can_match = True
                rb_data = line[5:]
                if len(rb_data) != bytes:
                    can_match = False
                    print('read back len not match {}', len(rb_data))
                else:
                    for i in range(len(rb_data)):
                        if rb_data[i] != src_data[i]:
                            can_match = False
                            print('{} th char not match rb:{} src:{}'.format(
                                i, rb_data[i], src_data[i]))
                if can_match:
                    print('the read back data and written data is matched')


def main():
    test_1()
    pass


if __name__ == '__main__':
    main()
