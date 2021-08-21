#!/usr/bin/env python3
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""plot number of inodes when migrating.

"""

import sys

from bokeh.io import export_png
from bokeh.plotting import figure, output_file, show
from bokeh.palettes import Category10, Oranges, Greens, Blues, Reds, BuGn, BuPu, Greys, PuBu, PuRd, Purples, YlOrBr


def print_usage(argv):
    print('Usage {} <dir_name> <num_worker>'.format(argv[0]))


if len(sys.argv) < 3:
    print_usage(sys.argv)
    exit(1)

dir_name = sys.argv[1]
num_worker = int(sys.argv[2])

bg_color_list = [
    Oranges, Greens, Blues, Reds, BuGn, BuPu, Greys, PuBu, PuRd, Purples,
    YlOrBr
]

FONT_SIZE = "12pt"


def get_attr_from_item(item, attr_name, deli, tp):
    return tp(item[len(attr_name + deli):])


def process_fsp_log(fname, ts_log_name):
    first_nano = None
    nano_num_migration_dict = {}

    with open(ts_log_name) as f:
        for line in f:
            line = line.strip()
            items = line.split()
            if 'real_nano' in line:
                nano_item = items[2]
                nano = int(nano_item[len('real_nano:'):])
                if first_nano is None:
                    first_nano = nano
                    break
    print('first_nano:{}'.format(first_nano))

    kNanoStep = int(50 * 1024 * 1024)  # 50 ms

    with open(fname) as f:
        for line in f:
            line = line.strip()
            items = line.split()
            if 'LbPlan' in line:
                continue
            if 'nano:' in line and 'pid:' in line and 'from' in line:
                if line[0:4] != 'nano':
                    continue
                print(line)
                nano_item = items[0]
                nano = int(nano_item[len('nano:'):])
                cur_key = (nano - first_nano)
                if cur_key not in nano_num_migration_dict:
                    nano_num_migration_dict[cur_key] = 0
                nano_num_migration_dict[cur_key] += 1
            if 'kLm_JoinAllAck num_join' in line:
                if line[0:2] != 'kL':
                    continue
                nano_item = items[-1]
                nano = int(nano_item[len('nano:'):])
                num_join_item = items[1]
                num_join = get_attr_from_item(num_join_item, 'num_join', ':',
                                              int)
                if num_join <= 0:
                    continue
                cur_key = (nano - first_nano)
                if cur_key not in nano_num_migration_dict:
                    nano_num_migration_dict[cur_key] = 0
                print('num_join:{}'.format(num_join))
                nano_num_migration_dict[cur_key] += num_join
        # done with line iteration
        begin_nano = min(nano_num_migration_dict.keys())
        print('begin_nano:{}'.format(begin_nano))
        xy_dict = {}
        for nano, num in nano_num_migration_dict.items():
            idx = (nano) // kNanoStep
            cur_norm_nano = int(idx * kNanoStep)
            if cur_norm_nano not in xy_dict:
                xy_dict[cur_norm_nano] = 0
            xy_dict[cur_norm_nano] += num
        xs = list(xy_dict.keys())
        ys = list(xy_dict.values())
        print(xs)
        print(ys)
        with open('{}/num_inodes_move'.format(dir_name), 'w') as f:
            xs_str = [str(_x) for _x in xs]
            ys_str = [str(_y) for _y in ys]
            f.write(','.join(xs_str))
            f.write('\n')
            f.write(','.join(ys_str))

        cur_title_name = 'num_inodes_move'
        p = figure(title=cur_title_name, plot_width=1200, plot_height=800)
        p.toolbar.logo = None
        p.toolbar_location = None
        p.xaxis.axis_label = 'Time (sec)'
        p.yaxis.axis_label = 'Number of Inodes Moved'
        p.title.text_font_size = FONT_SIZE
        for cur_axis in [p.xaxis, p.yaxis]:
            cur_axis.axis_label_text_font_size = FONT_SIZE
            cur_axis.major_label_text_font_size = FONT_SIZE
        xs = [1e-9 * x for x in xs]
        p.circle(x=list(xs),
                 y=list(ys),
                 size=6,
                 alpha=0.8,
                 color=Category10[10][0])
        p.line(x=xs,
               y=ys,
               color=Category10[10][0],
               line_width=2,
               alpha=0.5,
               line_dash='dotted')
        # p.vbar(x=xs, top=ys, width=0.05)
        p.x_range.start = 0
        p.y_range.start = 0
        # p.x_range.end = 12

        # export_png(p, filename='{}/{}.png'.format(dir_name, cur_title_name))


fsp_log_name = '{}/{}'.format(dir_name, 'err_log')
ts_log = '{}/{}'.format(dir_name, 'fsp_log')
process_fsp_log(fsp_log_name, ts_log)
