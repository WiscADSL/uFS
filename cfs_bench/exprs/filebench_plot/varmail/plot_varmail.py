#! /usr/bin/env python3

# Note that, for readability purposes, we only plot 1-4 threads for uFS.
# Empirically, we observe uFS-5 to uFS-10 perform similar to uFS-4, which causes
# the curve overlapping with each other and make it difficult to read.

import sys
from zplot import *


def print_usage_and_exit():
    print(f"Usage: {sys.argv[0]} <plot_name> <varmail_data_path>")
    exit(1)


if len(sys.argv) != 3:
    print_usage_and_exit()

plot_name = sys.argv[1]
varmail_data_path = sys.argv[2]

DRAWABLE_X_LEN = 200
DRAWABLE_Y_LEN = 100
DRAWABLE_COORD_X = 20
DRAWABLE_COORD_Y = 22

YLABEL_SHIFT = [0, 7]

XTITLE_SHIFT = [0, 2]
YTITLE_SHIFT = [12, 0]

LEGEND_BASE = [30, 120]
LEGEND_X_OFFSET = 40
LEGEND_Y_OFFSET = 10
LEGEND_EACH_ROW = 2
LEGEND_FONT_SZ = 6

TITLE_FONT_SZ = 6
LABEL_FONT_SZ = 6

LINE_WIDTH = 1
LINES = [
    "ufs_1",
    "ufs_2",
    "ufs_3",
    "ufs_4",
    # "ufs_5", "ufs_6", "ufs_7", "ufs_8",
    # "ufs_9", "ufs_10",
    "ext4"
]

LINES_COLOR_MAP = {
    "ufs_1": "0.85,0.85,0.85",
    "ufs_2": "0.6,0.6,0.6",
    "ufs_3": "0.4,0.4,0.4",
    "ufs_4": "0,0,0",
    # "ufs_5": "green",
    # "ufs_6": "greenyellow",
    # "ufs_7": "pink",
    # "ufs_8": "orange",
    # "ufs_9": "tomato",
    # "ufs_10": "red",
    "ext4": "black"
}

LINES_DASH_MAP = {
    "ufs_1": 0,
    "ufs_2": 0,
    "ufs_3": 0,
    "ufs_4": 0,
    # "ufs_5": "green",
    # "ufs_6": "greenyellow",
    # "ufs_7": "pink",
    # "ufs_8": "orange",
    # "ufs_9": "tomato",
    # "ufs_10": "red",
    "ext4": [4, 1.6]
}

LINES_NAME_MAP = {
    "ufs_1": "uFS-1",
    "ufs_2": "uFS-2",
    "ufs_3": "uFS-3",
    "ufs_4": "uFS-4",
    "ufs_5": "uFS-5",
    "ufs_6": "uFS-6",
    "ufs_7": "uFS-7",
    "ufs_8": "uFS-8",
    "ufs_9": "uFS-9",
    "ufs_10": "uFS-10",
    "ext4": "ext4"
}

legend_map = {line: legend() for line in LINES}

ctype = 'eps'
if len(sys.argv) == 2:
    ctype = sys.argv[1]

c = canvas(ctype,
           title=plot_name,
           dimensions=[
               DRAWABLE_X_LEN + DRAWABLE_COORD_X + 5,
               DRAWABLE_Y_LEN + DRAWABLE_COORD_Y + 10
           ])
p = plotter()
t = table(file=varmail_data_path)
d = drawable(canvas=c,
             coord=[DRAWABLE_COORD_X, DRAWABLE_COORD_Y],
             dimensions=[DRAWABLE_X_LEN, DRAWABLE_Y_LEN],
             xrange=[1, 10],
             yrange=[0, 400000])

ymanual = [[f"{y//1000}K", y] for y in range(100000, 400001, 100000)]
# ymanual[0] = ["0", 0]

for line in LINES:
    p.line(drawable=d,
           table=t,
           xfield='num_client',
           yfield=line,
           linedash=LINES_DASH_MAP[line],
           linewidth=LINE_WIDTH,
           linecolor=LINES_COLOR_MAP[line],
           legend=legend_map[line],
           legendtext=LINES_NAME_MAP[line])

axis(
    drawable=d,
    #  title='Varmail Throughput',
    xtitle='Number of Clients',
    ytitle='IOPS',
    xlabelfontsize=LABEL_FONT_SZ,
    ylabelfontsize=LABEL_FONT_SZ,
    xtitlesize=TITLE_FONT_SZ,
    ytitlesize=TITLE_FONT_SZ,
    ylabelshift=YLABEL_SHIFT,
    xtitleshift=XTITLE_SHIFT,
    ytitleshift=YTITLE_SHIFT,
    ylabelrotate=90,
    ymanual=ymanual)

legend_base_x, legend_base_y = LEGEND_BASE
cnt = 0
for line in LINES:
    legend = legend_map[line]
    legend.draw(canvas=c,
                coord=[
                    legend_base_x + (cnt % LEGEND_EACH_ROW) * LEGEND_X_OFFSET,
                    legend_base_y - (cnt // LEGEND_EACH_ROW) * LEGEND_Y_OFFSET
                ],
                fontsize=LEGEND_FONT_SZ)
    cnt += 1

c.render()
