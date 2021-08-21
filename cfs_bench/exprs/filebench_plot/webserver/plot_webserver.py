#! /usr/bin/env python3

import sys
from zplot import *


def print_usage_and_exit():
    print(f"Usage: {sys.argv[0]} <plot_name> <webserver_data_path>")
    exit(1)


if len(sys.argv) != 3:
    print_usage_and_exit()

plot_name = sys.argv[1]
webserver_data_path = sys.argv[2]

LINE_WIDTH = 1
LINES = ["ufs_0", "ufs_50", "ufs_75", "ufs_100", "ext4"]

LINES_COLOR_MAP = {
    "ufs_100": "0,0,0",
    "ufs_75": "0.4,0.4,0.4",
    "ufs_50": "0.6,0.6,0.6",
    "ufs_0": "0.8,0.8,0.8",
    "ext4": "black"
}

LINES_DASH_MAP = {
    "ufs_0": 0,
    "ufs_50": 0,
    "ufs_75": 0,
    "ufs_100": 0,
    "ext4": [4, 1.6],
}

# LINES_POINT_MAP = {}

LINES_NAME_MAP = {
    "ufs_0": "uFS-0%",
    "ufs_50": "uFS-50%",
    "ufs_75": "uFS-75%",
    "ufs_100": "uFS-100%",
    "ext4": "ext4"
}

POINTS_MAP = {
    "ufs_50": "xline",
    "ufs_75": "triangle",
}

DRAWABLE_X_LEN = 200
DRAWABLE_Y_LEN = 100
DRAWABLE_COORD_X = 20
DRAWABLE_COORD_Y = 22

YLABEL_SHIFT = [0, 4]

XTITLE_SHIFT = [0, 2]
YTITLE_SHIFT = [6, 0]

LEGEND_BASE = [30, 120]
LEGEND_X_OFFSET = 50
LEGEND_Y_OFFSET = 10
LEGEND_EACH_ROW = 2
LEGEND_FONT_SZ = 7

TITLE_FONT_SZ = 7
LABEL_FONT_SZ = 7

legend_map = {line: legend() for line in LINES}
point_legend_map = {
    "ufs_50": legend(),
    "ufs_75": legend(),
}

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
t = table(file=webserver_data_path)
d = drawable(canvas=c,
             coord=[DRAWABLE_COORD_X, DRAWABLE_COORD_Y],
             dimensions=[DRAWABLE_X_LEN, DRAWABLE_Y_LEN],
             xrange=[1, 10],
             yrange=[0, 5000000])

ymanual = [[f"{y//1000000}M", y] for y in range(1000000, 5000001, 1000000)]
# ymanual[0] = ['0', 0]

for line in LINES:
    p.line(drawable=d,
           table=t,
           xfield='num_client',
           yfield=line,
           linewidth=LINE_WIDTH,
           linecolor=LINES_COLOR_MAP[line],
           linedash=LINES_DASH_MAP[line],
           legend=legend_map[line],
           legendtext=LINES_NAME_MAP[line])
    if line in POINTS_MAP:
        p.points(
            drawable=d,
            table=t,
            xfield='num_client',
            yfield=line,
            size=1,
            style=POINTS_MAP[line],
            legend=point_legend_map[line],
            linecolor=LINES_COLOR_MAP[line],
        )

axis(drawable=d,
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
     xauto=[1, 10, 1],
     ymanual=ymanual)

legend_base_x, legend_base_y = LEGEND_BASE
cnt = 0

# manually set order
for line in ["ufs_100", "ufs_75", "ufs_50", "ufs_0", "ext4"]:
    legend = legend_map[line]
    legend.draw(
        canvas=c,
        coord=[
            legend_base_x + (cnt % LEGEND_EACH_ROW) * LEGEND_X_OFFSET,
            legend_base_y - (cnt // LEGEND_EACH_ROW) * LEGEND_Y_OFFSET
        ],
        # width=3,
        # height=3,
        fontsize=LEGEND_FONT_SZ)
    cnt += 1

point_legend_map["ufs_50"].draw(
    canvas=c,
    coord=[
        legend_base_x + (2 % LEGEND_EACH_ROW) * LEGEND_X_OFFSET + 3.5,
        legend_base_y - (2 // LEGEND_EACH_ROW) * LEGEND_Y_OFFSET
    ],
    height=3,
    width=3)

point_legend_map["ufs_75"].draw(
    canvas=c,
    coord=[
        legend_base_x + (1 % LEGEND_EACH_ROW) * LEGEND_X_OFFSET + 3.5,
        legend_base_y - (1 // LEGEND_EACH_ROW) * LEGEND_Y_OFFSET
    ],
    height=3,
    width=3)

c.render()
