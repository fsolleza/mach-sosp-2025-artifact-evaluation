from io import StringIO
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.gridspec as gridspec

import common
common.latex_matplotlib_defaults()
COLORS = common.tab20bc_colors();
fishstore_choice = "fishstore-8"

colors = {
    "mach": COLORS[28],
    "fishstore-1": COLORS[24],
    "fishstore-8": COLORS[24],
    "fishstore-3": COLORS[24],
    "rocksdb-1": COLORS[2],
    "rocksdb-8": COLORS[2],
    "lmdb": COLORS[9],
}

hatches = {
    "mach": "",
    "fishstore-8": "///",
    "fishstore-3": "///",
    "fishstore-1": "",
    "rocksdb-8": "///",
    "rocksdb-1": "",
    "lmdb": "",
}

labels = {
    "mach": "SysX (1 CPU)",
    "fishstore-8": "FishStore (8 CPUs)",
    "fishstore-3": "FishStore (3 CPUs)",
    "fishstore-1": "FishStore (1 CPU)",
    "rocksdb-8": "RocksDB (8 CPUs)",
    "rocksdb-1": "RocksDB (1 CPU)",
    "lmdb": "LMDB (1 CPU)",
}

data = ""
with open("data/ingest-scaling.csv") as f:
	for line in f.readlines():
		data = data + line

df = pd.read_csv(StringIO(data))
print(df)

fig, axs = plt.subplots(2, 1, dpi=300, figsize=(3.33, 3))

bar_width = 1
bar_space = .1
group_space = 2
xpos = 0

ax = axs[0]
x_labels = []
x_ticks = []
group = df.loc[df["size"] == 8]


def plot_bar(db, size, field):
    global xpos
    y = list(df.loc[(df["size"] == size) & (df["storage"] == db), field])[0]
    label = labels[db]
    color = colors[db]
    hatch = hatches[db]
    ax.bar(xpos, y, width=bar_width, label=label, color=color, hatch=hatch)
    xpos += bar_width + bar_space

def plot_group(size, field):
    global xpos

    group_start = xpos - bar_width/2

    plot_bar("lmdb", size, field)
    xpos += bar_space * 2
    plot_bar("fishstore-1", size, field)
    plot_bar(fishstore_choice, size, field)
    xpos += bar_space
    plot_bar("rocksdb-1", size, field)
    plot_bar("rocksdb-8", size, field)
    xpos += bar_space
    plot_bar("mach", size, field)

    group_end = xpos - bar_space - bar_width / 2
    x_ticks.append(group_start + (group_end - group_start) / 2)
    x_labels.append("{}".format(size))

    xpos -= bar_width + bar_space
    xpos += bar_width / 2 + group_space

plot_group(8, "mrps")
plot_group(64, "mrps")
plot_group(256, "mrps")
plot_group(1024, "mrps")

ax = axs[1]
xpos = 0
x_labels = []
x_ticks = []

plot_group(8, "mbps")

# need to get the labels now since the rest of the bars will add to the label
# list
legend_handles, legend_labels = ax.get_legend_handles_labels()

plot_group(64, "mbps")
plot_group(256, "mbps")
plot_group(1024, "mbps")

axs[0].set_xticks([])
axs[0].set_ylabel("Million\nRecords / sec")
axs[0].set_yticks([0, 10, 20])

axs[1].set_xticks(x_ticks, x_labels)
axs[1].set_xlabel("Record size (bytes)")
axs[1].set_ylabel("MiB / sec")
#axs[1].set_yticks([0, 250, 500, 750])


axs[0].legend(legend_handles, legend_labels, loc='upper right', borderaxespad=0.02, handlelength=2, labelspacing=0.05, #bbox_to_anchor = (.10, -.1),
              ncols=1, frameon=False)

plt.tight_layout()
fig.subplots_adjust(hspace=0.1)
outfile = "figures/ingest-scaling.pdf"
plt.savefig(outfile, bbox_inches="tight")



