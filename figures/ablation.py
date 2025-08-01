from io import StringIO
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.gridspec as gridspec

import common
common.latex_matplotlib_defaults()
COLORS = common.tab20bc_colors();

colors = { "Mach": COLORS[28], "Influx": COLORS[20], "FishStore-I": COLORS[24] }

data = ""
with open("data/ablation.csv") as f:
	for line in f.readlines():
		data = data + line

df = pd.read_csv(StringIO(data))
print(df)

# Ablation

fig, axs = plt.subplots(2, 1, figsize=(3.33, 2));


def plot_line(idx, label, linestyle, marker, color):
    f = (df["ablation"] == idx) & (df["lookback"] <= 300)
    y = list(df.loc[f, "duration"])
    x = list(df.loc[f, "lookback"])
    print(y)
    markersize = 5
    if marker == '^':
        markersize = 5
    ax.plot(x, y, linestyle=linestyle, marker=marker, label = label, color = color , markersize=markersize)

ax = axs[0]
plot_line("noindex", "No index", ':', '+', COLORS[28])
plot_line("timeonly", "Time index only", '--', 'x', COLORS[28])

handles, labels = ax.get_legend_handles_labels()
# ax.legend(handles, labels, loc='upper left', ncols=1, frameon=False, borderaxespad=0.02)

ax = axs[1]
plot_line("rangeonly", "Range index only", '-.', '^', COLORS[28])
plot_line("timerange", "Both indexes", '-', 'o',  COLORS[28])


axs[0].set_ylim(100, 650)
axs[0].set_xticks([])
axs[0].spines.bottom.set_linestyle("dashed")
axs[0].spines.bottom.set_color(COLORS[38])

axs[1] = axs[1]
axs[1].set_xlabel("Lookback (s)")
axs[1].set_xticks(list(df.loc[df["ablation"] == "timerange", "lookback"]))
axs[1].set_xlim(0, 310)
axs[1].set_ylim(0, 20)
#axs[1].set_yticks([0, 25, 50])
axs[1].spines.top.set_linestyle("dashed")
axs[1].spines.top.set_color(COLORS[38])

handles2, labels2 = ax.get_legend_handles_labels()
handles = handles + handles2
labels = labels + labels2

fig.legend(handles, labels, loc='upper center', ncols=2,
           bbox_to_anchor=(0.55,1.1), frameon=False, borderaxespad=0.02)

fig.supylabel('Query\nLatency (s)', x=.1, y=.6, ha="center", fontsize=10)

plt.tight_layout()
fig.subplots_adjust(hspace=0.05)
outfile = "figures/ablation.pdf"
plt.savefig(outfile, bbox_inches="tight")

# Vs Fishstore Exact

def plot_line(idx, label, linestyle, marker, color):
    f = (df["ablation"] == idx) & (df["lookback"] >= 60)
    y = list(df.loc[f, "duration"])
    x = list(df.loc[f, "lookback"])
    print(y)
    markersize = 5
    if marker == '^':
        markersize = 5
    ax.plot(x, y, linestyle=linestyle, marker=marker, label = label, color = color , markersize=markersize)


fig, ax = plt.subplots(1, 1, figsize=(3.33, 1.5));
plot_line("timerange", "Full Index", '-', 'o',  COLORS[28])
plot_line("fishstore", "FishStore Exact", '-', 'x', COLORS[24])
ax.set_xticks([60, 120, 300, 600])
ax.set_ylim(0, 16)
ax.set_yticks([0, 5, 10, 15])
ax.set_xlabel("Lookback (s)")
ax.set_ylabel("Query\nLatency (s)")

ax.legend(frameon=False)

plt.tight_layout()
outfile = "figures/fishstore-exact.pdf"
plt.savefig(outfile, bbox_inches="tight")

