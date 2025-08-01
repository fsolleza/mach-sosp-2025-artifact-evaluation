from io import StringIO
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib
import matplotlib.gridspec as gridspec

import common
common.latex_matplotlib_defaults()
COLORS = common.tab20bc_colors();

data = ""
with open("data/e2e_rocksdb.csv") as f:
	for line in f.readlines():
		data = data + line


df = pd.read_csv(StringIO(data));
print("Drop rates")
print(df.loc[df["desc"] == "Dropped (%)"])

bar_width = 1.5
bar_space = .05
group_space = .5

colors = {
    "Mach": COLORS[28],
    "Influx": COLORS[20],
    "Influx-complete": COLORS[21],
    "FishStore-I": COLORS[24]
}

#########################
# Phase 1 Query Latency #
#########################

matplotlib.rc('figure', figsize=(2.8,1.5))
fig, axs = plt.subplots(1, 2, dpi=300, sharey = False)

phase_data = df.loc[df["phase"] == "P1"]

xpos = 1
ax = axs[0]
xticks = []
xlabels = []
dagger_locs = []
bar_labels = []
def plot_bar_group(db, field, bar_label_padding = 0, bar_label_color = "black", dagger = None):
    global xpos

    data = phase_data.loc[phase_data["database"] == db]

    # Get the low and high bounds of the bars to position the xtick
    xticks.append(xpos)
    xlabels.append(db)

    true_yval = list(data.loc[data["desc"] == field, "value"])[0]
    yval = true_yval

    label_rotation = "vertical"
    label_padding = 2
    label_color = "black"
    if yval > 55:
        yval = 55

    color = colors[db]
    lbl = db
    if db == "Influx-complete":
        lbl = "InfluxDB-idealized"
    elif db == "Influx":
        lbl = "InfluxDB"
    elif db == "FishStore-I":
        lbl = "FishStore"
    b = ax.bar(xpos, yval, width = bar_width, color = color, label=lbl)
    label = r"{:.1f}".format(true_yval)
    l = ax.bar_label(b, label_type="edge", labels=[label],
                 color=bar_label_color, rotation=label_rotation, padding=bar_label_padding)

    if dagger:
        ax.text(dagger[0], dagger[1], r"$\dagger$", fontsize=12, color="red", ha='center', va='bottom')

    bar_labels.append(l)

    xpos += bar_width + group_space

def format_ax():
    ax.set_xticks([])
    ax.tick_params(bottom=False)
    ax.set_ylim(0, 31)

# Q1 
# plot_bar_group("Influx", "Max", bar_label_padding=2, dagger=(xpos, 12))
plot_bar_group("Influx-complete", "Max", bar_label_padding=-20, bar_label_color = "white")
plot_bar_group("FishStore-I", "Max", bar_label_padding = -20, bar_label_color = "white")
plot_bar_group("Mach", "Max", bar_label_padding=2)

format_ax()
ax.set_ylabel("Query Latency (s)")
ax.set_title("Application\nMax Latency")
handles, labels = ax.get_legend_handles_labels()

x_low = 0
x_high = xpos - group_space
ax.set_xlim(x_low, x_high)
ax.set_ylim(0, 55);

# add daggers

# Q2
ax = axs[1]
xticks = []
xlabels = []
xpos = 1
dagger_locs = []
bar_labels = []

# plot_bar_group("Influx", "Percentile", bar_label_padding = -32, bar_label_color = "white", dagger=(xpos, 23))
plot_bar_group("Influx-complete", "Percentile", bar_label_padding = -30, bar_label_color = "white")
plot_bar_group("FishStore-I", "Percentile", bar_label_padding = -20, bar_label_color = "white")
plot_bar_group("Mach", "Percentile", bar_label_padding = 2)

format_ax()
ax.set_yticks([])
ax.set_title("Application\nTail Latency")
ax.set_ylim(0, 55);

# End all axes
#fig.legend(handles, labels, loc='lower left', bbox_to_anchor = (.175, -.15),
#           ncols=2, frameon=False)

x = np.arange(x_low, x_high, 0.01)
y1 = 2 * np.sin(np.pi * x) + 50
for ax in axs:
    ax.fill_between(x, y1, 60, color="white")

# daggers and bar labels


plt.tight_layout()
# plt.subplots_adjust(wspace=.05, hspace=0.07)
outfile = "figures/rocksdb_p1_query_latency.pdf"
plt.savefig(outfile, bbox_inches="tight")

#########################
# Phase 2 Query Latency #
#########################

matplotlib.rc('figure', figsize=(2.8,1.5))
fig, axs = plt.subplots(1, 2, dpi=300, sharey = False)

phase_data = df.loc[df["phase"] == "P2"]

xpos = 1
ax = axs[0]
xticks = []
xlabels = []

# Q3
# plot_bar_group("Influx", "Max", bar_label_padding = 2, dagger=(xpos, 10))
plot_bar_group("Influx-complete", "Max", bar_label_padding = -20, bar_label_color="white")
plot_bar_group("FishStore-I", "Max", bar_label_padding = -20, bar_label_color = "white")
plot_bar_group("Mach", "Max", bar_label_padding = 2)

format_ax()
#ax.set_ylabel("Query Latency (s)")
ax.set_title(r"\texttt{pread64}" "\n" "Max Latency")
handles, labels = ax.get_legend_handles_labels()

x_low = bar_width / 2 - group_space
x_high = xpos - group_space
ax.set_xlim(0, x_high)
ax.set_ylim(0, 55)

# Q4
ax = axs[1]
xticks = []
xlabels = []
xpos = 1

# plot_bar_group("Influx", "Percentile", bar_label_padding = 2, dagger = (xpos, 12))
plot_bar_group("Influx-complete", "Percentile", bar_label_padding = -20, bar_label_color="white")
plot_bar_group("FishStore-I", "Percentile", bar_label_padding = -20, bar_label_color="white")
plot_bar_group("Mach", "Percentile", bar_label_padding = 2)

format_ax()
ax.set_yticks([])
ax.set_title(r"\texttt{pread64}" "\n" "Tail Latency")
ax.set_ylim(0, 55)

# End all axes
#fig.legend(handles, labels, loc='lower left', bbox_to_anchor = (.175, -.15),
#           ncols=2, frameon=False)

x = np.arange(x_low, x_high, 0.01)
y1 = 2 * np.sin(np.pi * x) + 50
for ax in axs:
    ax.fill_between(x, y1, 60, color="white")

plt.tight_layout()
# plt.subplots_adjust(wspace=.05, hspace=0.07)
outfile = "figures/rocksdb_p2_query_latency.pdf"
plt.savefig(outfile, bbox_inches="tight")


#########################
# Phase 3 Query Latency #
#########################

matplotlib.rc('figure', figsize=(1.4,1.5))
fig, ax = plt.subplots(1, 1, dpi=300, figsize=(1.4, 1.5), sharey = False)

phase_data = df.loc[df["phase"] == "P3"]

xpos = 1
xticks = []
xlabels = []

# plot_bar_group("Influx", "Count", bar_label_padding=2)
plot_bar_group("Influx-complete", "Count", bar_label_padding=2)
plot_bar_group("FishStore-I", "Count", bar_label_padding=2)
plot_bar_group("Mach", "Count", bar_label_padding=2)

ax.set_ylim(0, 3)
#ax.set_ylabel("Query Latency (s)")
ax.set_title("Page Cache\nEvent Count")
ax.set_xticks([])
ax.set_ylim(0, 50)

handles, labels = ax.get_legend_handles_labels()
# End all axes
#fig.legend(handles, labels, loc='lower left', bbox_to_anchor = (.175, -.15),
#           ncols=2, frameon=False)

# End all axes

plt.tight_layout()
# plt.subplots_adjust(wspace=.05, hspace=0.07)
outfile = "figures/rocksdb_p3_query_latency.pdf"
plt.savefig(outfile, bbox_inches="tight")


##############
# Legend fig #
##############
legend = plt.figure(figsize=(1,0.3))
labels[2] = "SysX"
leg = legend.legend(handles, labels, ncols=4, loc='center', frameon=False, prop={'weight':'bold'})
plt.tight_layout()
legend.savefig('figures/legend.pdf', bbox_inches='tight', transparent=True)

