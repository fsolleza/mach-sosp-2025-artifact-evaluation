import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import common
from matplotlib.lines import Line2D
from io import StringIO

common.latex_matplotlib_defaults()
COLORS = common.tab20bc_colors();

colors = { "SysX": COLORS[28], "Influx": COLORS[20], "FishStore": COLORS[24] }

data = ""
with open("data/probe_effects.csv") as f:
	for line in f.readlines():
		data = data + line
df = pd.read_csv(StringIO(data));

def get_tp(sys):
	return list(df[df["sys"]==sys]["throughput"])[0]

# baseline is no storing of HFT
no_storage = get_tp("noop")
baseline = no_storage / 1000000

method = [
        'InfluxDB',
        'FishStore-I',
        'FishStore-N',
        'Raw File',
        'SysX'
]
data = [ get_tp(x) for x in method ]

bar_colors = [
        COLORS[20],
        COLORS[24],
        COLORS[25],
        COLORS[37],
        COLORS[28]
]

# data_to_plot = [no_storage - d/1000000 for d in data]
data_to_plot = [100 * (1 - d/no_storage) for d in data]
print(data_to_plot)

fig, ax = plt.subplots()
b = ax.bar(method, data_to_plot, color=bar_colors, label=method)

bar_labels = ["{:.2f}%".format(x).replace('%', '\%') for x in data_to_plot]
# bar_labels[0] = "{:.2f}".format(data_to_plot[0])
ax.bar_label(b, label_type="edge", labels=bar_labels, fontsize=7)
handles, labels = ax.get_legend_handles_labels()

# draw horizontal line
# ax.plot([-2, 5], [baseline, baseline], linestyle="--")

# add a text box
# props = dict(boxstyle='round', alpha=0.0)
# ax.text(0.04, 0.91, "No Storage: {:.2f}M".format(baseline), transform=ax.transAxes,
#        fontsize=7, verticalalignment='top', bbox=props)

#ax.set_title("Ovearhead of Co-Located Monitoring\nApplications on RocksDB Application Throughput")
ax.set_ylabel("Probe Effect")
ax.set_xticks(method, method, ha='right', rotation=20)
ax.set_ylim(0, 20)
ax.set_xlim(-.50, 4.5)

#fig.legend(handles, labels, loc='upper left', #bbox_to_anchor = (.175, -.2),
#           ncols=3, frameon=False, columnspacing=0.5)

plt.tight_layout()
plt.subplots_adjust(wspace=.05, hspace=0.07)
plt.savefig("figures/probe-effect.pdf", bbox_inches="tight")
