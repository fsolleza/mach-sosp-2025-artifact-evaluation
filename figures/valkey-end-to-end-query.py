from io import StringIO
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.gridspec as gridspec

import common
common.latex_matplotlib_defaults()
COLORS = common.tab20bc_colors();

data = ""
with open("data/e2e_valkey.csv") as f:
	for line in f.readlines():
		data = data + line

df = pd.read_csv(StringIO(data));
print("Drop rates")
print(df.loc[df["desc"] == "Dropped (%)"])

bar_width = 1.5
bar_space = .01
group_space = .5

colors = {
    "Mach": COLORS[28],
    "Mach-light": COLORS[29],
    "Influx-complete": COLORS[21],
    "FishStore-I": COLORS[24]
}

#########################
# Phase 1 Query Latency #
#########################

matplotlib.rc('figure', figsize=(1.4,1.5))
fig, ax = plt.subplots(1, 1, dpi=300, sharey = False)

phase_data = df.loc[df["phase"] == "P1"]

def format_ax():
    ax.set_xticks([])
    ax.tick_params(bottom=False)
    ax.set_ylim(0, 35)

# Q1 
is_p1 = df["phase"] == "P1"
is_query = df["desc"] == "Slow Requests"
is_mach = df["database"] == "Mach"
is_influx_complete = df["database"] == "Influx-complete"
is_fishstore = df["database"] == "FishStore-I"

xpos = 1
xticks = []
xlabels = []

def plot_bar(val, db, cutoff = None, bar_label_padding=0, bar_label_color="black", dagger=None):
    global xpos
    y = val
    if cutoff:
        y = cutoff
    b = ax.bar(xpos, y, width = bar_width, color=colors[db], label=db)
    padding = 2
    label = r"{:.1f}".format(val)
    ax.bar_label(b, label_type="edge", labels=[label], rotation="vertical", padding=bar_label_padding, color=bar_label_color, fontsize=8)
    if dagger:
        ax.text(dagger[0], dagger[1], r"$\dagger$", fontsize=12, color="red", ha='center', va='bottom')
    xticks.append(xpos)
    xpos += bar_width + group_space

# Influx complete
total_dur = list(df.loc[is_p1 & is_influx_complete & is_query, "value"])[0]
plot_bar(total_dur, "Influx-complete", cutoff=50, bar_label_padding = -18, bar_label_color = "white")

# Fishstore
total_dur = list(df.loc[is_p1 & is_fishstore & is_query, "value"])[0]
plot_bar(total_dur, "FishStore-I", bar_label_padding = 2)

# Mach
total_dur = list(df.loc[is_p1 & is_mach & is_query, "value"])[0]
plot_bar(total_dur, "Mach", bar_label_padding = 2)

# Other figure stuff
ax.set_ylabel("Query Latency (s)")
ax.set_title("Slow\nRequests")
ax.set_ylim(0, 55)
ax.set_xticks([])

# Squiggles at the top
x_low = bar_width / 2 - group_space
x_high = xpos - group_space
x = np.arange(x_low, x_high, 0.01)
y1 = 2 * np.sin(np.pi * x) + 48
ax.fill_between(x, y1, 60, color="white")

handles, labels = ax.get_legend_handles_labels()
# End all axes
#fig.legend(handles, labels, loc='lower left', bbox_to_anchor = (.25, -.15),
#           ncols=2, frameon=False)

plt.tight_layout()
# plt.subplots_adjust(wspace=.05, hspace=0.07)
outfile = "figures/valkey_p1_query_latency.pdf"
plt.savefig(outfile, bbox_inches="tight")

#########################
# Phase 2 Query Latency #
#########################

matplotlib.rc('figure', figsize=(2.8,1.5))
fig, axs = plt.subplots(1, 2, dpi=300, sharey = False)

xpos = 1

values_ax0 = {}
values_ax1 = {}

slow_requests_df = df["desc"] == "Slow Requests"
slow_sendto_df = df["desc"] == "Slow sendto"
for db in ["Influx-complete", "FishStore-I", "Mach"]:
	filt = (df["database"] == db) & (df["phase"] == "P2")
	slow_requests = list(df.loc[filt & slow_requests_df, "value"])[0]
	slow_sendto = list(df.loc[filt & slow_sendto_df, "value"])[0]
	values_ax0[db] = slow_requests
	values_ax1[db] = slow_sendto

ax = axs[0]
xpos = 1
plot_bar(values_ax0["Influx-complete"], "Influx-complete", cutoff=50,
         bar_label_padding=-22, bar_label_color="white")
plot_bar(values_ax0["FishStore-I"], "FishStore-I", bar_label_padding = 2)
plot_bar(values_ax0["Mach"], "Mach", bar_label_padding = 2)

ax = axs[1]
xpos = 1
plot_bar(values_ax1["Influx-complete"], "Influx-complete", cutoff=50,
         bar_label_padding=-22, bar_label_color="white")
plot_bar(values_ax1["FishStore-I"], "FishStore-I", bar_label_padding = 2)
plot_bar(values_ax1["Mach"], "Mach")

# Other figure stuff
ax = axs[0]
ax.set_title("Slow\nRequests")
#ax.set_ylabel("Query Latency (s)")
ax.set_ylim(0, 55)
ax.set_xticks([])
handles, labels = ax.get_legend_handles_labels()

ax = axs[1]
ax.set_title(r"Slow \texttt{sendto}" "\n" "Execution");
ax.set_ylim(0, 55)
ax.set_yticks([])
ax.set_xticks([])

#fig.legend(handles, labels, loc='lower left', bbox_to_anchor = (.2, -.175),
#           ncols=2, frameon=False)

x_low = bar_width / 2 - group_space
x_high = xpos - group_space
x = np.arange(x_low, x_high, 0.01)
y1 = 2 * np.sin(np.pi * x) + 48
for ax in axs:
    ax.fill_between(x, y1, 60, color="white")

plt.tight_layout()
# plt.subplots_adjust(wspace=.05, hspace=0.07)
outfile = "figures/valkey_p2_query_latency.pdf"
plt.savefig(outfile, bbox_inches="tight")

#########################
# Phase 3 Query Latency #
#########################

matplotlib.rc('figure', figsize=(2.8,1.5))
fig, axs = plt.subplots(1, 2, dpi=300, sharey = False)

xpos = 1

values_ax0 = {}
values_ax1 = {}

max_request_df = df["desc"] == "Max Request"
packet_scan_df = df["desc"] == "Packet Scan"
for db in ["Influx-complete", "FishStore-I", "Mach"]:
	filt = (df["database"] == db) & (df["phase"] == "P3")
	max_request = list(df.loc[filt & max_request_df, "value"])[0]
	packet_scan = list(df.loc[filt & packet_scan_df, "value"])[0]
	values_ax0[db] = max_request
	values_ax1[db] = packet_scan

ax = axs[0]
xpos = 1
plot_bar(values_ax0["Influx-complete"], "Influx-complete", bar_label_padding=2)
plot_bar(values_ax0["FishStore-I"], "FishStore-I", bar_label_padding=2)
plot_bar(values_ax0["Mach"], "Mach", bar_label_padding = 2)

ax = axs[1]
xpos = 1
plot_bar(values_ax1["Influx-complete"], "Influx-complete", cutoff = 50, bar_label_padding=-22, bar_label_color="white")
plot_bar(values_ax1["FishStore-I"], "FishStore-I", bar_label_padding = -16, bar_label_color=  "white")
plot_bar(values_ax1["Mach"], "Mach")


ax = axs[0]
ax.set_title("Maximum\nLatency Request");
#ax.set_ylabel("Query Latency (s)")
ax.set_ylim(0, 55)
ax.set_xticks([])
handles, labels = ax.get_legend_handles_labels()

ax = axs[1]
ax.set_title("TCP Packet Dump");
ax.set_ylim(0, 55)
ax.set_yticks([])
ax.set_xticks([])

#fig.legend(handles, labels, loc='lower left', bbox_to_anchor = (.2, -.175), ncols=2, frameon=False)


x_low = bar_width / 2 - group_space
x_high = xpos - group_space
x = np.arange(x_low, x_high, 0.01)
y1 = 2 * np.sin(np.pi * x) + 48
for ax in axs:
    ax.fill_between(x, y1, 60, color="white")

plt.tight_layout()
# plt.subplots_adjust(wspace=.05, hspace=0.07)
outfile = "figures/valkey_p3_query_latency.pdf"
plt.savefig(outfile, bbox_inches="tight")

