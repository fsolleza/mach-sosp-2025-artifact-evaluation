import matplotlib
import matplotlib.pyplot as plt

# def tab20_colors():
#     cmap = matplotlib.colormaps["tab20"]
#     color_list = []
#     for color in cmap.colors:
#         color_list.append(matplotlib.colors.rgb2hex(color))
#     return color_list

def tab20bc_colors():
    cmap = matplotlib.colormaps["tab20b"]
    color_list = []
    for color in cmap.colors:
        color_list.append(matplotlib.colors.rgb2hex(color))
        cmap = matplotlib.colormaps["tab20c"]
    for color in cmap.colors:
        color_list.append(matplotlib.colors.rgb2hex(color))
    return color_list

def default_colors():
    return plt.rcParams['axes.prop_cycle'].by_key()['color']

COLORS = tab20bc_colors()

def plot_colors(out):
    color_list = tab20bc_colors()
    square = .30
    to_plot = list(range(0, len(color_list)))

    fig, ax = plt.subplots(1, 1, figsize=(square * len(color_list), square), sharey=True)
    ax.scatter(to_plot,[0 for x in range(0, len(color_list))],  marker="s", c=color_list, s=200)
    ax.spines[["left", "right", "top", "bottom"]].set_visible(False)
    ax.set_xticks(to_plot, labels=["{} [{}]".format(c, i) for i, c in enumerate(color_list)], rotation=90)
    ax.set_yticks([])
    plt.savefig(out, bbox_inches="tight")


def latex_matplotlib_defaults():
    matplotlib.rc('font', family='serif', size=10)
    matplotlib.rc('text.latex', preamble='\\usepackage{times,mathptmx,graphicx}')
    matplotlib.rc('text', usetex=True)
    matplotlib.rc('legend', fontsize=8)
    matplotlib.rc('figure', figsize=(3.33,1.5))
    matplotlib.rc('axes', linewidth=0.5, titlesize=10)
    matplotlib.rc('lines', linewidth=0.5)
