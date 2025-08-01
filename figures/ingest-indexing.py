from io import StringIO
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.gridspec as gridspec

import common
common.latex_matplotlib_defaults()
COLORS = common.tab20bc_colors();

colors = {
    "mach": COLORS[28],
    "fishstore": COLORS[24],
}

labels = {
    "mach": "Mach",
    "fishstore": "Fishstore",
}
