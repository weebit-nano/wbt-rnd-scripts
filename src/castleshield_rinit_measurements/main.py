import os

import addcopyfighandler
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# =========================
# CONFIGURATION
# =========================

DATA_PATH = r"C:/Users/MarcDrouard/Weebit Nano/Weebit Nano RnD Data - Documents/Test/Projects info/NeMo 32x32 1k/test_results/2025_12_04_Rinit/2025_12_04/"
Y_NAME = "Resistance(Ohm)"

# Voltages [V]
V_RANGE = np.linspace(25e-3, 175e-3, 7)

# Currents as strings used in folder names / column names
I_RANGE_STR = ["100n", "1u", "10u"]

# Mapping from label to actual current [A]
I_VALUES = {
    "100n": 100e-9,
    "1u":   1e-6,
    "10u":  10e-6,
}

# ---- Resistance ranges / thresholds ----

# For plotting (x-axis limits)
XMIN_PLOT = 1e6
XMAX_PLOT = 1e10
XMAX_PLOT_FIT_ONLY = 1e12  # for the "fit only" panel

# For CDF fit in log(R)
XMIN_FIT = 2e7           # lower bound used in the fit mask
XMAX_FIT_FACTOR = 1e9    # upper bound ~ v * XMAX_FIT_FACTOR

# Empirical CDF tail cut (keep only R < v * R_CUT_FACTOR)
R_CUT_FACTOR = 1e9

# Grid for extrapolated CDF curves
R_GRID_MIN_EXP = 6
R_GRID_MAX_EXP = 12
R_GRID_POINTS = 600


# =========================
# HELPERS
# =========================

def build_folder_path(base_path, v, i_str):
    """
    Build the folder path for a given voltage v and current label i_str.
    Assumes the same naming convention as your original code.
    """
    folder = f"Rinit_{v}v_{i_str}A_R/0_Rinit/"
    return os.path.join(base_path, folder)


def load_resistance_columns_for_voltage(v):
    """
    For a given voltage v, load R columns for each current
    and return a DataFrame with one column per current:
    R_100nA, R_1uA, R_10uA
    """
    cols = {}

    for i_str in I_RANGE_STR:
        path = build_folder_path(DATA_PATH, v, i_str)

        # Assuming a single file per folder as in original code
        file_name = os.listdir(path)[0]
        file_path = os.path.join(path, file_name)

        df_raw = pd.read_csv(file_path, sep=" ")
        col_name = f"R_{i_str}A"
        cols[col_name] = df_raw[Y_NAME].dropna().reset_index(drop=True)

    return pd.DataFrame(cols)


def compute_effective_resistance(df, v):
    """
    Apply the threshold-based selection of R:

    - Default: R_10uA
    - If R_100nA > v / 100nA: use R_100nA
    - Else if v / 1uA < R_1uA < v / 100nA: use R_1uA
    """
    R_100nA = df["R_100nA"].copy()
    R_1uA   = df["R_1uA"].copy()
    R_10uA  = df["R_10uA"].copy()

    thresh_100n = v / I_VALUES["100n"]  # v / 100nA
    thresh_1u   = v / I_VALUES["1u"]    # v / 1uA

    # Start with 10uA everywhere
    R_eff = R_10uA.copy()

    # Condition 1: use R_100nA if R_100nA > v/100n
    cond1 = R_100nA > thresh_100n
    R_eff[cond1] = R_100nA[cond1]

    # Condition 2: (only where cond1 is False) use R_1uA if v/1u < R_1uA < v/100n
    cond2 = (~cond1) & (R_1uA < thresh_100n) & (R_1uA > thresh_1u)
    R_eff[cond2] = R_1uA[cond2]

    # Track which current was used
    selected_current = np.where(cond1, "100nA",
                                np.where(cond2, "1uA", "10uA"))

    df["R"] = R_eff
    df["I_selected"] = selected_current

    return df


def fit_log_logistic(sorted_R, emp_cdf, v):
    """
    Fit a logistic in log(R) over a window [XMIN_FIT, v * XMAX_FIT_FACTOR].

    Returns (a, b, alpha, lam) and the fitted CDF on a log-spaced grid.
    If not enough points, returns (None, None, None, None, None).
    """
    # Fit window
    xmax_fit = v * XMAX_FIT_FACTOR

    mask = ((sorted_R >= XMIN_FIT) &
            (sorted_R <= xmax_fit) &
            (emp_cdf > 0) &
            (emp_cdf < 1))

    if np.sum(mask) <= 5:
        return None, None, None, None, None

    x_fit = sorted_R[mask]
    F_fit = emp_cdf[mask]

    # log-scale for x
    logx = np.log(x_fit)

    # logit for F
    eps = 1e-8
    F_fit_clipped = np.clip(F_fit, eps, 1 - eps)
    logitF = np.log(F_fit_clipped / (1 - F_fit_clipped))

    # Fit: logit(F) = a + b * ln(R)
    b, a = np.polyfit(logx, logitF, 1)

    # Convert to log-logistic parameters
    alpha = b
    lam = np.exp(-a / b) if b != 0 else np.nan

    # Extrapolate on a common grid
    R_grid = np.logspace(R_GRID_MIN_EXP, R_GRID_MAX_EXP, R_GRID_POINTS)
    logR_grid = np.log(R_grid)
    logit_grid = a + b * logR_grid
    cdf_fit = 1.0 / (1.0 + np.exp(-logit_grid))

    return a, b, alpha, lam, (R_grid, cdf_fit)


# =========================
# MAIN SCRIPT
# =========================

dfs_per_V = {}
loglogistic_params = {}

fig, ax = plt.subplots(1, 2, figsize=(12, 6))
ax = ax.flatten()

for v in V_RANGE:
    # 1) Load raw R columns for this voltage
    df = load_resistance_columns_for_voltage(v)

    # 2) Compute effective R and selected current based on thresholds
    df = compute_effective_resistance(df, v)
    dfs_per_V[v] = df

    # 3) Empirical CDF
    R_values = df["R"].dropna().values
    sorted_R = np.sort(R_values)
    emp_cdf = np.arange(1, len(sorted_R) + 1) / len(sorted_R)

    # Cut tail where R < v * R_CUT_FACTOR
    tail_limit = v * R_CUT_FACTOR
    n_tail = np.sum(sorted_R < tail_limit)

    # Plot empirical CDF and capture color
    (line_data,) = ax[0].plot(
        sorted_R[:n_tail],
        emp_cdf[:n_tail],
        ls="",
        marker="o",
        markersize=3,
        label=f"{v*1000:.0f} mV",
    )
    color = line_data.get_color()

    # 4) Fit log-logistic in log(R)
    a, b, alpha, lam, fit_result = fit_log_logistic(sorted_R, emp_cdf, v)

    if fit_result is None:
        print(f"V = {v:.3f} V: not enough points in logistic fit region")
        continue

    loglogistic_params[v] = (a, b, alpha, lam)
    R_grid, cdf_fit = fit_result

    print(
        f"V = {v:.3f} V: logistic fit a={a:.3f}, b={b:.3f}, "
        f"log-logistic alpha={alpha:.3f}, lambda={lam:.3e}"
    )

    # Plot fit on both panels
    ax[0].plot(R_grid, cdf_fit, linestyle=":", color=color)
    ax[1].plot(
        R_grid,
        cdf_fit,
        linestyle="-",
        color=color,
        label=f"{v*1000:.0f} mV",
    )

# =========================
# PLOT STYLING
# =========================

# Left: empirical CDF + fits
ax[0].set_xscale("log")
ax[0].set_yscale("log")
ax[0].set_xlim([XMIN_PLOT, XMAX_PLOT])
ax[0].set_ylim([1e-3, 1])
ax[0].set_xlabel("Resistance (Ohm)")
ax[0].set_ylabel("CDF")
ax[0].grid(True, which="both", ls = '--', color = 'lightgray')
ax[0].legend(fontsize=8, ncol=2)
ax[0].set_title("Empirical CDF and log-logistic fit")

# Right: fits only
ax[1].set_xscale("log")
ax[1].set_xlim([XMIN_PLOT, XMAX_PLOT_FIT_ONLY])
ax[1].set_ylim([1e-3, 1])
ax[1].set_xlabel("Resistance (Ohm)")
ax[1].set_ylabel("CDF")
ax[1].grid(True, which="both", ls = '--', color = 'lightgray')
ax[1].legend(fontsize=8, ncol=2)
ax[1].set_title("Log-logistic fit")

plt.tight_layout()
plt.show()
