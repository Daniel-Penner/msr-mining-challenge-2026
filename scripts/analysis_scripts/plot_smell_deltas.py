#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ---------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data" / "processed" / "smell_deltas_per_commit.csv"
OUT_DIR = PROJECT_ROOT / "outputs" / "plots"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------------
df = pd.read_csv(DATA_PATH)
print(f"Loaded {len(df)} rows")

# Ensure numeric columns are parsed correctly
df["delta"] = pd.to_numeric(df["delta"], errors="coerce")
df["smells_before"] = pd.to_numeric(df["smells_before"], errors="coerce")
df["smells_after"] = pd.to_numeric(df["smells_after"], errors="coerce")

# ---------------------------------------------------------------
# 1. Boxplot of smell deltas per agent
# ---------------------------------------------------------------
plt.figure(figsize=(8, 6))
df.boxplot(column="delta", by="agent", grid=False)
plt.title("Distribution of Smell Deltas per Agent")
plt.suptitle("")  # remove default subtitle
plt.xlabel("Agent")
plt.ylabel("Smell Delta (after - before)")
plt.yscale("symlog", linthresh=1)
plt.tight_layout()
plt.savefig(OUT_DIR / "smell_deltas_boxplot.png", dpi=300)
plt.close()

# ---------------------------------------------------------------
# 2. Bar graph: smells before vs after per agent
# ---------------------------------------------------------------
grouped = df.groupby("agent")[["smells_before", "smells_after"]].mean().reset_index()

x = range(len(grouped))
width = 0.35

plt.figure(figsize=(8, 6))
plt.bar([i - width/2 for i in x], grouped["smells_before"], width=width, label="Before")
plt.bar([i + width/2 for i in x], grouped["smells_after"], width=width, label="After")

plt.xticks(x, grouped["agent"])
plt.title("Average Smells Before and After per Agent")
plt.xlabel("Agent")
plt.ylabel("Average Smell Count")
plt.legend()
plt.tight_layout()
plt.savefig(OUT_DIR / "smells_before_after_bargraph.png", dpi=300)
plt.close()

# ---------------------------------------------------------------
# 3. Stacked barplot: rate of less/same/more smells per agent
# ---------------------------------------------------------------
# Categorize each commit
def categorize(delta):
    if delta < 0:
        return "Less Smells"
    elif delta == 0:
        return "Same Smells"
    else:
        return "More Smells"

df["category"] = df["delta"].apply(categorize)

# Compute proportions per agent
counts = (
    df.groupby(["agent", "category"])
    .size()
    .unstack(fill_value=0)
)

proportions = counts.div(counts.sum(axis=1), axis=0)

# Plot stacked bar
plt.figure(figsize=(8, 6))
bottom = None
colors = {"Less Smells": "#10B981", "Same Smells": "#A3A3A3", "More Smells": "#EF4444"}

for cat in ["Less Smells", "Same Smells", "More Smells"]:
    plt.bar(
        proportions.index,
        proportions[cat],
        bottom=bottom,
        label=cat,
        color=colors[cat],
    )
    bottom = proportions[cat] if bottom is None else bottom + proportions[cat]

plt.title("Proportion of Commits by Smell Change Category per Agent")
plt.ylabel("Proportion of Commits")
plt.xlabel("Agent")
plt.legend(title="Change Type")
plt.tight_layout()
plt.savefig(OUT_DIR / "smell_change_stacked_barplot.png", dpi=300)
plt.close()

print("✅ Saved plots to:", OUT_DIR)
