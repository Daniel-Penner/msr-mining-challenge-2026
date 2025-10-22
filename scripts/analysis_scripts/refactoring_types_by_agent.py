#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
refactoring_types_by_agent.py
-----------------------------
Analyzes *what kinds* of refactorings different agents perform.

Outputs:
 - CSV tables of counts and percentages per agent.
 - A stacked bar chart showing refactoring-type mix across agents.
 - A per-agent bar chart of top refactoring types.

Inputs (expected in data/processed/):
  - refactoring_dataset_refactorings.parquet
  - baseline_refactorings.parquet
  - refactoring_dataset_commits.parquet
  - human_refactoring_commits.parquet

Outputs:
  outputs/tables/refactor_types_by_agent_*.csv
  outputs/plots/refactor_types_by_agent_*.png
"""

from pathlib import Path
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# -------------------------- Setup --------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA = PROJECT_ROOT / "data" / "processed"
OUT_TABLES = PROJECT_ROOT / "outputs" / "tables"
OUT_PLOTS = PROJECT_ROOT / "outputs" / "plots"
OUT_TABLES.mkdir(parents=True, exist_ok=True)
OUT_PLOTS.mkdir(parents=True, exist_ok=True)

AGENTIC_COMMITS = DATA / "refactoring_dataset_commits.parquet"
AGENTIC_REFACTS = DATA / "refactoring_dataset_refactorings.parquet"
HUMAN_COMMITS   = DATA / "human_refactoring_commits.parquet"
HUMAN_REFACTS   = DATA / "baseline_refactorings.parquet"

plt.rcParams.update({"figure.dpi": 140})
sns.set_theme(style="whitegrid", context="talk")

# -------------------------- Load Data -----------------------
print("📦 Loading datasets...")
agentic = pd.read_parquet(AGENTIC_COMMITS)
agentic_ref = pd.read_parquet(AGENTIC_REFACTS)
human = pd.read_parquet(HUMAN_COMMITS)
human_ref = pd.read_parquet(HUMAN_REFACTS)

# Normalize commit SHAs
def _norm_sha(df, col):
    if col in df.columns:
        df[col] = df[col].astype(str).str.lower().str.strip()
for (df, col) in [(agentic,"sha"), (agentic_ref,"sha"), (human,"sha"), (human_ref,"commit_sha")]:
    _norm_sha(df, col)

# --------------------- Harmonize schemas --------------------
# Human refactorings: rename commit column, add agent & project
human_ref = human_ref.rename(columns={"commit_sha": "sha"})
human_ref = human_ref.merge(
    human[["sha", "full_name"]].drop_duplicates(), on="sha", how="left"
)
human_ref["agent"] = "Human"

# Agentic refactorings: make sure agent + repo columns exist
if "agent" not in agentic_ref.columns or "full_name" not in agentic_ref.columns:
    agentic_ref = agentic_ref.merge(
        agentic[["sha", "agent", "full_name"]].drop_duplicates(),
        on="sha", how="left"
    )

# -------------------- Combine & clean ------------------------
ref_events = pd.concat(
    [
        agentic_ref[["sha", "full_name", "agent", "refactoring_type"]].assign(dataset="Agentic"),
        human_ref[["sha", "full_name", "agent", "refactoring_type"]].assign(dataset="Human"),
    ],
    ignore_index=True
).dropna(subset=["refactoring_type"])

print(f"✅ Loaded {len(ref_events):,} total refactoring events from {ref_events['agent'].nunique()} agents.")

# -------------------- Counts & shares ------------------------
# (1) Count per agent × type
ref_types_by_agent = (
    ref_events.groupby(["agent", "refactoring_type"]).size()
              .reset_index(name="count")
)

# (2) Compute within-agent total and share percentage
ref_types_by_agent["agent_total"] = ref_types_by_agent.groupby("agent")["count"].transform("sum")
ref_types_by_agent["share_pct"] = ref_types_by_agent["count"] / ref_types_by_agent["agent_total"] * 100

# (3) Save sorted CSV
ref_types_by_agent.sort_values(["agent", "share_pct"], ascending=[True, False]).to_csv(
    OUT_TABLES / "refactor_types_by_agent_counts_and_share.csv", index=False
)
print("💾 Saved per-agent refactoring type counts and shares.")

# (4) Pivot for plotting
pivot = (
    ref_types_by_agent
    .pivot(index="refactoring_type", columns="agent", values="share_pct")
    .fillna(0)
)

# -------------------- Stacked bar plot (all types, full legend shown) -----------------------
import math

# Pivot data as before
pivot_agent_all = (
    ref_types_by_agent
    .pivot(index="agent", columns="refactoring_type", values="share_pct")
    .fillna(0)
)

# Sort agents and refactoring types by global frequency
agent_order = (
    ref_types_by_agent.groupby("agent")["count"]
    .sum()
    .sort_values(ascending=False)
    .index.tolist()
)
pivot_agent_all = pivot_agent_all.loc[agent_order]

type_order = (
    ref_types_by_agent.groupby("refactoring_type")["count"]
    .sum()
    .sort_values(ascending=False)
    .index.tolist()
)
pivot_agent_all = pivot_agent_all[type_order]

# Plot
fig, ax = plt.subplots(figsize=(18, 10))
pivot_agent_all.plot(
    kind="bar",
    stacked=True,
    colormap="tab20",
    width=0.7,
    ax=ax
)

ax.set_title("Refactoring Type Composition by Agent (All Types, Ordered by Frequency)")
ax.set_ylabel("Share of Agent Total (%)")
ax.set_xlabel("Agent")

# Place the legend in its own column to the right
# Automatically determine legend height by number of types
ncols = math.ceil(len(type_order) / 30)  # 30 rows per column
ax.legend(
    title="Refactoring Type (most → least common)",
    bbox_to_anchor=(1.02, 1),
    loc="upper left",
    ncol=ncols,
    fontsize="small",
    frameon=False
)

# Use tight bounding box to ensure legend and labels aren't cropped
plt.subplots_adjust(right=0.75)  # leaves room for legend
plt.savefig(
    OUT_PLOTS / "stacked_refactor_type_composition_by_agent_all_ordered.png",
    dpi=300,
    bbox_inches="tight"   # ensures *nothing* gets cut off
)
plt.close()

print("🎨 Saved full stacked bar chart with complete legend.")

# -------------------- Stacked bar plot (Top 15 Types + Other) -----------------------
print("🎨 Generating Top 15 + Other stacked bar plot (formatted to match main chart)...")

# 1️⃣ Select top 15 globally frequent types
top_types = (
    ref_events["refactoring_type"]
    .value_counts()
    .head(15)
    .index.tolist()
)

# 2️⃣ Group everything else into "Other"
ref_types_top15 = ref_types_by_agent.copy()
ref_types_top15["refactoring_type"] = ref_types_top15["refactoring_type"].apply(
    lambda t: t if t in top_types else "Other"
)

# 3️⃣ Reaggregate counts and shares per agent
ref_types_top15 = (
    ref_types_top15.groupby(["agent", "refactoring_type"], as_index=False)["count"].sum()
)
ref_types_top15["agent_total"] = ref_types_top15.groupby("agent")["count"].transform("sum")
ref_types_top15["share_pct"] = (
    ref_types_top15["count"] / ref_types_top15["agent_total"] * 100
)

# 4️⃣ Pivot and order
pivot_agent = (
    ref_types_top15
    .pivot(index="agent", columns="refactoring_type", values="share_pct")
    .fillna(0)
)

# Sort agents by total count (for consistency)
agent_order = (
    ref_types_by_agent.groupby("agent")["count"]
    .sum()
    .sort_values(ascending=False)
    .index.tolist()
)
pivot_agent = pivot_agent.loc[agent_order]

# Sort types by global frequency, but ensure "Other" is last
type_order = (
    ref_types_top15.groupby("refactoring_type")["count"]
    .sum()
    .sort_values(ascending=False)
    .index.tolist()
)
if "Other" in type_order:
    type_order = [t for t in type_order if t != "Other"] + ["Other"]

pivot_agent = pivot_agent[type_order]

# 5️⃣ Create stacked bar plot — same format as main "Top 15" plot
plt.figure(figsize=(14, 8))
pivot_agent.plot(
    kind="bar",
    stacked=True,
    colormap="tab20",
    width=0.7,
    ax=plt.gca()
)
plt.title("Refactoring Type Composition by Agent (Top 15 Types + Other)")
plt.ylabel("Share of Agent Total (%)")
plt.xlabel("Agent")
plt.legend(
    title="Refactoring Type",
    bbox_to_anchor=(1.05, 1),
    loc="upper left",
    ncol=1,           # single-column legend
    fontsize="medium",
    frameon=True,            # ✅ add this (or remove the line entirely)
    fancybox=True,           # ✅ optional: rounded corners
)
plt.tight_layout()
plt.savefig(OUT_PLOTS / "stacked_refactor_type_composition_by_agent_top15_other.png", dpi=300)
plt.close()

print("✅ Saved plot: stacked_refactor_type_composition_by_agent_top15_other.png")


# -------------------- Per-agent bar plots --------------------
for agent, grp in ref_types_by_agent.groupby("agent"):
    top = grp.sort_values("share_pct", ascending=False).head(10)
    plt.figure(figsize=(9, 5))
    sns.barplot(data=top, x="share_pct", y="refactoring_type", color="skyblue")
    plt.title(f"Top Refactoring Types — {agent}")
    plt.xlabel("Share of Agent Total (%)")
    plt.ylabel("Refactoring Type")
    plt.tight_layout()
    out_path = OUT_PLOTS / f"bar_refactor_types_{agent}.png"
    plt.savefig(out_path)
    plt.close()
    print(f"🎨 Saved plot: {out_path.name}")

# -------------------- Summary tables -------------------------
# Overall totals across all agents
ref_types_overall = (
    ref_events["refactoring_type"]
    .value_counts()
    .rename_axis("refactoring_type")
    .reset_index(name="count")
)
ref_types_overall.to_csv(OUT_TABLES / "refactor_types_overall_counts.csv", index=False)

print("✅ Analysis complete.")
print(f"📁 Tables: {OUT_TABLES}")
print(f"📊 Plots:  {OUT_PLOTS}")
