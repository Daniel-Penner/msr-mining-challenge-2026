#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_data.py
---------------
RQ1: What types of refactoring are agents doing, and how frequent is agent refactoring?

This script:
 - Normalizes Human refactoring rates using ALL PR commits (literature-comparable).
 - Deduplicates agentic commits by SHA (avoids inflating Claude, etc.).
 - Outputs rich CSV tables and clean plots that won’t be dominated by outliers.
 - Computes refactoring-type counts and SHARES (percent of total) for Human vs Agentic
   and per agent (optionally per project).

Inputs (relative to repo root):
  data/processed/refactoring_dataset_commits.parquet
  data/processed/refactoring_dataset_refactorings.parquet
  data/processed/human_refactoring_commits.parquet
  data/processed/baseline_refactorings.parquet
  data/processed/java_baseline_pr_commits.parquet

Outputs:
  outputs/tables/*.csv
  outputs/plots/*.png
  outputs/analysis_summary.txt
"""

from pathlib import Path
import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# -------------------------- Setup & Paths --------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # scripts/analysis_scripts -> repo root
DATA = PROJECT_ROOT / "data" / "processed"
OUT_TABLES = PROJECT_ROOT / "outputs" / "tables"
OUT_PLOTS = PROJECT_ROOT / "outputs" / "plots"
OUT_TEXT = PROJECT_ROOT / "outputs" / "analysis_summary.txt"
OUT_TABLES.mkdir(parents=True, exist_ok=True)
OUT_PLOTS.mkdir(parents=True, exist_ok=True)

AGENTIC_COMMITS = DATA / "refactoring_dataset_commits.parquet"
AGENTIC_REFACTS = DATA / "refactoring_dataset_refactorings.parquet"
HUMAN_COMMITS   = DATA / "human_refactoring_commits.parquet"
HUMAN_REFACTS   = DATA / "baseline_refactorings.parquet"
HUMAN_PR_COMMITS = DATA / "java_baseline_pr_commits.parquet"

plt.rcParams.update({"figure.dpi": 140})
sns.set_theme(style="whitegrid", context="talk")

# -------------------------- Load Data -----------------------------
print("📦 Loading datasets...")
agentic = pd.read_parquet(AGENTIC_COMMITS)
agentic_ref = pd.read_parquet(AGENTIC_REFACTS)
human = pd.read_parquet(HUMAN_COMMITS)
human_ref = pd.read_parquet(HUMAN_REFACTS)
human_pr = pd.read_parquet(HUMAN_PR_COMMITS)

# Normalize SHAs
def _norm_sha(df, col):
    if col in df.columns:
        df[col] = df[col].astype(str).str.lower().str.strip()
for (df, col) in [(agentic,"sha"), (agentic_ref,"sha"), (human,"sha"),
                  (human_ref,"commit_sha"), (human_pr,"sha")]:
    _norm_sha(df, col)

# ---------------------- Deduplicate Agentic -----------------------
before = len(agentic)
agentic = agentic.drop_duplicates(subset=["sha"], keep="first")
removed = before - len(agentic)
if removed > 0:
    print(f"🧹 Deduped agentic commits by SHA: removed {removed} rows.")

# ---------------------- Human normalization -----------------------
human_pr_total = human_pr["sha"].nunique()
human_analyzed_total = human["sha"].nunique()
human_analyzed_with_ref = int(human["has_refactoring"].sum())

print(f"✅ Agentic commits: {len(agentic):,} | agents={agentic['agent'].nunique()}")
print(f"✅ Human analyzed commits: {len(human):,} (unique={human_analyzed_total:,})")
print(f"✅ Human PR commits (denominator): {human_pr_total:,}")

# ---------------------- Harmonize Schemas -------------------------
agentic["dataset"] = "Agentic"
human["dataset"] = "Human"
for df in (agentic, human):
    df["refactoring_count"] = df["refactoring_count"].fillna(0).astype(int)
    df["has_refactoring"] = df["has_refactoring"].fillna(False)

# Enrich refactor-event tables
# Human events: commit_sha -> sha, join project & agent label
human_ref = human_ref.rename(columns={"commit_sha":"sha"})
human_ref = human_ref.merge(
    human[["sha","full_name"]].drop_duplicates(),
    on="sha", how="left"
)
human_ref["agent"] = "Human"

# Agentic events: ensure agent & full_name present
if "agent" not in agentic_ref.columns or "full_name" not in agentic_ref.columns:
    agentic_ref = agentic_ref.merge(agentic[["sha","agent","full_name"]].drop_duplicates(),
                                    on="sha", how="left")

# ---------------------- Combined & Summary ------------------------
combined = pd.concat([agentic, human], ignore_index=True)
total_commits = combined["sha"].nunique()
ref_commits = int(combined["has_refactoring"].sum())

# Summary text collector
lines = []
def log_line(s): print(s); lines.append(s)

log_line("\n=== 🌍 GLOBAL SUMMARY ===")
log_line(f"Total unique commits: {total_commits:,}")
log_line(f"Commits with refactoring (observed): {ref_commits:,} ({ref_commits/total_commits*100:.2f}%)")
log_line(f"Agents detected: {combined['agent'].nunique()}")

# ----------------- Per-Agent (commit-level) summary ---------------
per_agent = (
    combined.groupby("agent", dropna=False)
    .agg(
        total_commits=("sha", "nunique"),
        refactoring_commits=("has_refactoring", "sum"),
        mean_refactorings=("refactoring_count", "mean"),
        median_refactorings=("refactoring_count", "median"),
        std_refactorings=("refactoring_count", "std"),
        max_refactorings=("refactoring_count", "max"),
    )
    .reset_index()
)
per_agent["refactoring_rate_%"] = per_agent["refactoring_commits"] / per_agent["total_commits"] * 100

human_norm_rate = (human_analyzed_with_ref / human_pr_total * 100) if human_pr_total else 0.0
log_line("\n=== ⚖️ HUMAN NORMALIZATION CHECK ===")
if human_analyzed_total:
    log_line(f"Subset (analyzed only): {human_analyzed_with_ref}/{human_analyzed_total} = {human_analyzed_with_ref/human_analyzed_total*100:.2f}%")
else:
    log_line("Subset (analyzed only): n/a")
log_line(f"Normalized to ALL PR commits: {human_analyzed_with_ref}/{human_pr_total} = {human_norm_rate:.2f}%  ← use this in papers")

per_agent.sort_values("agent").to_csv(OUT_TABLES / "per_agent_summary.csv", index=False)

# ---------------- Per-Project (agent × repo) summary --------------
# Agentic per-project (observed denominator)
agentic_proj = (
    agentic.groupby(["agent","full_name"], dropna=False)
    .agg(total_commits=("sha","nunique"),
         refactoring_commits=("has_refactoring","sum"),
         mean_refactorings=("refactoring_count","mean"),
         median_refactorings=("refactoring_count","median"))
    .reset_index()
)
agentic_proj["refactoring_rate_%"] = agentic_proj["refactoring_commits"] / agentic_proj["total_commits"] * 100
agentic_proj["denominator"] = "Observed agentic commits"

# HUMAN per-project: denominator = ALL PR commits in that project
human_pr_proj = human_pr.groupby("full_name", dropna=False)["sha"].nunique().rename("total_commits_pr").reset_index()
human_proj_observed = (
    human.groupby("full_name", dropna=False)
         .agg(refactoring_commits=("has_refactoring","sum"),
              analyzed_commits=("sha","nunique"),
              mean_refactorings=("refactoring_count","mean"),
              median_refactorings=("refactoring_count","median"))
         .reset_index()
)
human_proj = human_pr_proj.merge(human_proj_observed, on="full_name", how="left").fillna(
    {"refactoring_commits":0, "analyzed_commits":0, "mean_refactorings":0, "median_refactorings":0}
)
human_proj["agent"] = "Human"
human_proj["refactoring_rate_%"] = human_proj["refactoring_commits"] / human_proj["total_commits_pr"] * 100
human_proj = human_proj.rename(columns={"total_commits_pr":"total_commits"})
human_proj["denominator"] = "All PR commits (normalized)"

per_project = pd.concat([
    agentic_proj[["agent","full_name","total_commits","refactoring_commits","mean_refactorings","median_refactorings","refactoring_rate_%","denominator"]],
    human_proj[["agent","full_name","total_commits","refactoring_commits","mean_refactorings","median_refactorings","refactoring_rate_%","denominator"]]
], ignore_index=True)

per_project.to_csv(OUT_TABLES / "per_project_summary.csv", index=False)

# ----------- Refactoring types: counts & shares -------------------
ref_events = pd.concat([
    agentic_ref[["sha","full_name","agent","refactoring_type"]].assign(dataset="Agentic"),
    human_ref[["sha","full_name","agent","refactoring_type"]].assign(dataset="Human")
], ignore_index=True).dropna(subset=["refactoring_type"])

# Overall counts by type
ref_types_overall = (
    ref_events["refactoring_type"]
    .value_counts()
    .rename_axis("refactoring_type")
    .reset_index(name="count")
)
ref_types_overall.to_csv(OUT_TABLES / "refactor_types_overall.csv", index=False)

# Counts by dataset (Human vs Agentic)
ref_types_by_dataset = (
    ref_events.groupby(["dataset","refactoring_type"]).size()
              .reset_index(name="count")
)
# Percent of total within dataset
ref_types_by_dataset["dataset_total"] = ref_types_by_dataset.groupby("dataset")["count"].transform("sum")
ref_types_by_dataset["share_pct"] = ref_types_by_dataset["count"] / ref_types_by_dataset["dataset_total"] * 100
ref_types_by_dataset.sort_values(["dataset","share_pct"], ascending=[True, False]).to_csv(
    OUT_TABLES / "refactor_types_by_dataset_counts_and_share.csv", index=False
)

# Counts & shares per agent
ref_types_by_agent = (
    ref_events.groupby(["agent","refactoring_type"]).size()
              .reset_index(name="count")
)
ref_types_by_agent["agent_total"] = ref_types_by_agent.groupby("agent")["count"].transform("sum")
ref_types_by_agent["share_pct"] = ref_types_by_agent["count"] / ref_types_by_agent["agent_total"] * 100
ref_types_by_agent.sort_values(["agent","share_pct"], ascending=[True, False]).to_csv(
    OUT_TABLES / "refactor_types_by_agent_counts_and_share.csv", index=False
)

# Agent × project × type (big table, useful for drilling down)
ref_types_by_agent_project = (
    ref_events.groupby(["agent","full_name","refactoring_type"]).size()
              .reset_index(name="count")
)
ref_types_by_agent_project.to_csv(OUT_TABLES / "refactor_types_by_agent_project.csv", index=False)

# ------------- Extra commit-level distributions (by agent) --------
commit_dist = (
    combined.groupby("agent")["refactoring_count"]
    .describe(percentiles=[0.25,0.5,0.75])
    .round(2)
)
commit_dist.to_csv(OUT_TABLES / "commit_level_distribution_by_agent.csv")

log_line("\n✅ Tables written to: " + str(OUT_TABLES))

# ------------------------------ Plots ------------------------------
print("🎨 Generating plots...")
cap_val = combined["refactoring_count"].quantile(0.99)
combined["refactor_count_capped"] = np.minimum(combined["refactoring_count"], cap_val)

# 1) Boxplot raw (log)
plt.figure(figsize=(12,6))
sns.boxplot(data=combined, x="agent", y="refactoring_count", showfliers=False)
plt.yscale("log")
plt.title("Refactorings per Commit by Agent (log scale)")
plt.xlabel("Agent")
plt.ylabel("Refactorings per Commit (log)")
plt.tight_layout()
plt.savefig(OUT_PLOTS / "box_refactorings_per_commit_log.png")

# 2) Boxplot capped (99th percentile)
plt.figure(figsize=(12,6))
sns.boxplot(data=combined, x="agent", y="refactor_count_capped", showfliers=False)
plt.title(f"Refactorings per Commit by Agent (capped at 99th pct = {int(cap_val)})")
plt.xlabel("Agent")
plt.ylabel("Refactorings per Commit (capped)")
plt.tight_layout()
plt.savefig(OUT_PLOTS / "box_refactorings_per_commit_capped.png")

# 3) Violin — per-project refactoring rate (Human normalized)
agentic_proj_for_plot = agentic_proj[["agent","full_name","refactoring_rate_%"]].rename(columns={"refactoring_rate_%":"rate"})
human_proj_for_plot = human_proj[["agent","full_name","refactoring_rate_%"]].rename(columns={"refactoring_rate_%":"rate"})
proj_rates_plot = pd.concat([agentic_proj_for_plot, human_proj_for_plot], ignore_index=True)

plt.figure(figsize=(12,6))
sns.violinplot(data=proj_rates_plot, x="agent", y="rate", inner="quart", cut=0)
plt.title("Per-Project Refactoring Rate by Agent (% of commits in project)")
plt.xlabel("Agent")
plt.ylabel("Refactoring Rate (%)")
plt.tight_layout()
plt.savefig(OUT_PLOTS / "violin_project_refactoring_rate.png")

# 4) Histogram — distribution, density‐normalized, log y
plt.figure(figsize=(12,7))
sns.histplot(data=combined, x="refactoring_count", hue="dataset",
             bins=60, element="step", stat="density", common_norm=True, alpha=0.35, log_scale=(False, True))
plt.title("Distribution of Refactoring Counts per Commit (density, log y)")
plt.xlabel("Refactorings per Commit")
plt.ylabel("Density (log y)")
plt.tight_layout()
plt.savefig(OUT_PLOTS / "hist_refactoring_counts_normed.png")

# 5) Mean bar — mean refactorings per commit by agent
plt.figure(figsize=(12,6))
mean_df = per_agent.sort_values("mean_refactorings", ascending=False)
sns.barplot(data=mean_df, x="agent", y="mean_refactorings")
plt.title("Mean Refactorings per Commit by Agent")
plt.xlabel("Agent")
plt.ylabel("Mean Refactorings per Commit")
plt.tight_layout()
plt.savefig(OUT_PLOTS / "bar_mean_refactorings_per_agent.png")

# 6) CDF — cumulative distribution of refactorings per commit by agent
plt.figure(figsize=(12,7))
for a, grp in combined.groupby("agent"):
    vals = np.sort(grp["refactoring_count"].values)
    if len(vals) == 0: 
        continue
    y = np.arange(1, len(vals)+1) / len(vals)
    plt.plot(vals, y, label=a)
plt.xscale("log")
plt.xlabel("Refactorings per Commit (log)")
plt.ylabel("Cumulative Fraction of Commits")
plt.title("CDF of Refactorings per Commit by Agent")
plt.legend()
plt.tight_layout()
plt.savefig(OUT_PLOTS / "cdf_refactorings_per_commit.png")

# 7) Agentic vs Human (box, log y)
cmp_df = combined.copy()
cmp_df["group"] = np.where(cmp_df["agent"]=="Human", "Human", "Agentic")
plt.figure(figsize=(9,6))
sns.boxplot(data=cmp_df, x="group", y="refactoring_count", showfliers=False)
plt.yscale("log")
plt.title("Refactorings per Commit: Agentic vs Human")
plt.xlabel("")
plt.ylabel("Refactorings per Commit (log)")
plt.tight_layout()
plt.savefig(OUT_PLOTS / "box_agentic_vs_human_log.png")

# 8) Refactoring type distribution — percent of total per dataset (Human vs Agentic)
types_plot = (ref_types_by_dataset
              .pivot(index="refactoring_type", columns="dataset", values="share_pct")
              .fillna(0)
              .sort_values("Human", ascending=False))
plt.figure(figsize=(14,8))
types_plot.head(25).plot(kind="bar")
plt.title("Top Refactoring Types — Share of Total (%) by Dataset")
plt.ylabel("Share of Dataset Total (%)")
plt.xlabel("Refactoring Type")
plt.legend(title="")
plt.tight_layout()
plt.savefig(OUT_PLOTS / "bar_refactor_types_share_human_vs_agentic.png")
plt.close()

# 9) Stacked shares per dataset (top 15 types)
top_types = (ref_types_overall.head(15)["refactoring_type"].tolist()
             if len(ref_types_overall) > 15 else ref_types_overall["refactoring_type"].tolist())
stacked = (ref_types_by_dataset[ref_types_by_dataset["refactoring_type"].isin(top_types)]
           .pivot(index="refactoring_type", columns="dataset", values="share_pct")
           .fillna(0))
stacked = stacked.loc[top_types]
plt.figure(figsize=(12,7))
stacked.plot(kind="bar", stacked=True, colormap="tab20")
plt.title("Refactoring Type Mix — Human vs Agentic (share of dataset total, top types)")
plt.ylabel("Share of Dataset Total (%)")
plt.xlabel("Refactoring Type")
plt.tight_layout()
plt.savefig(OUT_PLOTS / "stacked_refactor_types_share_top.png")
plt.close()

print("✅ Plots written to:", OUT_PLOTS)

# ------------------------- (Optional) Stats ------------------------
try:
    from scipy.stats import kruskal, mannwhitneyu
    # Kruskal–Wallis across agents
    groups = [g["refactoring_count"].values for _, g in combined.groupby("agent")]
    if all(len(g) > 0 for g in groups) and len(groups) > 1:
        H, p_kw = kruskal(*groups)
        log_line(f"\n📈 Kruskal–Wallis across agents: H={H:.2f}, p={p_kw:.4g}")

    # Pairwise: Human vs each agent
    hum_vals = combined.loc[combined["agent"]=="Human","refactoring_count"].values
    log_line("\n📊 Pairwise Mann–Whitney U (Human vs Agent):")
    for a in sorted([x for x in combined["agent"].unique() if x != "Human"]):
        ag_vals = combined.loc[combined["agent"]==a,"refactoring_count"].values
        if len(ag_vals)==0 or len(hum_vals)==0:
            continue
        U, p = mannwhitneyu(ag_vals, hum_vals, alternative="two-sided")
        log_line(f"  {a:>12s} : U={U:.0f}, p={p:.4g}")
except Exception:
    log_line("\n(Stats skipped — install scipy to enable tests)")

# -------------------------- Save Summary --------------------------
OUT_TEXT.write_text("\n".join(lines), encoding="utf-8")
print(f"\n📝 Summary written to: {OUT_TEXT}")
print("\n✅ Analysis complete.")
