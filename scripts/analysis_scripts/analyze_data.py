#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os

# === Load the dataset ===
df = pd.read_parquet("data/processed/refactoring_dataset_refactorings.parquet")
df2 = pd.read_parquet("data\/processed/refactoring_dataset_commits_deduped.parquet")

print("=== Columns ===")
print(df.columns.tolist())
print("Total rows:", len(df))

# === Create directory for plots ===
os.makedirs("plots", exist_ok=True)

# === Aggregate refactoring data per commit ===
commit_stats = (
    df.groupby(["sha","agent"])
    .size()
    .reset_index(name="refactoring_count")
)

print("\n=== Aggregated commit-level dataset ===")
print(commit_stats.head())

# Compute per-project refactoring rates
project_rates = (
    df2.groupby(["agent", "full_name"])
    .agg(
        total_commits=("sha", "nunique"),
        refactoring_commits=("has_refactoring", "sum")
    )
    .reset_index()
)
project_rates["refactoring_rate"] = (
    project_rates["refactoring_commits"] / project_rates["total_commits"] * 100
)
project_rates.loc[project_rates["refactoring_rate"] > 100, "refactoring_rate"] = 100.0  # cap invalids

proj_counts = project_rates["agent"].value_counts()
print("\nProjects per agent:\n", proj_counts)

# === Compute summary statistics per agent ===
stats = (
    commit_stats.groupby("agent")["refactoring_count"]
    .agg(["count", "mean", "median", "std", "min", "max"])
    .sort_values("mean", ascending=False)
)
print("\n=== Summary statistics per agent ===")
print(stats)

summary = (
    df2.groupby("agent")
    .agg(
        total_commits=("sha", "nunique"),
        refactoring_commits=("has_refactoring", "sum"),
        avg_refactorings=("refactoring_count", "mean"),
        median_refactorings=("refactoring_count", "median"),
    )
    .reset_index()
)
summary["refactoring_rate"] = summary["refactoring_commits"] / summary["total_commits"]

print("\n=== Summary per Agent ===")
print(summary.sort_values("refactoring_rate", ascending=False).round(3))

print(project_rates.head())
print("\n=== Mean refactoring rate per agent ===")
summary_stats = project_rates.groupby("agent")["refactoring_rate"].agg(["mean", "median", "std"])
print(summary_stats.round(2))

valid_agents = project_rates["agent"].value_counts()[lambda x: x >= 3].index
filtered_rates = project_rates[project_rates["agent"].isin(valid_agents)]

valid_agents = project_rates["agent"].value_counts()[lambda x: x >= 3].index
filtered_stats = commit_stats[commit_stats["agent"].isin(valid_agents)]

# === Boxplots ===
plt.figure(figsize=(8, 5))
filtered_stats.boxplot(column="refactoring_count", by="agent", grid=False)
plt.title("Refactoring Count per Commit by Agent")
plt.suptitle("")
plt.ylabel("Number of Refactorings")
plt.xlabel("Agent")
plt.yscale('log')
plt.tight_layout()
plt.savefig("plots/boxplot_refactorings_per_agent.png")
print("[SAVED] plots/boxplot_refactorings_per_agent.png")

plt.figure(figsize=(8, 5))
filtered_rates.boxplot(column="refactoring_rate", by="agent", grid=False)
plt.title("Refactoring Rate per Project by Agent")
plt.suptitle("")
plt.ylabel("Refactoring Rate (% of Commits per Project)")
plt.xlabel("Agent")
plt.tight_layout()
plt.savefig("plots/boxplot_refactoring_rate_per_project.png")
print("[SAVED] plots/boxplot_refactoring_rate_per_project.png")

plt.figure(figsize=(8,6))
sns.violinplot(x="agent", y="refactoring_rate", data=filtered_rates)
plt.title("Refactoring Rate Distribution per Project by Agent")
plt.ylabel("Refactoring Rate (%)")
plt.xlabel("Agent")
plt.tight_layout()
plt.savefig("plots/violin_refactoring_rate_per_project.png")

print("\nAll plots saved in ./plots/")
