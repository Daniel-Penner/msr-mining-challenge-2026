# scripts/analysis_scripts/refactoring_rate_per_project.py

import pandas as pd
import matplotlib.pyplot as plt
from dataloader import load_datasets, PLOTS_DIR, TABLES_DIR

# ---------------------------------------------------------------
# Load datasets
# ---------------------------------------------------------------
df = load_datasets()

agentic = df[df["dataset"] == "Agentic"]
human = df[df["dataset"] == "Human"]

# ---------------------------------------------------------------
# Per-project (agent × repo) summary
# ---------------------------------------------------------------
def summarize_per_project(sub_df, label):
    proj = (
        sub_df.groupby(["agent", "full_name"], dropna=False)
        .agg(
            total_commits=("sha", "nunique"),
            refactoring_commits=("has_refactoring", "sum"),
            total_refactorings=("refactoring_count", "sum"),
            mean_refactorings=("refactoring_count", "mean"),
            median_refactorings=("refactoring_count", "median"),
        )
        .reset_index()
    )
    proj["refactoring_rate_%"] = (
        proj["refactoring_commits"] / proj["total_commits"] * 100
    )
    proj["refactors_per_all_commits"] = (
        proj["total_refactorings"] / proj["total_commits"]
    )
    proj["refactors_per_refactoring_commit"] = proj.apply(
        lambda r: r["total_refactorings"] / r["refactoring_commits"]
        if r["refactoring_commits"] > 0 else 0,
        axis=1,
    )
    proj["denominator"] = label
    return proj


agentic_proj = summarize_per_project(agentic, "Observed agentic commits")
human_proj = summarize_per_project(human, "Observed human commits")

proj_summary = pd.concat([agentic_proj, human_proj], ignore_index=True)
proj_summary.to_csv(TABLES_DIR / "per_project_refactoring_rate.csv", index=False)

# ---------------------------------------------------------------
# Compute descriptive statistics
# ---------------------------------------------------------------
stats = (
    proj_summary.groupby("agent")[[
        "refactoring_rate_%",
        "refactors_per_all_commits",
        "refactors_per_refactoring_commit"
    ]]
    .agg(["count", "mean", "median", "std", "min", "max"])
)

stats.to_csv(TABLES_DIR / "per_agent_refactoring_stats.csv")

print("\n📊 Per-Agent Refactoring Statistics:")
print(stats.round(3).to_string())

# ---------------------------------------------------------------
# Create and save boxplots
# ---------------------------------------------------------------
agents = sorted(proj_summary["agent"].dropna().unique())

def make_boxplot(metric_col, title, ylabel, filename, log_scale=False):
    box_data = [
        proj_summary.loc[proj_summary["agent"] == a, metric_col].dropna()
        for a in agents
    ]
    plt.figure(figsize=(10, 5))
    plt.boxplot(box_data, showfliers=False, tick_labels=agents)
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xlabel("Agent")
    if log_scale:
        plt.yscale("log")
    plt.grid(axis="y", linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / filename, dpi=300)
    plt.close()
    print(f"✅ Saved plot: {filename}")


# 1️⃣ Refactoring rate (%)
make_boxplot(
    "refactoring_rate_%",
    "Per-Project Refactoring Commit Rate by Agent",
    "Refactoring Commit Rate (%)",
    "box_refactoring_rate_per_project.png",
)

# 2️⃣ Refactors per all commits
make_boxplot(
    "refactors_per_all_commits",
    "Per-Project Refactors per All Commits by Agent (log scale)",
    "Refactors per Commit (log)",
    "box_refactors_per_all_commits_per_project.png",
    log_scale=True,
)

# 3️⃣ Refactors per refactoring commit
make_boxplot(
    "refactors_per_refactoring_commit",
    "Per-Project Refactors per Refactoring Commit by Agent (log scale)",
    "Refactors per Refactoring Commit (log)",
    "box_refactors_per_refactoring_commit_per_project.png",
    log_scale=True,
)


print("\n✅ All plots and statistical summaries saved successfully.")
