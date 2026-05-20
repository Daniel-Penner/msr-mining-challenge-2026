from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# --- Setup Paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "processed"
OUT_DIR = PROJECT_ROOT / "outputs"
PLOTS_DIR = OUT_DIR / "plots"
TABLES_DIR = OUT_DIR / "tables"

for d in (OUT_DIR, PLOTS_DIR, TABLES_DIR):
    d.mkdir(parents=True, exist_ok=True)

# --- Helper Functions for Confidence Intervals ---
def bootstrap_mean(data, n_boot=2000, ci=95):
    if len(data) == 0: return np.nan, np.nan
    boot_means = [np.mean(np.random.choice(data, size=len(data), replace=True)) for _ in range(n_boot)]
    return np.percentile(boot_means, (100 - ci) / 2), np.percentile(boot_means, 100 - (100 - ci) / 2)

def bootstrap_proportion(successes, total, n_boot=2000, ci=95):
    if total == 0: return np.nan, np.nan
    observations = np.array([1] * int(successes) + [0] * int(total - successes))
    boot_props = [np.mean(np.random.choice(observations, size=len(observations), replace=True)) for _ in range(n_boot)]
    return np.percentile(boot_props, (100 - ci) / 2) * 100, np.percentile(boot_props, 100 - (100 - ci) / 2) * 100

# --- 1. Load Datasets and Calculate Ground Truth Counts ---
def load_and_fix_counts():

    
    # 1. Load Commits
    agentic_commits = pd.read_parquet(DATA_DIR / "agentic_refactoring_commits.parquet")
    human_commits = pd.read_parquet(DATA_DIR / "baseline_refactoring_commits_normalized.parquet")
    
    # 2. Load Raw Refactoring Events
    agentic_events = pd.read_parquet(DATA_DIR / "agentic_refactorings.parquet")
    human_events = pd.read_parquet(DATA_DIR / "baseline_refactorings.parquet")

    # 3. Defensive Renaming (Fixes the KeyError: 'sha')
    # We ensure every dataframe has a 'sha' column regardless of its source naming
    def ensure_sha_column(df, name):
        if 'sha' in df.columns:
            pass 
        elif 'commit_sha' in df.columns:
            df = df.rename(columns={'commit_sha': 'sha'})
        else:
            print(f"⚠️ Warning: Could not find SHA column in {name}. Columns: {df.columns.tolist()}")
        
        # Clean the SHA values immediately
        if 'sha' in df.columns:
            df['sha'] = df['sha'].astype(str).str.lower().str.strip()
        return df

    agentic_commits = ensure_sha_column(agentic_commits, "agentic_commits")
    human_commits = ensure_sha_column(human_commits, "human_commits")
    agentic_events = ensure_sha_column(agentic_events, "agentic_events")
    human_events = ensure_sha_column(human_events, "human_events")


    agent_counts = agentic_events.groupby("sha").size().reset_index(name="true_count")
    human_counts = human_events.groupby("sha").size().reset_index(name="true_count")

    # Merge True Counts back into Commit Dataframes
    agentic_commits = agentic_commits.merge(agent_counts, on="sha", how="left").fillna({"true_count": 0})
    human_commits = human_commits.merge(human_counts, on="sha", how="left").fillna({"true_count": 0})

    # Override the old columns with Ground Truth
    agentic_commits["refactoring_count"] = agentic_commits["true_count"].astype(int)
    human_commits["refactoring_count"] = human_commits["true_count"].astype(int)
    
    # Update boolean flags
    agentic_commits["has_refactoring"] = agentic_commits["refactoring_count"] > 0
    human_commits["has_refactoring"] = human_commits["refactoring_count"] > 0

    agentic_commits["dataset"] = "Agentic"
    human_commits["dataset"] = "Human"

    df_combined = pd.concat([agentic_commits, human_commits], ignore_index=True)
    return df_combined, agentic_events

df, agent_events = load_and_fix_counts()

# --- 2. Statistical Summary Table ---

summary_rows = []

for agent, group in df.groupby("agent", dropna=False):
    total_commits = group["sha"].nunique()
    ref_commits_count = group["has_refactoring"].sum()
    total_refactorings_count = group["refactoring_count"].sum()
    
    rate_low, rate_high = bootstrap_proportion(ref_commits_count, total_commits)
    
    ref_only_data = group[group["has_refactoring"]]["refactoring_count"].values
    mean_val = np.mean(ref_only_data) if len(ref_only_data) > 0 else 0
    mean_low, mean_high = bootstrap_mean(ref_only_data)
    
    summary_rows.append({
        "Agent": agent,
        "Total Commits": total_commits,
        "Ref. Commits (n)": ref_commits_count,
        "Total Refactorings": int(total_refactorings_count),
        "Ref. Rate %": round(ref_commits_count / total_commits * 100, 2),
        "Rate 95% CI": f"[{rate_low:.2f}, {rate_high:.2f}]",
        "Mean Ref/Commit": round(mean_val, 2),
        "Mean 95% CI": f"[{mean_low:.2f}, {mean_high:.2f}]",
        "Median": int(np.median(ref_only_data)) if len(ref_only_data) > 0 else 0,
        "Max": int(np.max(ref_only_data)) if len(ref_only_data) > 0 else 0
    })

df_ci_summary = pd.DataFrame(summary_rows)
df_ci_summary.to_csv(TABLES_DIR / "per_agent_statistical_summary_FINAL.csv", index=False)

print("\nTable: Corrected Statistical Summary")
print(df_ci_summary.to_string(index=False))


print(f"\n✅ Processing complete. Ground truth tables generated in: {TABLES_DIR}")