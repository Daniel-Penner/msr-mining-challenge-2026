from pathlib import Path
import pandas as pd

# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "processed"

# Use the PR commit dataset (commits analyzed by RefMiner)
BASELINE_COMMITS = DATA_DIR / "baseline_pr_commits.parquet"
HUMAN_REFACTORING_COMMITS = DATA_DIR / "human_refactoring_commits.parquet"
UPDATED_HUMAN = DATA_DIR / "human_refactoring_commits_normalized.parquet"

# ---------------------------------------------------------
# Load data
# ---------------------------------------------------------
print("📦 Loading baseline commits...")
baseline_df = pd.read_parquet(BASELINE_COMMITS)
print(f"Baseline commits: {len(baseline_df):,}")

print("📦 Loading existing human refactoring commits...")
human_df = pd.read_parquet(HUMAN_REFACTORING_COMMITS)
print(f"Existing human refactoring commits: {len(human_df):,}")

# ---------------------------------------------------------
# Ensure required columns exist in human_df
# ---------------------------------------------------------
for col, default in {
    "has_refactoring": False,
    "refactoring_types": [],
    "refactoring_count": 0,
}.items():
    if col not in human_df.columns:
        print(f"⚠️ Missing column '{col}' in human dataset — creating default values.")
        human_df[col] = [default for _ in range(len(human_df))]

# ---------------------------------------------------------
# Align commit column name
# ---------------------------------------------------------
if "commit" in baseline_df.columns and "sha" not in baseline_df.columns:
    baseline_df = baseline_df.rename(columns={"commit": "sha"})
if "commit" in human_df.columns and "sha" not in human_df.columns:
    human_df = human_df.rename(columns={"commit": "sha"})

# ---------------------------------------------------------
# Identify commits missing from the refactoring dataset
# ---------------------------------------------------------
missing = baseline_df[~baseline_df["sha"].isin(human_df["sha"])]
print(f"🧩 Commits missing from human_refactoring_commits: {len(missing):,}")

# ---------------------------------------------------------
# Create placeholder entries for missing commits
# ---------------------------------------------------------
required_cols = list(human_df.columns)
carry_cols = [c for c in baseline_df.columns if c in required_cols]

missing_df = missing[carry_cols].copy()

# Fill missing required columns with defaults
for col in required_cols:
    if col not in missing_df.columns:
        if col == "has_refactoring":
            missing_df[col] = False
        elif col == "refactoring_types":
            missing_df[col] = [[] for _ in range(len(missing_df))]
        elif col == "refactoring_count":
            missing_df[col] = 0
        else:
            missing_df[col] = pd.NA

# ---------------------------------------------------------
# Merge and clean
# ---------------------------------------------------------
updated_df = pd.concat([human_df, missing_df], ignore_index=True)
updated_df["has_refactoring"] = updated_df["has_refactoring"].fillna(False).astype(bool)
updated_df["refactoring_count"] = updated_df["refactoring_count"].fillna(0).astype(int)

# Normalize type column if it exists
if "refactoring_types" in updated_df.columns:
    updated_df["refactoring_types"] = updated_df["refactoring_types"].apply(
        lambda x: x if isinstance(x, list) else []
    )

# ---------------------------------------------------------
# Save normalized dataset
# ---------------------------------------------------------
updated_df.to_parquet(UPDATED_HUMAN, index=False)
print(f"✅ Normalized file saved: {UPDATED_HUMAN}")
print(f"Total commits after normalization: {len(updated_df):,}")
print(f"Refactoring rate: {(updated_df['has_refactoring'].mean() * 100):.2f}%")
