from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "processed"
OUT_DIR = PROJECT_ROOT / "outputs"
PLOTS_DIR = OUT_DIR / "plots"
TABLES_DIR = OUT_DIR / "tables"
for d in (OUT_DIR, PLOTS_DIR, TABLES_DIR):
    d.mkdir(parents=True, exist_ok=True)

def load_datasets():
    agentic = pd.read_parquet(DATA_DIR / "agentic_refactoring_commits.parquet")
    human = pd.read_parquet(DATA_DIR / "baseline_refactoring_commits_normalized.parquet")
    human_pr = pd.read_parquet(DATA_DIR / "baseline_pr_commits.parquet")
    human["dataset"] = "Human"
    agentic["dataset"] = "Agentic"
    df = pd.concat([agentic, human], ignore_index=True)
    df["refactoring_count"] = df["refactoring_count"].fillna(0).astype(int)
    return df
