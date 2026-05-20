import pandas as pd
from pathlib import Path
from collections import Counter
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "processed"
TEMP_DIR = DATA_DIR / "designite_temp"
TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"

TABLES_DIR.mkdir(parents=True, exist_ok=True)

AGENTIC_COMMITS = DATA_DIR / "agentic_refactoring_commits.parquet"
HUMAN_COMMITS = DATA_DIR / "baseline_refactoring_commits.parquet"

logging.basicConfig(level=logging.INFO, format="%(message)s")

def get_smell_column(df: pd.DataFrame):
    for c in df.columns:
        lc = c.lower()
        if "smell" in lc and "id" not in lc:
            return c
    return None


agentic = pd.read_parquet(AGENTIC_COMMITS)
human = pd.read_parquet(HUMAN_COMMITS)

agentic["dataset"] = "Agentic"
human["dataset"] = "Human"

commits = pd.concat([agentic, human], ignore_index=True)
commits = commits[commits["has_refactoring"] == True]

commit_dataset = {
    row["sha"][:8]: row["dataset"]
    for _, row in commits.iterrows()
}

by_commit = {}

for d in TEMP_DIR.iterdir():
    if not d.is_dir():
        continue

    parts = d.name.split("_")
    if len(parts) < 3:
        continue

    sha = parts[-2]
    stage = parts[-1]

    if stage not in {"before", "after"}:
        continue

    by_commit.setdefault(sha, {})[stage] = d


#Count smells
introduced = {
    "Agentic": Counter(),
    "Human": Counter(),
}

for sha, stages in by_commit.items():
    if "before" not in stages or "after" not in stages:
        continue
    if sha not in commit_dataset:
        continue

    dataset = commit_dataset[sha]

    def extract_smells(folder: Path) -> set[str]:
        smells = set()
        for csv in folder.glob("*.csv"):
            if "Metric" in csv.name or "Summary" in csv.name:
                continue
            try:
                df = pd.read_csv(csv)
            except Exception:
                continue

            smell_col = get_smell_column(df)
            if smell_col is None:
                continue

            smells.update(df[smell_col].dropna().astype(str).str.strip())

        return smells

    before_smells = extract_smells(stages["before"])
    after_smells = extract_smells(stages["after"])

    introduced_smells = after_smells - before_smells

    for smell in introduced_smells:
        introduced[dataset][smell] += 1

#Build tables for top 10
def top_n(counter, n=10):
    total = sum(counter.values())
    df = pd.DataFrame(counter.most_common(n), columns=["smell_type", "count"])
    if total > 0:
        df["percentage"] = (df["count"] / total * 100).round(2)
    else:
        df["percentage"] = 0.0
    return df

top_agentic = top_n(introduced["Agentic"])
top_human = top_n(introduced["Human"])



top_agentic.to_csv(TABLES_DIR / "top_10_introduced_smells_agentic.csv", index=False)
top_human.to_csv(TABLES_DIR / "top_10_introduced_smells_human.csv", index=False)

print("\nTop 10 Introduced Smells — Agentic")
print(top_agentic.to_string(index=False))

print("\nTop 10 Introduced Smells — Human")
print(top_human.to_string(index=False))

print("\nSaved to outputs/tables/")
