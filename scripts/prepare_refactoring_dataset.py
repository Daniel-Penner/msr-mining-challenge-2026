"""
prepare_refactoring_dataset.py
-------------------------------
Filters RefactoringMiner results (agentic + baseline)
to create structured parquet datasets of refactoring commits.

Each entry includes:
- repo_name
- commit_sha
- refactoring_type
- entities_before / entities_after
- commit_url
- agent_type (agentic vs baseline)
"""

import json
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Input paths
AGENTIC_REFM_JSON = PROJECT_ROOT / "data" / "processed" / "refminer_results" / "refminer_all.json"
BASELINE_REFM_JSON = PROJECT_ROOT / "data" / "processed" / "refminer_baseline_results" / "refminer_all_baseline.json"

# Output paths
OUTPUT_AGENTIC = PROJECT_ROOT / "data" / "processed" / "agentic_refactorings.parquet"
OUTPUT_BASELINE = PROJECT_ROOT / "data" / "processed" / "baseline_refactorings.parquet"

def extract_refactorings(json_path, agent_type):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)["commits"]

    rows = []
    for commit in data:
        repo = commit.get("repository", "")
        sha = commit.get("sha1", "")
        url = commit.get("url", "")
        refactorings = commit.get("refactorings", [])
        for ref in refactorings:
            rows.append({
                "agent_type": agent_type,
                "repo_name": repo.split("/")[-1].replace(".git", ""),
                "commit_sha": sha,
                "commit_url": url,
                "refactoring_type": ref.get("type", ""),
                "description": ref.get("description", ""),
                "entities_before": [e.get("name") for e in ref.get("leftSideLocations", [])],
                "entities_after": [e.get("name") for e in ref.get("rightSideLocations", [])],
            })

    df = pd.DataFrame(rows)
    print(f"✅ {agent_type}: {len(df)} refactorings from {len(data)} commits ({json_path.name})")
    return df


# Extract both datasets
df_agentic = extract_refactorings(AGENTIC_REFM_JSON, "agentic")
df_baseline = extract_refactorings(BASELINE_REFM_JSON, "baseline")

# Save
df_agentic.to_parquet(OUTPUT_AGENTIC, index=False)
df_baseline.to_parquet(OUTPUT_BASELINE, index=False)

print(f"\n💾 Saved agentic → {OUTPUT_AGENTIC}")
print(f"💾 Saved baseline → {OUTPUT_BASELINE}")

# Summary
print("\n📊 SUMMARY:")
print(f"Agentic commits with refactorings: {df_agentic['commit_sha'].nunique()}")
print(f"Baseline commits with refactorings: {df_baseline['commit_sha'].nunique()}")
print(f"Total refactorings (agentic): {len(df_agentic)}")
print(f"Total refactorings (baseline): {len(df_baseline)}")
