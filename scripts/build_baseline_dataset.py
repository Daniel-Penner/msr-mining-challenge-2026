#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_human_refactoring_dataset.py
----------------------------------
Constructs a complete HUMAN (baseline) refactoring dataset with:
 - One row per analyzed PR commit, including counts and types (commits table)
 - One row per refactoring event (refactorings table)
 - A normalized version including all baseline commits (even with zero refactorings)

Inputs:
  - data/processed/baseline_pr_commits.parquet
      (PR commit list used as input to RefactoringMiner baseline run)
  - data/processed/refminer_baseline_results/refminer_all_baseline.json
      (RefactoringMiner combined JSON output; includes 0-refactoring commits)

Outputs:
  - data/processed/baseline_refactoring_commits.parquet
      (commit-level dataset: sha, pr_id, number, full_name, owner, repo, agent="Human",
       has_refactoring, refactoring_count, unique_types)
  - data/processed/baseline_refactorings.parquet
      (one row per refactoring event)
  - data/processed/baseline_refactoring_commits_normalized.parquet
      (normalized dataset ensuring coverage of all baseline PR commits)
"""

import json
import pandas as pd
from pathlib import Path

# --------------------------------------------------------------------
# PATH CONFIGURATION
# --------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "processed"

PR_COMMITS = DATA_DIR / "baseline_pr_commits.parquet"
RM_JSON = DATA_DIR / "refminer_baseline_results" / "refminer_all_baseline.json"

COMMITS_OUT = DATA_DIR / "baseline_refactoring_commits.parquet"
REFACT_OUT = DATA_DIR / "baseline_refactorings.parquet"
NORMALIZED_OUT = DATA_DIR / "baseline_refactoring_commits_normalized.parquet"

# --------------------------------------------------------------------
# LOAD INPUTS
# --------------------------------------------------------------------
print("📦 Loading inputs...")
if not PR_COMMITS.exists():
    raise SystemExit(f"❌ Missing PR commits parquet: {PR_COMMITS}")
if not RM_JSON.exists():
    raise SystemExit(f"❌ Missing RefactoringMiner JSON: {RM_JSON}")

pr_df = pd.read_parquet(PR_COMMITS)
pr_df["sha"] = pr_df["sha"].astype(str).str.lower().str.strip()

# --------------------------------------------------------------------
# READ REFACTORINGMINER JSON (COMMITS WITH 0 INCLUDED)
# --------------------------------------------------------------------
with RM_JSON.open("r", encoding="utf-8") as f:
    rm_data = json.load(f)

print("🧩 Extracting commit-level and refactoring-level data...")
rm_commits = []
ref_rows = []

for c in rm_data.get("commits", []):
    sha = str(c.get("sha1", "")).strip().lower()
    if not sha:
        continue

    repo = c.get("repository", "")
    url = c.get("url", "")
    refs = c.get("refactorings", []) or []
    types = sorted({r.get("type") for r in refs if r.get("type")})

    rm_commits.append({
        "sha": sha,
        "refactoring_count": len(refs),
        "unique_types": types,
        "has_refactoring": len(refs) > 0,
    })

    for ref in refs:
        ref_rows.append({
            "agent_type": "baseline",
            "repo_name": repo.split("/")[-1].replace(".git", ""),
            "commit_sha": sha,
            "commit_url": url,
            "refactoring_type": ref.get("type", ""),
            "description": ref.get("description", ""),
            "entities_before": [e.get("name") for e in ref.get("leftSideLocations", [])],
            "entities_after": [e.get("name") for e in ref.get("rightSideLocations", [])],
        })

rm_df = pd.DataFrame(rm_commits).drop_duplicates(subset=["sha"])
ref_df = pd.DataFrame(ref_rows)

print(f"✅ Parsed {len(rm_df)} commits ({rm_df['has_refactoring'].sum()} with ≥1 refactoring)")
print(f"✅ Extracted {len(ref_df)} total refactoring events")

# --------------------------------------------------------------------
# MERGE: KEEP ONLY COMMITS ANALYZED BY REFACTORINGMINER
# --------------------------------------------------------------------
print("🔗 Merging with baseline PR commits...")
merged = pr_df.merge(rm_df, on="sha", how="inner")  # inner = analyzed subset

if "full_name" in merged.columns:
    merged["owner"] = merged["full_name"].str.split("/", n=1).str[0]
    merged["repo"]  = merged["full_name"].str.split("/", n=1).str[1]
else:
    merged["owner"] = None
    merged["repo"]  = None

# Normalize schema
merged["agent"] = "Human"
merged["refactoring_count"] = merged["refactoring_count"].fillna(0).astype(int)
merged["has_refactoring"] = merged["has_refactoring"].fillna(False)
merged["unique_types"] = merged["unique_types"].apply(lambda v: v if isinstance(v, list) else [])

# Dedupe (sha, pr_id, agent)
merged = merged.drop_duplicates(subset=["sha", "pr_id", "agent"])

# --------------------------------------------------------------------
# WRITE INITIAL OUTPUTS
# --------------------------------------------------------------------
print("\n💾 Writing outputs...")
DATA_DIR.mkdir(parents=True, exist_ok=True)
merged.to_parquet(COMMITS_OUT, index=False)
ref_df.to_parquet(REFACT_OUT, index=False)

print(f"  • Commits table → {COMMITS_OUT.name}")
print(f"  • Refactorings table → {REFACT_OUT.name}")

# --------------------------------------------------------------------
# NORMALIZATION STEP (INCLUDE MISSING BASELINE COMMITS)
# --------------------------------------------------------------------
print("\n⚙️ Normalizing baseline_refactoring_commits to include all baseline commits...")

baseline_df = pr_df.copy()
human_df = merged.copy()

# Ensure required columns exist
for col, default in {
    "has_refactoring": False,
    "unique_types": [],
    "refactoring_count": 0,
}.items():
    if col not in human_df.columns:
        print(f"⚠️ Missing column '{col}' in human dataset — creating defaults.")
        human_df[col] = [default for _ in range(len(human_df))]

# Align column names
if "commit" in baseline_df.columns and "sha" not in baseline_df.columns:
    baseline_df = baseline_df.rename(columns={"commit": "sha"})
if "commit" in human_df.columns and "sha" not in human_df.columns:
    human_df = human_df.rename(columns={"commit": "sha"})

# Identify missing commits
missing = baseline_df[~baseline_df["sha"].isin(human_df["sha"])]
print(f"🧩 Commits missing from baseline_refactoring_commits: {len(missing):,}")

# Create placeholder rows for missing commits
required_cols = list(human_df.columns)
carry_cols = [c for c in baseline_df.columns if c in required_cols]

missing_df = missing[carry_cols].copy()

# Fill required columns
for col in required_cols:
    if col not in missing_df.columns:
        if col == "has_refactoring":
            missing_df[col] = False
        elif col == "unique_types":
            missing_df[col] = [[] for _ in range(len(missing_df))]
        elif col == "refactoring_count":
            missing_df[col] = 0
        else:
            missing_df[col] = pd.NA

# Merge and clean
updated_df = pd.concat([human_df, missing_df], ignore_index=True)
updated_df["has_refactoring"] = updated_df["has_refactoring"].fillna(False).astype(bool)
updated_df["refactoring_count"] = updated_df["refactoring_count"].fillna(0).astype(int)
updated_df["unique_types"] = updated_df["unique_types"].apply(lambda x: x if isinstance(x, list) else [])

# Save normalized version
updated_df.to_parquet(NORMALIZED_OUT, index=False)

print(f"✅ Normalized dataset saved → {NORMALIZED_OUT.name}")
print(f"Total commits after normalization: {len(updated_df):,}")
print(f"Refactoring rate: {(updated_df['has_refactoring'].mean() * 100):.2f}%")

# --------------------------------------------------------------------
# SUMMARY
# --------------------------------------------------------------------
print("\n📊 FINAL SUMMARY")
print(f"  • Total analyzed commits: {len(merged):,}")
print(f"  • Total normalized commits: {len(updated_df):,}")
print(f"  • Commits with ≥1 refactoring: {int(updated_df['has_refactoring'].sum()):,}")
print(f"  • Total refactoring events: {len(ref_df):,}")
print(f"  • Unique commits with refactorings: {ref_df['commit_sha'].nunique():,}")
print("\n✅ Done.")
