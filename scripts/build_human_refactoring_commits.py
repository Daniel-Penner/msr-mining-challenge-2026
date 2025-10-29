#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build a commit-level HUMAN dataset limited to PR commits that were actually
analyzed by RefactoringMiner (including those with zero refactorings).

Inputs:
  - data/processed/baseline_pr_commits.parquet
      (the PR commit list used as input to run_refactoringminer_baseline.py)
  - data/processed/refminer_baseline_results/refminer_all_baseline.json
      (RefactoringMiner combined JSON output; includes commits with 0 refactorings)

Optional:
  - data/processed/baseline_refactorings.parquet
      (refactoring *events*; used only to double-check counts, not required)

Output:
  - data/processed/human_refactoring_commits.parquet
      one row per successfully analyzed PR commit, with:
        sha, pr_id, number, full_name, owner, repo, agent="Human",
        has_refactoring (bool), refactoring_count (int), unique_types (list[str])
"""

from pathlib import Path
import json
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PR_COMMITS = PROJECT_ROOT / "data" / "processed" / "baseline_pr_commits.parquet"
RM_JSON    = PROJECT_ROOT / "data" / "processed" / "refminer_baseline_results" / "refminer_all_baseline.json"
OUT_PATH   = PROJECT_ROOT / "data" / "processed" / "human_refactoring_commits.parquet"

print("📦 Loading inputs...")
if not PR_COMMITS.exists():
    raise SystemExit(f"❌ Missing PR commits parquet: {PR_COMMITS}")
if not RM_JSON.exists():
    raise SystemExit(f"❌ Missing RefactoringMiner JSON: {RM_JSON}")

pr_df = pd.read_parquet(PR_COMMITS)
pr_df["sha"] = pr_df["sha"].astype(str).str.lower().str.strip()

# --- read refminer JSON (includes commits with 0 refactorings) ---
with RM_JSON.open("r", encoding="utf-8") as f:
    rm = json.load(f)

rm_commits = []
for c in rm.get("commits", []):
    sha = str(c.get("sha1", "")).strip().lower()
    if not sha:
        continue
    refs = c.get("refactorings", []) or []
    types = sorted({r.get("type") for r in refs if r.get("type")})
    rm_commits.append({
        "sha": sha,
        "refactoring_count": len(refs),
        "unique_types": types,
        "has_refactoring": len(refs) > 0,
    })

rm_df = pd.DataFrame(rm_commits).drop_duplicates(subset=["sha"])
print(f"✅ RefMiner JSON commits: {len(rm_df)} (unique) | "
      f"with refactorings: {int(rm_df['has_refactoring'].sum())}")

# --- merge: keep only PR commits that RefMiner successfully analyzed ---
merged = pr_df.merge(rm_df, on="sha", how="inner")  # inner = analyzed subset only
if "full_name" in merged.columns:
    merged["owner"] = merged["full_name"].str.split("/", n=1).str[0]
    merged["repo"]  = merged["full_name"].str.split("/", n=1).str[1]
else:
    merged["owner"] = None
    merged["repo"]  = None

# normalize schema
merged["agent"] = "Human"
merged["refactoring_count"] = merged["refactoring_count"].fillna(0).astype(int)
merged["has_refactoring"] = merged["has_refactoring"].fillna(False)
merged["unique_types"] = merged["unique_types"].apply(lambda v: v if isinstance(v, list) else [])

# optional dedupe by (sha, pr_id, agent)
merged = merged.drop_duplicates(subset=["sha", "pr_id", "agent"])

print("\n📊 Summary")
total = len(merged)
with_ref = int(merged["has_refactoring"].sum())
print(f"  • PR commits analyzed by RefMiner: {total}")
print(f"  • With ≥1 refactoring: {with_ref} ({with_ref/total*100:.2f}%)")

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
merged.to_parquet(OUT_PATH, index=False)
print(f"\n💾 Saved to {OUT_PATH}")
print("🏁 Done.")
