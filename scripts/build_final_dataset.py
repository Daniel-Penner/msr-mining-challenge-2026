#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_refactoring_dataset.py
----------------------------
Create a research-grade dataset combining RefactoringMiner results with
the AIDev Java PR commit metadata.

Inputs:
  - data/processed/refminer_results/refminer_all.json         (RefactoringMiner merged output)
  - data/processed/java_agentic_pr_commits_final.parquet      (commit + PR + repo + agent)

Outputs:
  - data/processed/refactoring_dataset_commits.parquet        (one row per commit; flags & counts)
  - data/processed/refactoring_dataset_refactorings.parquet   (one row per refactoring event)

Run:
  python scripts/build_refactoring_dataset.py
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RM_JSON = PROJECT_ROOT / "data" / "processed" / "refminer_results" / "refminer_all.json"
META_PARQUET = PROJECT_ROOT / "data" / "processed" / "java_agentic_pr_commits_final.parquet"

OUT_DIR = PROJECT_ROOT / "data" / "processed"
COMMITS_OUT = OUT_DIR / "refactoring_dataset_commits.parquet"
REFACT_OUT = OUT_DIR / "refactoring_dataset_refactorings.parquet"


def _safe_list(x: Optional[Iterable]) -> List:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _norm_repo_name_from_url(url: str) -> str:
    # Accept both https://github.com/owner/repo and https://api.github.com/repos/owner/repo
    if not isinstance(url, str):
        return ""
    u = url.strip().rstrip("/")
    if "/repos/" in u:
        # API form
        parts = u.split("/repos/", 1)[-1].split("/")
    else:
        # web form
        parts = u.replace("https://github.com/", "").split("/")
    if len(parts) >= 2:
        return f"{parts[0]}/{parts[1].replace('.git','')}"
    return ""


def _flatten_locations(loc_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert RefactoringMiner location objects into compact, consistent dicts.
    Location example keys include: filePath, startLine, endLine, codeElement, description
    """
    out = []
    for loc in _safe_list(loc_list):
        out.append({
            "filePath": loc.get("filePath"),
            "startLine": loc.get("startLine"),
            "endLine": loc.get("endLine"),
            "codeElement": loc.get("codeElement"),
            "description": loc.get("description"),
        })
    return out


print("Loading inputs...")
if not RM_JSON.exists():
    sys.exit(f"Missing RefactoringMiner JSON: {RM_JSON}")
if not META_PARQUET.exists():
    sys.exit(f"Missing metadata parquet: {META_PARQUET}")

with RM_JSON.open("r", encoding="utf-8") as f:
    rm = json.load(f)

meta = pd.read_parquet(META_PARQUET)

# Keep the meta columns we’ll need
meta = meta[[
    "sha",          # commit hash
    "pr_id",        # internal PR id (dataset)
    "number",       # PR number on GitHub
    "repo_url",     # API repo url
    "full_name",    # owner/repo
    "language",     # "Java"
    "agent"         # agent label (Copilot, Codex, Claude, etc.)
]].drop_duplicates()

print(f"  • Meta rows: {len(meta)} | commits: {meta['sha'].nunique()} | PRs: {meta['pr_id'].nunique()} | repos: {meta['full_name'].nunique()}")


print("Flattening RefactoringMiner refactorings...")
ref_rows: List[Dict[str, Any]] = []

commits_json: List[Dict[str, Any]] = rm.get("commits", [])

for c in commits_json:
    repo_url = c.get("repository")
    commit_sha = c.get("sha1")
    commit_url = c.get("url")
    refactorings = _safe_list(c.get("refactorings"))

    if not commit_sha:
        continue

    # If there are no refactorings, we won't add to ref_rows (commit table will capture 0 later)
    for ref in refactorings:
        ref_type = ref.get("type")
        desc = ref.get("description", "")

        left_locs = _flatten_locations(ref.get("leftSideLocations", []))
        right_locs = _flatten_locations(ref.get("rightSideLocations", []))

        # capture a compact set of code elements (strings) too
        left_elems = [x.get("codeElement") for x in left_locs if x.get("codeElement")]
        right_elems = [x.get("codeElement") for x in right_locs if x.get("codeElement")]

        ref_rows.append({
            "sha": commit_sha,
            "repo_url_rm": repo_url,
            "repo_full_name_rm": _norm_repo_name_from_url(repo_url),
            "commit_url": commit_url,
            "refactoring_type": ref_type,
            "description": desc,
            "left_locations": left_locs,     # list of dicts
            "right_locations": right_locs,   # list of dicts
            "left_elements": left_elems,     # list of strings
            "right_elements": right_elems,   # list of strings
        })

ref_df = pd.DataFrame(ref_rows)

if len(ref_df) == 0:
    print("⚠️ No refactorings found in refminer JSON. You’ll still get a per-commit table with zeros.")
else:
    print(f"  • Refactorings: {len(ref_df)} across {ref_df['sha'].nunique()} commits and {ref_df['refactoring_type'].nunique()} types.")


print("Aggregating per-commit metrics...")
if len(ref_df) > 0:
    agg = (
        ref_df.groupby("sha")
        .agg(
            refactoring_count=("refactoring_type", "count"),
            unique_types=("refactoring_type", lambda s: sorted(set(s))),
        )
        .reset_index()
    )
    agg["has_refactoring"] = True
else:
    # Build empty agg with required columns so merge works
    agg = pd.DataFrame(columns=["sha", "refactoring_count", "unique_types", "has_refactoring"])

# Merge with meta to get agent, repo, PR info per commit
commits = meta.merge(agg, on="sha", how="left")
commits["has_refactoring"] = commits["has_refactoring"].fillna(False)
commits["refactoring_count"] = commits["refactoring_count"].fillna(0).astype(int)
commits["unique_types"] = commits["unique_types"].apply(lambda v: v if isinstance(v, list) else [])

# Useful derived columns
commits["owner"] = commits["full_name"].apply(lambda s: s.split("/")[0] if isinstance(s, str) and "/" in s else s)
commits["repo"] = commits["full_name"].apply(lambda s: s.split("/")[1] if isinstance(s, str) and "/" in s else s)


OUT_DIR.mkdir(parents=True, exist_ok=True)

print("Writing outputs...")
commits.to_parquet(COMMITS_OUT, index=False)
if len(ref_df) > 0:
    # enrich ref_df with agent & PR metadata too (handy for per-refactoring analyses)
    ref_enriched = ref_df.merge(
        commits[["sha", "pr_id", "number", "full_name", "owner", "repo", "agent"]],
        on="sha",
        how="left"
    )
    ref_enriched.to_parquet(REFACT_OUT, index=False)

print(f"  • {COMMITS_OUT}")
if len(ref_df) > 0:
    print(f"  • {REFACT_OUT}")


print("Summary Stats")
total_commits = len(commits)
ref_commits = int(commits["has_refactoring"].sum())
pct = (ref_commits / total_commits * 100) if total_commits else 0.0
mean_per_ref_commit = commits.loc[commits["has_refactoring"], "refactoring_count"].mean() if ref_commits else 0.0

print(f"  • Commits total: {total_commits}")
print(f"  • Commits with refactoring: {ref_commits} ({pct:.2f}%)")
print(f"  • Avg # refactorings per refactoring-commit: {mean_per_ref_commit:.2f}")

if len(ref_df) > 0:
    top_types = (
        ref_df["refactoring_type"]
        .value_counts()
        .head(10)
        .rename_axis("refactoring_type")
        .reset_index(name="count")
    )
    print("\n  • Top 10 refactoring types:")
    for _, row in top_types.iterrows():
        print(f"     - {row['refactoring_type']}: {row['count']}")
else:
    print("  • No refactoring types present (empty ref_df).")

print("\n✅ Done.")
