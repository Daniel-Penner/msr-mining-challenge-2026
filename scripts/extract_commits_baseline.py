"""
extract_pr_commits_baseline.py
------------------------------
Extracts all commits associated with each pull request
for every baseline repository in repos_baseline/.

Outputs:
  data/processed/java_baseline_pr_commits.parquet
"""

import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import os

# --------------------------------------------------------------------
# PATH CONFIGURATION
# --------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOS_DIR = PROJECT_ROOT / "repos_baseline"
CSV_PATH = PROJECT_ROOT / "data" / "processed" / "java_baseline_repos.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "java_baseline_pr_commits.parquet"

# Load repo list
repos_df = pd.read_csv(CSV_PATH)
print(f"📦 Loaded {len(repos_df)} baseline repositories.")

# Get GitHub token for higher rate limit
token = os.getenv("GITHUB_TOKEN")
if not token:
    raise EnvironmentError("❌ Please set your GitHub token in GITHUB_TOKEN.")

headers = {"Authorization": f"token {token}"}

rows = []

# --------------------------------------------------------------------
# HELPER FUNCTION
# --------------------------------------------------------------------
def get_pr_commits(full_name, pr_number):
    """Return list of commits for a given PR"""
    commits_url = f"https://api.github.com/repos/{full_name}/pulls/{pr_number}/commits"
    resp = requests.get(commits_url, headers=headers)
    if resp.status_code != 200:
        return []
    return [c["sha"] for c in resp.json()]


# --------------------------------------------------------------------
# MAIN LOOP
# --------------------------------------------------------------------
for _, row in tqdm(repos_df.iterrows(), total=len(repos_df), desc="Extracting PR commits"):
    repo_url = row["repo_url"]
    full_name = repo_url.replace("https://github.com/", "").replace(".git", "")

    # list PRs for this repo
    prs_url = f"https://api.github.com/repos/{full_name}/pulls?state=closed&per_page=100"
    resp = requests.get(prs_url, headers=headers)
    if resp.status_code != 200:
        print(f"⚠️ Failed to fetch PRs for {full_name}")
        continue

    for pr in resp.json():
        pr_id = pr["id"]
        number = pr["number"]

        for sha in get_pr_commits(full_name, number):
            rows.append({
                "sha": sha,
                "pr_id": pr_id,
                "number": number,
                "repo_url": repo_url,
                "full_name": full_name,
                "language": "Java",
                "agent": "Human"  # ✅ differentiate from AI dataset
            })

# --------------------------------------------------------------------
# SAVE OUTPUT
# --------------------------------------------------------------------
df = pd.DataFrame(rows)
print(f"✅ Extracted {len(df)} PR commits from {df['full_name'].nunique()} repos.")
df.to_parquet(OUTPUT_PATH, index=False)
print(f"💾 Saved to {OUTPUT_PATH}")
