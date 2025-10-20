"""
clone_fork_repos.py
-------------------
Clones all unique fork repositories from pr_fork_map_java.parquet
to repos_forks/ for later RefactoringMiner analysis.
"""

import os
import subprocess
import pandas as pd
from pathlib import Path
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FORK_MAP = PROJECT_ROOT / "data" / "processed" / "pr_fork_map_java.parquet"
CLONE_DIR = PROJECT_ROOT / "repos_forks"
CLONE_DIR.mkdir(exist_ok=True, parents=True)

# Load fork mapping
df = pd.read_parquet(FORK_MAP)
forks = sorted(set(df["fork_repo"].dropna()))

print(f"🔗 Found {len(forks)} unique fork repositories to clone.")

for url in tqdm(forks, desc="Cloning forks"):
    name = url.rstrip("/").split("/")[-1].replace(".git", "")
    dest = CLONE_DIR / name
    if dest.exists():
        continue
    try:
        subprocess.run(["git", "clone", url, str(dest)], check=True)
    except subprocess.CalledProcessError:
        print(f"❌ Failed to clone {url}")
        continue

print("✅ All fork repositories cloned.")
