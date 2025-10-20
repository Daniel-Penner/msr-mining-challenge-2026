import os
import subprocess
from tqdm import tqdm
from datetime import datetime
import requests

OUTPUT_DIR = "G:\\msr-mining-challenge-2026\\human_java_repos"
MIN_STARS = 50
MAX_REPOS = 1
GITHUB_TOKEN = "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"
PR_BEFORE_DATE = "2021-01-01"

HEADERS = {}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"token {GITHUB_TOKEN}"


def get_top_java_repos(top_n=5, min_stars=50, max_pages=10):
    """Get top Java repos by stars with activity before PR_BEFORE_DATE."""
    repos = []
    for page in range(1, max_pages + 1):
        url = (
            f"https://api.github.com/search/repositories?"
            f"q=language:Java+stars:>={min_stars}+pushed:<{PR_BEFORE_DATE}"
            f"&sort=stars&order=desc&per_page={top_n}&page={page}"
        )
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        items = r.json()["items"]
        for item in items:
            repos.append((item["owner"]["login"], item["name"]))
        if len(repos) >= top_n:
            break
    return repos[:top_n]


def clone_repo_in_folder(owner, repo):
    parent_dir = os.path.join(OUTPUT_DIR, f"{owner}__{repo}_folder")
    os.makedirs(parent_dir, exist_ok=True)

    repo_dir = os.path.join(parent_dir, "repo")
    if not os.path.exists(os.path.join(repo_dir, ".git")):
        git_url = f"https://github.com/{owner}/{repo}.git"
        print(f"Cloning {owner}/{repo} into {repo_dir}...")
        subprocess.run(["git", "clone", "--depth", "1", git_url, repo_dir], check=True)
    else:
        print(f"{owner}/{repo} already cloned, skipping.")
    return parent_dir, repo_dir


def fetch_pr_commit_hashes(owner, repo):
    """Fetch all PR numbers and their head commit hashes for a repo."""
    pr_info = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state=all&per_page=100&page={page}"
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        prs = r.json()
        if not prs:
            break

        for pr in prs:
            created_at = datetime.strptime(pr["created_at"], "%Y-%m-%dT%H:%M:%SZ")
            if created_at < datetime.strptime(PR_BEFORE_DATE, "%Y-%m-%d"):
                pr_number = pr["number"]
                head_sha = pr["head"]["sha"]
                base_sha = pr["base"]["sha"]
                pr_info.append((pr_number, base_sha, head_sha))
        page += 1
    return pr_info


def save_pr_info(parent_dir, pr_info):
    """Save PR numbers and commit hashes to a file."""
    path = os.path.join(parent_dir, "PR_commits.txt")
    with open(path, "w", encoding="utf-8") as f:
        for pr_number, base_sha, head_sha in pr_info:
            f.write(f"{pr_number},{base_sha},{head_sha}\n")
    print(f"Saved {len(pr_info)} PR commits to {path}")


def main():
    repos = get_top_java_repos(top_n=MAX_REPOS, min_stars=MIN_STARS)
    for owner, repo in tqdm(repos, desc="Processing Repos"):
        try:
            parent_dir, repo_dir = clone_repo_in_folder(owner, repo)
            pr_info = fetch_pr_commit_hashes(owner, repo)
            save_pr_info(parent_dir, pr_info)
        except Exception as e:
            print(f"Error processing {owner}/{repo}: {e}")


if __name__ == "__main__":
    main()
