import os
import requests
from tqdm import tqdm
from datetime import datetime

OUTPUT_DIR = "G:\\msr-mining-challenge-2026\\human_java_pr_diffs"
MIN_STARS = 50
MAX_REPOS = 10
GITHUB_TOKEN = "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"
# GITHUB_TOKEN = None
PR_BEFORE_DATE = "2021-01-01" # pr before this date will be saved. I think this is early enough but a citation would be best to be sure

HEADERS = {}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"token {GITHUB_TOKEN}"


def get_top_java_repos(top_n=5, min_stars=50, max_pages=10):
    """Get top Java repos by stars."""
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

def get_pull_requests(owner, repo, state="all", per_page=100):
    """Get all pull requests for a repo."""
    prs = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state={state}&per_page={per_page}&page={page}"
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        for pr in data:
            # only include PRs created before 2021
            created_at = datetime.strptime(pr["created_at"], "%Y-%m-%dT%H:%M:%SZ")
            if created_at < datetime.strptime(PR_BEFORE_DATE, "%Y-%m-%d"):
                prs.append(pr)
        page += 1
    return prs

def get_pr_files(owner, repo, pr_number):
    """Get files changed in a pull request."""
    files = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files?per_page=100&page={page}"
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        files.extend(data)
        page += 1
    return files

def save_diff_file(repo_dir, pr_number, filename, patch):
    #save a diff patch to a file in the PR folder
    pr_dir = os.path.join(repo_dir, f"PR_{pr_number}")
    os.makedirs(pr_dir, exist_ok=True)

    safe_name = filename.replace("/", "__")
    path = os.path.join(pr_dir, f"{safe_name}.diff")
    with open(path, "w", encoding="utf-8") as f:
        f.write(patch)
def save_pr_metadata(repo_dir, pr_number, pr):
    pr_dir = os.path.join(repo_dir, f"PR_{pr_number}")
    os.makedirs(pr_dir, exist_ok=True)
    path = os.path.join(pr_dir, "PR_metadata.txt")
    
    created_at = pr.get("created_at")
    merged_at = pr.get("merged_at")
    if created_at and merged_at:
        created_dt = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
        merged_dt = datetime.strptime(merged_at, "%Y-%m-%dT%H:%M:%SZ")
        time_to_merge = merged_dt - created_dt
    else:
        time_to_merge = "Not merged"
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"Number: {pr.get('number')}\n")
        f.write(f"Title: {pr.get('title')}\n")
        f.write(f"Author: {pr.get('user', {}).get('login')}\n")
        f.write(f"State: {pr.get('state')}\n")
        f.write(f"Locked: {pr.get('locked')}\n")
        f.write(f"Created at: {created_at}\n")
        f.write(f"Updated at: {pr.get('updated_at')}\n")
        f.write(f"Closed at: {pr.get('closed_at')}\n")
        f.write(f"Merged at: {merged_at}\n")
        f.write(f"Merged by: {pr.get('merged_by', {}).get('login')}\n")
        f.write(f"Time to merge: {time_to_merge}\n")
        f.write(f"Commits: {pr.get('commits')}\n")
        f.write(f"Additions: {pr.get('additions')}\n")
        f.write(f"Deletions: {pr.get('deletions')}\n")
        f.write(f"Changed files: {pr.get('changed_files')}\n")
        f.write(f"Comments: {pr.get('comments')}\n")
        f.write(f"Review comments: {pr.get('review_comments')}\n")


def main():
    repos = get_top_java_repos(top_n=MAX_REPOS, min_stars=MIN_STARS)

    for owner, repo in tqdm(repos, desc="Repos"):
        repo_dir = os.path.join(OUTPUT_DIR, f"{owner}__{repo}")
        os.makedirs(repo_dir, exist_ok=True)
        try:
            prs = get_pull_requests(owner, repo)
            for pr in tqdm(prs, desc=f"{repo} PRs"):
                pr_number = pr["number"]
                save_pr_metadata(repo_dir, pr_number, pr)
                files = get_pr_files(owner, repo, pr_number)
                for f in files:
                    patch = f.get("patch")
                    if patch:
                        save_diff_file(repo_dir, pr_number, f["filename"], patch)
        except Exception as e:
            print(f"Error in {owner}/{repo}: {e}")

if __name__ == "__main__":
    main()
