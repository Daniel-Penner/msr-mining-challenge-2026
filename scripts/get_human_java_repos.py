import os
import csv
import requests
from tqdm import tqdm

# Configuration
OUTPUT_CSV = "L:\\msr-mining-challenge-2026\\data\\processed\\human_java_repos.csv"
MIN_STARS = 50
MAX_REPOS = 100
PUSHED_BEFORE = "2021-01-01"
GITHUB_TOKEN = "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"

HEADERS = {"Accept": "application/vnd.github+json"}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"token {GITHUB_TOKEN}"


def get_human_written_java_repos(min_stars=50, pushed_before="2021-01-01", max_repos=500, max_pages=20):
    """Fetch Java repositories last pushed before a date (likely human-written)."""
    repos = []
    per_page = 100  
    for page in range(1, max_pages + 1):
        url = (
            f"https://api.github.com/search/repositories?"
            f"q=language:Java+stars:>={min_stars}+pushed:<{pushed_before}"
            f"&sort=stars&order=desc&per_page={per_page}&page={page}"
        )
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()

        items = r.json().get("items", [])
        if not items:
            break

        for item in items:
            repo_url = item["html_url"] + ".git"
            name = item["name"]
            size_gb = item["size"] / 1_000_000

            repos.append({
                "repo_url": repo_url,
                "name": name,
                "size_gb": round(size_gb, 9)
            })

            if len(repos) >= max_repos:
                return repos

    return repos


def save_to_csv(repos, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["repo_url", "name", "size_gb"])
        writer.writeheader()
        writer.writerows(repos)



def main():
    repos = get_human_written_java_repos(
        min_stars=MIN_STARS,
        pushed_before=PUSHED_BEFORE,
        max_repos=MAX_REPOS
    )

    save_to_csv(repos, OUTPUT_CSV)


if __name__ == "__main__":
    main()
