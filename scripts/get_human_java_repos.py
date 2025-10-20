import requests
import csv


OUTPUT_CSV = "L:\\msr-mining-challenge-2026\\data\\processed\\human_java_repos.csv"

MIN_STARS = 50
MAX_REPOS = 100  
CREATED_BEFORE = "2021-01-01"
GITHUB_TOKEN = "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"

HEADERS = {"Accept": "application/vnd.github+json"}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"token {GITHUB_TOKEN}"


def get_java_repos(min_stars=50, created_before="2021-01-01", max_pages=10):
    repos = []
    per_page = 100  
    for page in range(1, max_pages + 1):
        url = (
            f"https://api.github.com/search/repositories?"
            f"q=language:Java+stars:>={min_stars}+created:<{created_before}"
            f"&sort=stars&order=desc&per_page={per_page}&page={page}"
        )
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items:
            break

        for item in items:
            size_gb = item["size"] / 1_000_000 
            repos.append({
                "repo_url": item["html_url"] + ".git",
                "name": item["name"],
                "size_gb": round(size_gb, 9)
            })

            if len(repos) >= MAX_REPOS:
                return repos

    return repos


def save_to_csv(repos, output_path):
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["repo_url", "name", "size_gb"])
        writer.writeheader()
        writer.writerows(repos)
    print(f"Saved {len(repos)} repos to {output_path}")


def main():
    repos = get_java_repos(min_stars=MIN_STARS, created_before=CREATED_BEFORE, max_pages=10)
    save_to_csv(repos, OUTPUT_CSV)


if __name__ == "__main__":
    main()
