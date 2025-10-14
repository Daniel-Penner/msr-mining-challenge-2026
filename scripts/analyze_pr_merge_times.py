import duckdb
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

PR_FILE = "data/raw/all_pull_request.parquet"

# Query: PRs that have both created_at and merged_at timestamps
query = f"""
SELECT
    id,
    repo_id,
    created_at,
    merged_at
FROM '{PR_FILE}'
WHERE merged_at IS NOT NULL
  AND created_at IS NOT NULL;
"""

df = duckdb.query(query).to_df()

# Convert timestamps to datetime objects
df["created_at"] = pd.to_datetime(df["created_at"])
df["merged_at"] = pd.to_datetime(df["merged_at"])

# Compute merge duration in hours
df["merge_duration_hours"] = (df["merged_at"] - df["created_at"]).dt.total_seconds() / 3600

print("Mean merge duration (hours):", df["merge_duration_hours"].mean())
print("Median merge duration (hours):", df["merge_duration_hours"].median())

# Optional: visualize distribution
plt.figure(figsize=(8,5))
plt.hist(df["merge_duration_hours"], bins=100, range=(0, 72))
plt.title("Distribution of PR Merge Times (0–72h window)")
plt.xlabel("Merge Duration (hours)")
plt.ylabel("Number of PRs")
plt.tight_layout()
plt.savefig("results/pr_merge_times.png")
plt.show()
