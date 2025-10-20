import pandas as pd

# Read parquet file
df = pd.read_parquet("L:\\msr-mining-challenge-2026\\data\processed\\java_agentic_pr_commits_final.parquet")

# Preview data
print(df.head())
