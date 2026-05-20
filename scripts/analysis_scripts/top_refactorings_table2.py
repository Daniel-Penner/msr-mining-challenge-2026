import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "outputs" / "tables" / "refactor_types_by_agent.csv"

df = pd.read_csv(DATA_PATH)

agents_df = df[df["agent"] != "Human"]
human_df = df[df["agent"] == "Human"]

agent_combined = (
    agents_df.groupby("refactoring_type")["count"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
    .reset_index()
)
agent_total = agents_df["count"].sum()
agent_combined["share_pct"] = agent_combined["count"] / agent_total * 100

human_top = (
    human_df.groupby("refactoring_type")["count"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
    .reset_index()
)
human_total = human_df["count"].sum()
human_top["share_pct"] = human_top["count"] / human_total * 100

print("--- Top 10 Refactoring Types: AI Agents (Combined) ---")
print(f"Total refactorings: {agent_total:,}\n")
print(agent_combined.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

print("\n--- Top 10 Refactoring Types: Human Developers ---")
print(f"Total refactorings: {human_total:,}\n")
print(human_top.to_string(index=False, float_format=lambda x: f"{x:.2f}"))
