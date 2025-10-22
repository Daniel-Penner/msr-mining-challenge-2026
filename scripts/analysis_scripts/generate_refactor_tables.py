import pandas as pd
from pathlib import Path

# ---------- Paths ----------
CSV_PATH = Path("refactor_types_by_agent_counts_and_share.csv")
OUT_DIR = Path("outputs/tables")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Load Data ----------
df = pd.read_csv(CSV_PATH)

# Round share for nicer formatting
df["share_pct"] = df["share_pct"].round(2)

# Separate AI agents vs Human
human_df = df[df["agent"].str.lower() == "human"]
agents_df = df[df["agent"].str.lower() != "human"]

# ---------- Helper: Format a LaTeX table ----------
def make_latex_table(data, caption, label):
    # Sort by agent then share descending
    data = data.sort_values(["agent", "share_pct"], ascending=[True, False])
    # Group by agent and build subtables
    tables = []
    for agent, sub in data.groupby("agent"):
        sub_latex = sub[["refactoring_type", "count", "agent_total", "share_pct"]].to_latex(
            index=False,
            header=["Refactoring Type", "Count", "Agent Total", "Share (\\%)"],
            float_format="%.2f",
            column_format="lrrr",
            escape=False
        )
        tables.append(f"\\textbf{{{agent}}}\n\n{sub_latex}\n\\vspace{{1em}}\n")

    full_table = (
        "\\begin{table}[H]\n"
        "\\centering\n"
        "\\small\n"
        f"\\caption{{{caption}}}\n"
        f"\\label{{{label}}}\n"
        "\\begin{tabular}{l}\n"
        "\\toprule\n"
        "\\textbf{Per-Agent Refactoring Type Distribution}\\\\\n"
        "\\midrule\n"
        "\\end{tabular}\n\n"
        + "\n".join(tables) +
        "\\end{table}\n"
    )
    return full_table

# ---------- Generate LaTeX ----------
latex_agents = make_latex_table(
    agents_df,
    "Most common refactoring types among AI agents.",
    "tab:refactor_types_agents"
)
latex_human = make_latex_table(
    human_df,
    "Most common refactoring types among human developers.",
    "tab:refactor_types_human"
)

# ---------- Write output ----------
(OUT_DIR / "refactor_types_agents.tex").write_text(latex_agents, encoding="utf-8")
(OUT_DIR / "refactor_types_human.tex").write_text(latex_human, encoding="utf-8")

print("✅ Saved LaTeX tables to:", OUT_DIR)
s