# How do Agents Refactor: An Empirical Study

This directory contains this scripts necessary to reproduce the data from "How do Agents Refactor: An Empirical Study".

The following is an ordered list of file groupings to run to build our dataset and compute our results. Files within a grouping (e.g. Clone Repositories) should also be run in the listed order.

## Execution Order Overview

1. **Clone Repositories**
2. **Refactoring Mining**
3. **Dataset Construction**
4. **Refactoring Analysis**
5. **Code Smell Analysis**

---

## 1. Clone Repositories

###
These scripts select and retrieve repositories for analysis.

### 1a. `build_agentic_pr_commits.py`
Creates a parquet file containing commits from agentic pull requests in Java projects.

### 1b. `clone_agentic_repos.py`
Clones repositories containing previously selected agentic.

### 1c. `get_human_java_repos.py`
Selects a subset of human repositories to be used as a baseline.

### 1d. `build_baseline_pr_commits.py`
Creates a parquet file containing commits from selected baseline repositories.

### 1e. `clone_baseline_repos.py`
Clones repositories used as the human baseline comparison set.

---

## 2. Refactoring Mining

These scripts run RefactoringMiner on the cloned repositories.

### 2a. `run_refactoringminer_agentic.py`
Collects refactoring data from agentic commits.

### 2b. `run_refactoringminer_baseline.py`
Collects refactoring data from human baseline commits.

---

## 3. Dataset Construction

These scripts combine data from existing parquets and newly obtained refactoring data into new parquets for analysis.

### 3a. `build_agentic_dataset.py`
Combines agentic refactoring data and metadata into a final analysis dataset.

### 3b. `build_baseline_dataset.py`
Combines baseline refactoring data and metadata into a final analysis dataset.

---

## 4. Refactoring Analysis

These scripts are for the analysis, plotting, and table compilation of refactoring data.

### 4a. `dataset_analysis.py`
Analyzes refactoring data and provides statistics.

### 4b. `refactoring_per_commit`
Analyzes per-commit refactoring data for both agents and humans.

### 4b. `refactoring_types_by_agent.py`
Analyzes refactoring type distributions across agent types and humans.

---

## 5. Code Smell Analysis

These scripts are for the analysis, plotting, and table compilation of refactoring code smell introduction.

### 5a. `analyze_smells_before_and_after.py`
Runs DesigniteJava on the before and after of human and agentic commits to compile code smell data.

### 5b. `smells_statistical_analysis.py`
Runs statistical tests on smell changes before and after refactoring commits.

### 5c. `plot_smell_deltas.py`
Plots smell changes before and after refactoring commits.

### 5d. `top_smells.py`
Identifies the most frequent smells in refactoring commits and the most commonly introduced by refactoring for humans and agents.

