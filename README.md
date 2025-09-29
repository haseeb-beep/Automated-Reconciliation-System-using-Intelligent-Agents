Overview

This project automates reconciliation between ledger and bank data using intelligent agents. It reduces manual effort by combining rule-based heuristics with an optional machine learning model (Gradient Boosting) that adapts thresholds based on past matches. The system categorizes unmatched cases, routes them to the right teams, and produces a full audit trail for transparency.

⚙️ Requirements

Python 3.8+

Install dependencies:

pip install pandas numpy scikit-learn openpyxl

🧩 Components
Part 1 – Baseline Matching

Matches records using fixed rules (amount/date tolerance, name/reference similarity).

Part 2 – Data Quality & Adaptive Thresholds

Loosens or tightens thresholds depending on data quality (missing values, duplicates).

Part 3 – Automated Matching with ML (Optional)

Trains a Gradient Boosting model using labels.csv (1 = match, 0 = not match).

Learns the best probability cutoff and applies it for smarter matching.

Part 4 – Exception Management

Categorizes unmatched items (AMOUNT_MISMATCH, DATE_MISMATCH, etc.).

Routes them into queues (Finance, Ops, Payments).

Part 5 – Master Orchestration

Runs the full pipeline: data quality → matching → exception handling → audit.

Generates final outputs in one place.

📊 Outputs

After running, you’ll find results in ./out_results:

matched.csv → Confirmed matches

unmatched_ledger.csv → Ledger rows not reconciled

unmatched_bank.csv → Bank rows not reconciled

exceptions.csv → Categorized exceptions

routed_queues/ → Team-specific exception files

audit.jsonl → Explainable AI decision logs

dq_metrics.json → Data quality metrics

summary.json → Overall reconciliation statistics

▶️ Usage
Run with heuristics only
python master_reconcile.py --a Customer_Ledger_Entries_FULL.xlsx --b KH_Bank.xlsx --out ./out_results

Run with ML training (optional)
python master_reconcile.py --a Customer_Ledger_Entries_FULL.xlsx --b KH_Bank.xlsx --out ./out_results --labels labels.csv

✨ Key Features

Multi-dimensional matching (amounts, dates, references, names)

Adaptive thresholds with data quality feedback

Machine learning agent (Gradient Boosting) for smarter decisions

Exception categorization & routing to queues

Transparent audit trail for compliance

👨‍💻 Author

Syed Haseeb Haider
