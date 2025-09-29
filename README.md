# Automated Reconciliation System Using Intelligent Agents

## 📌 Problem Statement
The current reconciliation process relies heavily on **manual intervention**, **static rule-based matching**, and **predetermined parameters** that require constant human oversight. This results in:
- Operational inefficiencies
- Increased error rates
- Inability to adapt to evolving data patterns

This project develops an **intelligent, agent-based reconciliation system** that automatically:
- Selects optimal parameters
- Adapts to changing data characteristics
- Minimizes human intervention
- Maintains accuracy and compliance

---

## 🚀 Project Scope
The system is designed as a **multi-agent system** with the following capabilities:

- **Machine Learning Agent** → Determines optimal matching thresholds  
- **Dynamic Tolerance Agent** → Adjusts tolerance based on data quality metrics  
- **Reconciliation Engine** → Performs matching (exact, fuzzy, and pattern-based)  
- **Exception Management Agent** → Routes unmatched transactions intelligently  
- **Audit & Explainability Agent** → Provides transparent logs and decision tracking  

---

## ⚙️ Features
- Multi-dimensional matching (counterparty, references, amounts, dates)
- Hierarchical 1:1 reconciliation process
- Adaptive thresholds with machine learning
- Exception categorization & routing to queues
- Data quality monitoring
- Comprehensive audit trail


---

## 📊 Outputs
After running the system, the following outputs are generated in `./out_results`:

- `matched.xlsx` → Matched transactions  
- `unmatched_ledger.xlsx` → Ledger entries not reconciled  
- `unmatched_bank.xlsx` → Bank entries not reconciled  
- `exceptions.csv` → Categorized unmatched items  
- `routed_queues/` → Routed exceptions per queue/team  
- `audit.jsonl` → Explainable AI decision logs  
- `dq_metrics.json` → Data quality metrics  
- `summary.json` → Overall reconciliation statistics  

---

## ▶️ How to Run

### . Clone the repo
```bash
git clone https://github.com/MaryamIjaz-ai/automated-reconciliation-system.git
cd automated-reconciliation-system

. Run full system (Part 5 orchestration)
---
python part5_master_reconcile.py --a Customer_Ledger_Entries_FULL.xlsx --b KH_Bank.xlsx --out ./out_results



👩‍💻 Author

Maryam Ijaz
Automated Reconciliation System using Intelligent Agents

