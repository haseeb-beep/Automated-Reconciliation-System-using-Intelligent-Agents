# master_reconcile.py
"""
Part 5 - Master Orchestrator for Automated Reconciliation System (simple multi-agent pipeline)

Usage:
    python master_reconcile.py --a Customer_Ledger_Entries_FULL.xlsx \
                               --b KH_Bank.xlsx \
                               --out ./out_results \
                               [--mapping mapping.xlsx] \
                               [--labels labels.csv]

Notes:
- mapping.xlsx optional: columns (ledger_code, bank_name)
- labels.csv optional: historical labels to train ML threshold agent
"""

import argparse
import os
import json
from collections import defaultdict
from difflib import SequenceMatcher
from datetime import datetime
import pandas as pd
import numpy as np

# Optional ML
try:
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.calibration import CalibratedClassifierCV
    SKL_AVAILABLE = True
except Exception:
    SKL_AVAILABLE = False

# -------------------------
# Utilities & preprocessing
# -------------------------
def normalize_string(s):
    if pd.isna(s) or s is None:
        return ""
    s = str(s).lower().strip()
    for token in ["ltd", "pvt", "inc", "bank", "corp", "co", "company", "gmbh", "kft", "zrt", "rt"]:
        s = s.replace(token, "")
    s = "".join(ch for ch in s if ch.isalnum() or ch.isspace())
    return " ".join(s.split())

def str_sim(a, b):
    return SequenceMatcher(None, normalize_string(a), normalize_string(b)).ratio()

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return np.nan

def days_diff(a, b):
    try:
        if pd.isna(a) or pd.isna(b):
            return None
        return abs((pd.to_datetime(a) - pd.to_datetime(b)).days)
    except Exception:
        return None

def load_table(path):
    if path.lower().endswith(".csv"):
        return pd.read_csv(path)
    elif path.lower().endswith((".xls", ".xlsx")):
        return pd.read_excel(path)
    else:
        raise ValueError("Unsupported file type: " + path)

def standardize_inputs(df_a, df_b):
    # Ledger
    ledger = df_a.copy()
    # pick likely amount column
    if "Amount (LCY)" in ledger.columns:
        ledger["amount"] = pd.to_numeric(ledger["Amount (LCY)"], errors="coerce")
    elif "Amount" in ledger.columns:
        ledger["amount"] = pd.to_numeric(ledger["Amount"], errors="coerce")
    else:
        # pick any numeric column with 'amount' substring
        cand = [c for c in ledger.columns if 'amount' in c.lower()]
        ledger["amount"] = pd.to_numeric(ledger[cand[0]], errors="coerce") if cand else np.nan

    # date
    date_cols = ["Posting Date", "VAT Date", "Due Date"]
    found = next((c for c in date_cols if c in ledger.columns), None)
    if found:
        ledger["date"] = pd.to_datetime(ledger[found], errors="coerce")
    else:
        dcols = [c for c in ledger.columns if 'date' in c.lower()]
        ledger["date"] = pd.to_datetime(ledger[dcols[0]], errors="coerce") if dcols else pd.NaT

    # reference & counterparty
    ledger["reference"] = ledger.get("Document No.", ledger.get("Entry No.", ledger.get("Document No", "")))
    ledger["counterparty"] = ledger.get("Counterparty", ledger.get("Customer Name", ""))
    ledger = ledger.reset_index().rename(columns={"index": "ledger_idx"})

    # Bank
    bank = df_b.copy()
    # amount heuristics
    candidates_amt = [c for c in bank.columns if 'amount' in c.lower()]
    bank["amount"] = pd.to_numeric(bank[candidates_amt[0]], errors="coerce") if candidates_amt else np.nan

    # date heuristics
    candidates_date = [c for c in bank.columns if 'date' in c.lower()]
    bank["date"] = pd.to_datetime(bank[candidates_date[0]], errors="coerce", dayfirst=True) if candidates_date else pd.NaT

    # reference heuristics
    if "Statement.Entry.EntryReference" in bank.columns:
        bank["reference"] = bank["Statement.Entry.EntryReference"]
    else:
        ref_cands = [c for c in bank.columns if 'ref' in c.lower() or 'reference' in c.lower()]
        bank["reference"] = bank[ref_cands[0]] if ref_cands else bank.index.astype(str)

    # counterparty heuristics
    debtor_col = "Statement.Entry.EntryDetails.TransactionDetails.RelatedParties.Debtor.Name"
    creditor_col = "Statement.Entry.EntryDetails.TransactionDetails.RelatedParties.Creditor.Name"
    if debtor_col in bank.columns and creditor_col in bank.columns:
        bank["counterparty"] = bank[debtor_col].fillna("") + " " + bank[creditor_col].fillna("")
        bank["counterparty"] = bank["counterparty"].str.strip().replace("", np.nan).fillna("")
    else:
        name_candidates = [c for c in bank.columns if any(k in c.lower() for k in ["name", "party", "relatedparties", "account.name"])]
        bank["counterparty"] = bank[name_candidates[0]] if name_candidates else ""

    bank = bank.reset_index().rename(columns={"index": "bank_idx"})
    return ledger, bank

# -------------------------
# Data Quality Agent
# -------------------------
def compute_dq(df):
    dq = {
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "missing_cells": int(df.isna().sum().sum()),
        "missing_ratio": float(df.isna().sum().sum() / (df.size + 1e-9)),
        "duplicate_rows": int(df.duplicated().sum())
    }
    return dq

# -------------------------
# ML Threshold Agent (simple)
# -------------------------
def train_threshold_agent(labels_path, ledger, bank):
    """
    If labels CSV present, train a classifier on labeled pairs and return a probability threshold.
    labels.csv should contain columns: ledger_idx, bank_idx, label (1 or 0)
    """
    if not SKL_AVAILABLE:
        print("scikit-learn not available, skipping ML Threshold Agent.")
        return None, None

    labels = pd.read_csv(labels_path)
    # build features for labeled pairs
    rows = []
    for _, r in labels.iterrows():
        li = int(r["ledger_idx"])
        bi = int(r["bank_idx"])
        lrow = ledger[ledger["ledger_idx"] == li].iloc[0]
        brow = bank[bank["bank_idx"] == bi].iloc[0]
        name_sim = str_sim(lrow.get("counterparty", ""), brow.get("counterparty", ""))
        ref_sim = str_sim(str(lrow.get("reference", "")), str(brow.get("reference", "")))
        amt_diff = abs(safe_float(lrow.get("amount")) - safe_float(brow.get("amount")))
        date_d = days_diff(lrow.get("date"), brow.get("date")) or 999
        rows.append({"name_sim": name_sim, "ref_sim": ref_sim, "amt_diff": amt_diff, "date_diff": date_d, "label": int(r["label"])})
    df_feat = pd.DataFrame(rows).dropna()
    if df_feat.empty or df_feat["label"].nunique() < 2:
        print("Insufficient label data to train. Need both positive and negative examples.")
        return None, None
    X = df_feat[["name_sim", "ref_sim", "amt_diff", "date_diff"]]
    y = df_feat["label"]
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=1)
    clf = GradientBoostingClassifier(n_estimators=100, random_state=1)
    clf.fit(X_train, y_train)
    # calibrated probabilities
    calib = CalibratedClassifierCV(clf, cv='prefit') if hasattr(CalibratedClassifierCV, '__call__') else None
    if calib is not None:
        calib.fit(X_val, y_val)
        model = calib
    else:
        model = clf

    # choose threshold by maximizing F1 on validation set
    probs = model.predict_proba(X_val)[:, 1]
    best_t = 0.5
    best_f1 = 0.0
    from sklearn.metrics import f1_score
    for t in np.linspace(0.3, 0.9, 31):
        preds = (probs >= t).astype(int)
        f1 = f1_score(y_val, preds)
        if f1 > best_f1:
            best_f1 = f1
            best_t = t
    print(f"Trained ML threshold agent: chosen threshold={best_t:.3f} (val F1={best_f1:.3f})")
    return model, best_t

# -------------------------
# Candidate generation + scoring
# -------------------------
def generate_candidates(ledger, bank, amount_window_pct=0.05, day_window=14, max_candidates=500):
    """
    Simple blocking on amount (±window) and date (±day_window). Returns list of candidate tuples.
    """
    candidates = []
    # pre-index bank by approximate buckets for speed
    bank_idx = bank.reset_index(drop=True)
    for _, l in ledger.iterrows():
        a_amt = safe_float(l.get("amount"))
        a_date = l.get("date")
        if np.isnan(a_amt):
            # fallback: sample top N bank rows
            cand = bank_idx.sample(n=min(max_candidates, len(bank_idx)), random_state=1)
        else:
            window = max(50.0, abs(a_amt) * amount_window_pct)
            mask = bank_idx["amount"].notna() & (abs(bank_idx["amount"] - a_amt) <= window)
            if pd.notna(a_date):
                mask = mask & bank_idx["date"].notna() & (bank_idx["date"].apply(lambda d: abs((d - a_date).days) <= day_window))
            cand = bank_idx[mask]
            if len(cand) == 0:
                # relax window
                mask2 = bank_idx["amount"].notna() & (abs(bank_idx["amount"] - a_amt) <= max(5000.0, abs(a_amt)*0.2))
                cand = bank_idx[mask2]
            if len(cand) == 0:
                cand = bank_idx.nsmallest(min(max_candidates, 200), columns=["amount"], key=lambda x: (x - a_amt).abs()) if not np.isnan(a_amt) else bank_idx.sample(n=min(max_candidates, len(bank_idx)), random_state=1)
        # collect candidate bank idxs
        for _, b in cand.iterrows():
            candidates.append((int(l["ledger_idx"]), int(b["bank_idx"])))
    # deduplicate
    candidates = list(dict.fromkeys(candidates))
    return candidates

def featurize_pair(ledger_row, bank_row):
    name_sim = str_sim(ledger_row.get("counterparty", ""), bank_row.get("counterparty", ""))
    ref_sim = str_sim(str(ledger_row.get("reference", "")), str(bank_row.get("reference", "")))
    amt_diff = None
    if not np.isnan(safe_float(ledger_row.get("amount"))) and not np.isnan(safe_float(bank_row.get("amount"))):
        amt_diff = abs(safe_float(ledger_row.get("amount")) - safe_float(bank_row.get("amount")))
    date_d = days_diff(ledger_row.get("date"), bank_row.get("date"))
    return {"name_sim": name_sim, "ref_sim": ref_sim, "amt_diff": amt_diff if amt_diff is not None else 999999.0, "date_diff": date_d if date_d is not None else 9999}

# -------------------------
# Matching Agent (greedy 1:1 by score)
# -------------------------
def match_candidates(ledger, bank, model=None, threshold=0.6):
    cand_pairs = generate_candidates(ledger, bank)
    rows = []
    for li, bi in cand_pairs:
        lrow = ledger[ledger["ledger_idx"] == li].iloc[0]
        brow = bank[bank["bank_idx"] == bi].iloc[0]
        feat = featurize_pair(lrow, brow)
        # compute score/prob
        if model is not None and SKL_AVAILABLE:
            X = pd.DataFrame([feat])[["name_sim", "ref_sim", "amt_diff", "date_diff"]]
            prob = model.predict_proba(X)[0, 1]
            score = float(prob)
        else:
            # heuristic composite
            # name/ref are in 0..1; amt/date penalize
            amt_score = max(0.0, 1.0 - min(feat["amt_diff"] / max(1.0, abs(safe_float(lrow.get("amount")))), 2.0))
            date_score = max(0.0, 1.0 - min((feat["date_diff"] or 9999) / 14.0, 1.0))
            score = 0.4 * feat["name_sim"] + 0.4 * feat["ref_sim"] + 0.1 * amt_score + 0.1 * date_score
        rows.append({"ledger_idx": li, "bank_idx": bi, "score": score, **feat})
    dfc = pd.DataFrame(rows).sort_values("score", ascending=False)
    # greedy 1:1
    matched = []
    used_l = set()
    used_b = set()
    for _, r in dfc.iterrows():
        if r["ledger_idx"] in used_l or r["bank_idx"] in used_b:
            continue
        if r["score"] >= threshold:
            matched.append(r.to_dict())
            used_l.add(r["ledger_idx"])
            used_b.add(r["bank_idx"])
    matched_df = pd.DataFrame(matched)
    return matched_df

# -------------------------
# Exception Agent
# -------------------------
def generate_exceptions(ledger, bank, matched_df):
    matched_ledger = set(matched_df["ledger_idx"].tolist()) if not matched_df.empty else set()
    matched_bank = set(matched_df["bank_idx"].tolist()) if not matched_df.empty else set()
    exceptions = []
    for _, l in ledger.iterrows():
        if l["ledger_idx"] in matched_ledger:
            continue
        # attempt to find best candidate anyway for explainability
        best_idx, best_feats = None, None
        # reuse find best in featurize logic: pick bank row with highest composite heuristic
        best_score = -1
        for _, b in bank.iterrows():
            feat = featurize_pair(l, b)
            amt_score = max(0.0, 1.0 - min(feat["amt_diff"] / max(1.0, abs(safe_float(l.get("amount")))), 2.0))
            date_score = max(0.0, 1.0 - min((feat["date_diff"] or 9999)/14.0, 1.0))
            score = 0.4 * feat["name_sim"] + 0.4 * feat["ref_sim"] + 0.1 * amt_score + 0.1 * date_score
            if score > best_score:
                best_score = score
                best_idx = int(b["bank_idx"])
                best_feats = {"name_sim": feat["name_sim"], "ref_sim": feat["ref_sim"], "amt_diff": feat["amt_diff"], "date_diff": feat["date_diff"], "composite_score": round(score, 4)}
        # categorize
        cat = "NO_MATCH"
        if best_feats:
            if best_feats["composite_score"] >= 0.5:
                cat = "LOW_CONFIDENCE_MATCH"
            elif best_feats["name_sim"] < 0.25:
                cat = "COUNTERPARTY_MISMATCH"
            elif best_feats["amt_diff"] and best_feats["amt_diff"] > max(1000, abs(safe_float(l.get("amount")))*0.1):
                cat = "AMOUNT_MISMATCH"
            else:
                cat = "REQUIRES_REVIEW"
        exceptions.append({
            "ledger_idx": int(l["ledger_idx"]),
            "ledger_ref": str(l.get("reference", "")),
            "ledger_amount": None if pd.isna(l.get("amount")) else float(l.get("amount")),
            "ledger_date": str(l.get("date")) if not pd.isna(l.get("date")) else "",
            "ledger_counterparty": str(l.get("counterparty", "")),
            "best_bank_idx": best_idx,
            "best_features": best_feats,
            "category": cat
        })
    # bank-side unmatched exceptions (optional)
    for _, b in bank.iterrows():
        if b["bank_idx"] in matched_bank:
            continue
        exceptions.append({
            "bank_idx": int(b["bank_idx"]),
            "bank_ref": str(b.get("reference", "")),
            "bank_amount": None if pd.isna(b.get("amount")) else float(b.get("amount")),
            "bank_date": str(b.get("date")) if not pd.isna(b.get("date")) else "",
            "bank_counterparty": str(b.get("counterparty", "")),
            "category": "BANK_UNMATCHED"
        })
    return pd.DataFrame(exceptions)

# -------------------------
# Orchestration
# -------------------------
def orchestrate(args):
    os.makedirs(args.out, exist_ok=True)
    df_a = load_table(args.a)
    df_b = load_table(args.b)
    ledger, bank = standardize_inputs(df_a, df_b)

    # DQ
    dq_ledger = compute_dq(ledger)
    dq_bank = compute_dq(bank)
    dq_report = {"ledger": dq_ledger, "bank": dq_bank}
    with open(os.path.join(args.out, "dq_metrics.json"), "w") as f:
        json.dump(dq_report, f, indent=2, default=str)
    print("DQ metrics saved.")

    # ML Threshold Agent (if labels provided)
    model, chosen_threshold = None, None
    if args.labels and os.path.exists(args.labels):
        model, chosen_threshold = train_threshold_agent(args.labels, ledger, bank)
    # fallback heuristic thresholds
    if chosen_threshold is None:
        # adapt thresholds by DQ (simple)
        base = 0.6
        if dq_ledger["missing_ratio"] > 0.1 or dq_bank["missing_ratio"] > 0.1:
            base -= 0.1
        chosen_threshold = min(max(base, 0.45), 0.9)
    print(f"Using match threshold = {chosen_threshold:.3f}")

    # Matching Agent
    matched_df = match_candidates(ledger, bank, model=model, threshold=chosen_threshold)
    matched_df.to_csv(os.path.join(args.out, "matched.csv"), index=False)
    print(f"Matched pairs: {len(matched_df)}")

    # produce unmatched outputs
    matched_ledger_idx = set(matched_df["ledger_idx"].tolist()) if not matched_df.empty else set()
    matched_bank_idx = set(matched_df["bank_idx"].tolist()) if not matched_df.empty else set()
    unmatched_ledger_df = ledger[~ledger["ledger_idx"].isin(matched_ledger_idx)].copy()
    unmatched_bank_df = bank[~bank["bank_idx"].isin(matched_bank_idx)].copy()
    unmatched_ledger_df.to_csv(os.path.join(args.out, "unmatched_ledger.csv"), index=False)
    unmatched_bank_df.to_csv(os.path.join(args.out, "unmatched_bank.csv"), index=False)

    # Exception Agent
    exceptions_df = generate_exceptions(ledger, bank, matched_df)
    exceptions_df.to_csv(os.path.join(args.out, "exceptions.csv"), index=False)

    # Audit events (simple)
    audit_events = []
    for _, r in (matched_df.iterrows() if not matched_df.empty else []):
        pass  # kept for backward compat
    # create audit for matched rows
    for _, row in matched_df.iterrows():
        audit_events.append({
            "event": "MATCH",
            "ledger_idx": int(row["ledger_idx"]),
            "bank_idx": int(row["bank_idx"]),
            "score": float(row["score"]),
            "ts": datetime.utcnow().isoformat()
        })
    # audit for exceptions
    for _, ex in exceptions_df.iterrows():
        audit_events.append({
            "event": "EXCEPTION",
            **({k: ex[k] for k in ex.index if k in ["ledger_idx", "bank_idx", "bank_idx", "bank_ref", "ledger_ref"]}),
            "category": ex.get("category"),
            "features": ex.get("best_features"),
            "ts": datetime.utcnow().isoformat()
        })

    with open(os.path.join(args.out, "audit.jsonl"), "w", encoding="utf-8") as f:
        for ev in audit_events:
            f.write(json.dumps(ev, default=str) + "\n")

    # Summary
    summary = {
        "total_ledger": int(len(ledger)),
        "total_bank": int(len(bank)),
        "matched": int(len(matched_df)),
        "exceptions": int(len(exceptions_df)),
        "unmatched_ledger": int(len(unmatched_ledger_df)),
        "unmatched_bank": int(len(unmatched_bank_df)),
        "threshold_used": float(chosen_threshold)
    }
    with open(os.path.join(args.out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print("Orchestration complete. Outputs in", args.out)
    print(json.dumps(summary, indent=2))

# -------------------------
# CLI
# -------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--a", required=True, help="Ledger file (csv/xlsx)")
    parser.add_argument("--b", required=True, help="Bank file (csv/xlsx)")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--mapping", required=False, help="Optional mapping file")
    parser.add_argument("--labels", required=False, help="Optional labels CSV for ML agent")
    args = parser.parse_args()
    orchestrate(args)
