#!/usr/bin/env python3
"""
main.py — Phase 6 Reconciliation Engine (Phase6 -> Phase7 prep)
Usage:
  python main.py [pivot_path] [accrual_path] [config_path]

Defaults if no args:
  pivot:  SW Pivot Table Service Dates 01.01.25_05.31.25.csv
  accrual: SmartWorks Sub Vendor Accrual - 05.31.25.csv
  config:  config_phase6.yaml

Outputs:
  - Phase6_Reconciliation_Report_May2025_Credits_Referrals.xlsx
  - phase6_reconciliation.log

This version:
 - Adds Credit Applied flag column
 - Adds Referral Variance calculation and column
 - Assigns Department_Assigned for unmatched records (AP / Billing)
 - Adds rate × hour dollar impact in vendor summary
 - Implements false variance filtering threshold (config)
 - Enhances vendor summary with revenue vs AP variance metrics
 - Keeps robust CSV/XLSX loading and diagnostic logging
"""
from __future__ import annotations
import sys
import os
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple
import warnings
import pandas as pd
from difflib import SequenceMatcher

warnings.filterwarnings("ignore")

# Logging
LOGFILE = "phase6_reconciliation.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOGFILE), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("phase6")

# Defaults
DEFAULT_PIVOT = "SW Pivot Table Service Dates 01.01.25_05.31.25.csv"
DEFAULT_ACCRUAL = "SmartWorks Sub Vendor Accrual - 05.31.25.csv"
DEFAULT_CONFIG = "config_phase6.yaml"
OUTPUT_XLSX = "Phase6_Reconciliation_Report_May2025_Credits_Referrals.xlsx"

# Config loader
import yaml


def load_config(path: str = DEFAULT_CONFIG) -> Dict:
    if os.path.exists(path):
        with open(path, "r") as fh:
            cfg = yaml.safe_load(fh) or {}
        logger.info(f"Loaded config: {path}")
        base = get_default_config()
        merge_dict(base, cfg)
        return base
    logger.warning(f"Config file {path} not found. Using defaults.")
    return get_default_config()


def get_default_config() -> Dict:
    return {
        "phase": 6,
        "version": "Phase6_May2025_Credits_Referrals",
        "variance_filtering": {
            "ignore_variance_under": 1.00,  # false variance threshold in dollars
            "hour_variance_tolerance": 0.5,
            "amount_variance_tolerance": 1.00,
        },
        "variance_severity": {
            "critical": {"threshold_dollars": 500.0, "threshold_hours": 40},
            "high": {"threshold_dollars": 250.0, "threshold_hours": 20},
            "normal": {"threshold_dollars": 50.0, "threshold_hours": 5},
        },
        "matching": {
            "tier_1_exact": {"name_fuzzy_threshold": 0.80, "amount_tolerance": 0.15},
            "tier_2_fuzzy": {"name_fuzzy_threshold": 0.75, "amount_tolerance": 0.20},
            "tier_3_fallback": {"name_fuzzy_threshold": 0.70, "amount_tolerance": 0.25},
        },
        "referral_logic": {"enabled": True},
    }


def merge_dict(base: Dict, override: Dict) -> None:
    for k, v in (override or {}).items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            merge_dict(base[k], v)
        else:
            base[k] = v


# Helpers
def fuzzy_score(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, str(a).strip().lower(), str(b).strip().lower()).ratio()


def fuzzy_match(a: str, b: str, threshold: float) -> bool:
    return fuzzy_score(a, b) >= threshold


def is_amount_match(a: float, b: float, tolerance: float) -> bool:
    try:
        a = float(a or 0)
        b = float(b or 0)
    except Exception:
        return False
    if a == 0 and b == 0:
        return True
    if max(abs(a), abs(b)) == 0:
        return False
    return abs(a - b) / max(abs(a), abs(b)) <= tolerance


# File readers (CSV or XLSX)
def read_table(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xls", ".xlsx"):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip()
    return df


# Pivot loader
def load_pivot_table(path: str) -> pd.DataFrame:
    df = read_table(path)
    # Normalize numeric columns
    for col in ["Bill_Hours", "Bill_Amount", "Bill_Rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r"[^0-9.\-]", "", regex=True), errors="coerce"
            ).fillna(0)
        else:
            df[col] = 0
    # Service month
    date_col = None
    for c in ("Assignment_PeriodEndingDate", "Period_EndDate", "Assignment Period Ending Date", "Period_End_Date"):
        if c in df.columns:
            date_col = c
            break
    if date_col:
        df["Service_Month"] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m")
    else:
        df["Service_Month"] = None
    # consultant id
    if "consultant_id" not in df.columns:
        for cand in ("Consultant_ID", "APPL_ID", "Applicant_ID", "CUST_ID"):
            if cand in df.columns:
                df["consultant_id"] = df[cand]
                break
        else:
            df["consultant_id"] = None
    # consultant name
    if "Consultant_Name" not in df.columns:
        if "CUST_NAME" in df.columns:
            df["Consultant_Name"] = df["CUST_NAME"]
        else:
            df["Consultant_Name"] = None
    logger.info(f"✅ Loaded {len(df)} pivot table records from {path}")
    logger.debug(f"Pivot columns: {list(df.columns)}")
    return df


# Accrual loader with robust mapping
def load_accrual_data(path: str) -> pd.DataFrame:
    df = read_table(path)
    df_cols_lower = {c.lower(): c for c in df.columns}

    # consultant id
    consultant_id_candidates = [
        "consultant_id",
        "consultant id",
        "consultant id ",
        "consultantid",
        "applicant_id",
        "appl_id",
        "cust_id",
        "applicant number",
        "applicant #",
    ]
    for cand in consultant_id_candidates:
        if cand in df_cols_lower:
            df["consultant_id"] = df[df_cols_lower[cand]]
            break
    else:
        df["consultant_id"] = None

    # consultant name
    name_candidates = ["consultant_name", "consultant name", "name", "vendor name", "vendor"]
    for cand in name_candidates:
        if cand in df_cols_lower:
            df["Consultant_Name"] = df[df_cols_lower[cand]]
            break
    else:
        df["Consultant_Name"] = None

    # accrued hours
    hours_candidates = ["accrued_hours", "accrued hours", "hours", "total_hours", "total hours", "hours_billed"]
    for cand in hours_candidates:
        if cand in df_cols_lower:
            df["Accrued_Hours"] = pd.to_numeric(
                df[df_cols_lower[cand]].astype(str).str.replace(r"[^0-9.\-]", "", regex=True), errors="coerce"
            ).fillna(0)
            break
    else:
        df["Accrued_Hours"] = 0

    # accrued amount
    amount_candidates = ["accrued_amount", "accrued amount", "amount", "total_amount", "accrued_amt", "accrual amount"]
    for cand in amount_candidates:
        if cand in df_cols_lower:
            df["Accrued_Amount"] = pd.to_numeric(
                df[df_cols_lower[cand]].astype(str).str.replace(r"[^0-9.\-]", "", regex=True), errors="coerce"
            ).fillna(0)
            break
    else:
        df["Accrued_Amount"] = 0

    # Service_Month inference
    if "Service_Month" not in df.columns:
        df["Service_Month"] = None
        for c in df.columns:
            if "date" in c.lower() or "service" in c.lower():
                try:
                    sm = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m")
                    if sm.notna().any():
                        df["Service_Month"] = sm
                        break
                except Exception:
                    continue

    logger.info(f"✅ Loaded {len(df)} accrual records from {path}")
    # diagnostics
    logger.info(f"Accrual columns: {list(df.columns)}")
    try:
        logger.info("Accrual sample: %s", df[["consultant_id", "Consultant_Name", "Accrued_Hours", "Accrued_Amount", "Service_Month"]].head(5).to_dict(orient="records"))
    except Exception:
        logger.debug("Accrual sample not available in expected columns.")
    return df


# Credit detection
def detect_credit(row: pd.Series) -> Tuple[bool, str]:
    reasons = []
    try:
        hours = float(row.get("Bill_Hours") or 0)
    except Exception:
        hours = 0
    try:
        amt = float(row.get("Bill_Amount") or 0)
    except Exception:
        amt = 0
    status = str(row.get("Record_Status") or "").strip().upper()
    if hours < 0:
        reasons.append("Negative_Bill_Hours")
    if amt < 0:
        reasons.append("Negative_Bill_Amount")
    if status == "DISC":
        reasons.append("Discount_Flag")
    return (len(reasons) > 0, " | ".join(reasons) if reasons else "None")


# 3-tier matching
def tier1_match(billed_row: pd.Series, accrual_df: pd.DataFrame, cfg: Dict) -> Optional[int]:
    cid = billed_row.get("consultant_id")
    if not cid:
        return None
    cand = accrual_df[accrual_df["consultant_id"].astype(str) == str(cid)]
    if cand.empty:
        return None
    amount_tol = cfg["matching"]["tier_1_exact"]["amount_tolerance"]
    for idx, arow in cand.iterrows():
        if is_amount_match(float(billed_row.get("Bill_Amount") or 0), float(arow.get("Accrued_Amount") or 0), amount_tol):
            return idx
    return None


def tier2_match(billed_row: pd.Series, accrual_df: pd.DataFrame, cfg: Dict) -> Optional[int]:
    cid = billed_row.get("consultant_id")
    if not cid:
        return None
    cand = accrual_df[accrual_df["consultant_id"].astype(str) == str(cid)]
    amount_tol = cfg["matching"]["tier_2_fuzzy"]["amount_tolerance"]
    name_thresh = cfg["matching"]["tier_2_fuzzy"]["name_fuzzy_threshold"]
    for idx, arow in cand.iterrows():
        if fuzzy_match(billed_row.get("Consultant_Name", ""), arow.get("Consultant_Name", ""), name_thresh):
            if is_amount_match(float(billed_row.get("Bill_Amount") or 0), float(arow.get("Accrued_Amount") or 0), amount_tol):
                return idx
    return None


def tier3_match(billed_row: pd.Series, accrual_df: pd.DataFrame, cfg: Dict) -> Optional[int]:
    name_thresh = cfg["matching"]["tier_3_fallback"]["name_fuzzy_threshold"]
    amount_tol = cfg["matching"]["tier_3_fallback"]["amount_tolerance"]
    for idx, arow in accrual_df.iterrows():
        if fuzzy_match(billed_row.get("Consultant_Name", ""), arow.get("Consultant_Name", ""), name_thresh):
            if is_amount_match(float(billed_row.get("Bill_Amount") or 0), float(arow.get("Accrued_Amount") or 0), amount_tol):
                return idx
    return None


def find_matching_accrual(billed_row: pd.Series, accrual_df: pd.DataFrame, cfg: Dict) -> Tuple[Optional[int], str]:
    idx = tier1_match(billed_row, accrual_df, cfg)
    if idx is not None:
        return idx, "Tier1_Exact"
    idx = tier2_match(billed_row, accrual_df, cfg)
    if idx is not None:
        return idx, "Tier2_Fuzzy"
    idx = tier3_match(billed_row, accrual_df, cfg)
    if idx is not None:
        return idx, "Tier3_Fallback"
    return None, "No_Match"


# Referral calculation across pivot rows
def calc_referral_variances(pivot_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify consultants with referral vendors and compute referral fee variance.
    Heuristic: vendor name contains 'referral' or 'ref' (case-insensitive).
    Returns DataFrame with consultant_id, referral_variance, referral_note
    """
    out = []
    if pivot_df.empty:
        return pd.DataFrame(out)
    for cid, group in pivot_df.groupby("consultant_id", dropna=False):
        if pd.isna(cid):
            continue
        vendors = group["T_INVOICE_COMPANY_NAME"].astype(str).str.strip().fillna("").tolist()
        # classify rows
        referral_mask = group["T_INVOICE_COMPANY_NAME"].astype(str).str.lower().str.contains("referral|referral fee|ref$")
        sub_mask = ~referral_mask
        sub_total = group.loc[sub_mask, "Bill_Amount"].astype(float).sum() if not group.loc[sub_mask].empty else 0.0
        ref_total = group.loc[referral_mask, "Bill_Amount"].astype(float).sum() if not group.loc[referral_mask].empty else 0.0
        referral_variance = float(ref_total) - float(sub_total)
        note = "No_Referral_Vendor" if abs(referral_variance) < 0.01 else f"Referral_Fee_Diff_${referral_variance:.2f}"
        out.append({"consultant_id": cid, "referral_variance": referral_variance, "referral_note": note})
    return pd.DataFrame(out)


# Variance classification and flags
def classify_severity(amount_variance: float, hour_variance: float, cfg: Dict) -> str:
    crit = cfg["variance_severity"]["critical"]
    high = cfg["variance_severity"]["high"]
    normal = cfg["variance_severity"]["normal"]
    if abs(amount_variance) >= crit["threshold_dollars"] or abs(hour_variance) >= crit["threshold_hours"]:
        return "Critical"
    if abs(amount_variance) >= high["threshold_dollars"] or abs(hour_variance) >= high["threshold_hours"]:
        return "High"
    if abs(amount_variance) >= normal["threshold_dollars"] or abs(hour_variance) >= normal["threshold_hours"]:
        return "Normal"
    return "Negligible"


def calculate_variances(billed_row: pd.Series, accrual_row: pd.Series) -> Dict:
    billed_hours = float(billed_row.get("Bill_Hours") or 0)
    accrued_hours = float(accrual_row.get("Accrued_Hours") or 0) if accrual_row is not None else 0
    billed_amount = float(billed_row.get("Bill_Amount") or 0)
    accrued_amount = float(accrual_row.get("Accrued_Amount") or 0) if accrual_row is not None else 0
    billed_rate = float(billed_row.get("Bill_Rate") or 0)
    hour_variance = billed_hours - accrued_hours
    amount_variance = billed_amount - accrued_amount
    rate_impact = hour_variance * billed_rate if billed_rate else 0
    return {
        "billed_hours": billed_hours,
        "accrued_hours": accrued_hours,
        "hour_variance": hour_variance,
        "billed_amount": billed_amount,
        "accrued_amount": accrued_amount,
        "amount_variance": amount_variance,
        "billed_rate": billed_rate,
        "rate_impact_$": rate_impact,
    }


def assign_flag_and_department(billed_hours: float, accrued_hours: float, amount_variance: float, is_credit: bool, cfg: Dict) -> Tuple[str, str]:
    ignore = float(cfg["variance_filtering"].get("ignore_variance_under", 1.0))
    # Priority: Missing Vendor Invoice
    if billed_hours > 0 and accrued_hours == 0:
        return "Missing Vendor Invoice", "Accounts Payable"
    # Unbilled to client
    if billed_hours == 0 and accrued_hours > 0:
        return "Unbilled to Client", "Billing"
    # Credit applied
    if is_credit:
        return "Credit Applied", "Accounting"
    # negligible
    if abs(amount_variance) < ignore:
        return "Balanced", "None"
    # otherwise review
    return "Variance Review", "Finance"


# Main reconcile orchestration
def reconcile_data(pivot_df: pd.DataFrame, accrual_df: pd.DataFrame, cfg: Dict) -> Dict[str, pd.DataFrame]:
    logger.info("Starting reconciliation...")
    matched = []
    unmatched_billed = []
    accrual_df["_processed"] = False

    # Precompute referral variances from pivot
    referral_df = calc_referral_variances(pivot_df) if cfg.get("referral_logic", {}).get("enabled", True) else pd.DataFrame()
    # iterate billed rows
    for i, brow in pivot_df.iterrows():
        is_credit, credit_reason = detect_credit(brow)
        match_idx, tier = find_matching_accrual(brow, accrual_df, cfg)
        if match_idx is not None:
            arow = accrual_df.loc[match_idx]
            accrual_df.at[match_idx, "_processed"] = True
            vars_ = calculate_variances(brow, arow)
            # apply false variance filter
            if abs(vars_["amount_variance"]) < float(cfg["variance_filtering"].get("ignore_variance_under", 1.0)):
                flag, dept = "Balanced", "None"
                severity = "Negligible"
            else:
                flag, dept = assign_flag_and_department(vars_["billed_hours"], vars_["accrued_hours"], vars_["amount_variance"], is_credit, cfg)
                severity = classify_severity(vars_["amount_variance"], vars_["hour_variance"], cfg)
            # referral join
            ref_row = referral_df[referral_df["consultant_id"].astype(str) == str(brow.get("consultant_id"))]
            referral_variance = float(ref_row["referral_variance"].iloc[0]) if (not ref_row.empty) else 0.0
            referral_note = ref_row["referral_note"].iloc[0] if (not ref_row.empty) else "No_Referral_Vendor"
            matched.append(
                {
                    "consultant_id": brow.get("consultant_id"),
                    "consultant_name": brow.get("Consultant_Name") or brow.get("CUST_NAME"),
                    "vendor_name": brow.get("T_INVOICE_COMPANY_NAME"),
                    "invoice_number": brow.get("Invoice_Number"),
                    "service_month": brow.get("Service_Month"),
                    "billed_hours": vars_["billed_hours"],
                    "accrued_hours": vars_["accrued_hours"],
                    "hour_variance": vars_["hour_variance"],
                    "billed_amount": vars_["billed_amount"],
                    "accrued_amount": vars_["accrued_amount"],
                    "amount_variance": vars_["amount_variance"],
                    "billed_rate": vars_["billed_rate"],
                    "rate_impact_$": vars_["rate_impact_$"],
                    "dollar_difference": vars_["hour_variance"] * vars_["billed_rate"],
                    "flag_type": flag,
                    "department_assigned": dept,
                    "credit_applied": "Y" if is_credit else "N",
                    "credit_reason": credit_reason,
                    "severity": severity,
                    "match_tier": tier,
                    "referral_variance": referral_variance,
                    "referral_note": referral_note,
                    "last_run_date": datetime.utcnow().strftime("%Y-%m-%d"),
                    "detected_change": "Y" if vars_["amount_variance"] != 0 else "N",
                }
            )
        else:
            # unmatched billed -> Missing Vendor Invoice (AP)
            unmatched_billed.append(
                {
                    "consultant_id": brow.get("consultant_id"),
                    "consultant_name": brow.get("Consultant_Name") or brow.get("CUST_NAME"),
                    "vendor_name": brow.get("T_INVOICE_COMPANY_NAME"),
                    "invoice_number": brow.get("Invoice_Number"),
                    "service_month": brow.get("Service_Month"),
                    "billed_hours": float(brow.get("Bill_Hours") or 0),
                    "billed_amount": float(brow.get("Bill_Amount") or 0),
                    "hour_variance": float(brow.get("Bill_Hours") or 0),
                    "amount_variance": float(brow.get("Bill_Amount") or 0),
                    "billed_rate": float(brow.get("Bill_Rate") or 0),
                    "dollar_difference": (float(brow.get("Bill_Hours") or 0)) * float(brow.get("Bill_Rate") or 0),
                    "flag_type": "Missing Vendor Invoice",
                    "department_assigned": "Accounts Payable",
                    "credit_applied": "Y" if is_credit else "N",
                    "credit_reason": credit_reason,
                    "referral_variance": float(referral_df[referral_df["consultant_id"].astype(str) == str(brow.get("consultant_id"))]["referral_variance"].iloc[0]) if not referral_df.empty and not referral_df[referral_df["consultant_id"].astype(str) == str(brow.get("consultant_id"))].empty else 0.0,
                }
            )

    # remaining accrual rows not matched -> Unbilled to client (Billing)
    unmatched_accrual = []
    for idx, arow in accrual_df[accrual_df["_processed"] == False].iterrows():
        unmatched_accrual.append(
            {
                "consultant_id": arow.get("consultant_id"),
                "consultant_name": arow.get("Consultant_Name"),
                "service_month": arow.get("Service_Month"),
                "accrued_hours": float(arow.get("Accrued_Hours") or 0),
                "accrued_amount": float(arow.get("Accrued_Amount") or 0),
                "flag_type": "Unbilled to Client",
                "department_assigned": "Billing",
                "referral_variance": 0.0,
            }
        )

    # Create DataFrames
    matched_df = pd.DataFrame(matched)
    unmatched_pivot_df = pd.DataFrame(unmatched_billed)
    unmatched_accrual_df = pd.DataFrame(unmatched_accrual)

    # Executive summary
    total_billed_hours = pivot_df["Bill_Hours"].sum() if "Bill_Hours" in pivot_df else 0
    total_accrued_hours = accrual_df["Accrued_Hours"].sum() if "Accrued_Hours" in accrual_df else 0
    total_billed_amount = pivot_df["Bill_Amount"].sum() if "Bill_Amount" in pivot_df else 0
    total_accrued_amount = accrual_df["Accrued_Amount"].sum() if "Accrued_Amount" in accrual_df else 0
    match_count = len(matched_df)
    unmatched_billed_count = len(unmatched_pivot_df)
    unmatched_accrual_count = len(unmatched_accrual_df)
    denom = match_count + unmatched_billed_count + unmatched_accrual_count or 1
    match_rate = f"{(match_count / denom * 100):.1f}%"

    executive = pd.DataFrame(
        {
            "Metric": [
                "Total Billed Hours",
                "Total Accrued Hours",
                "Total Billed Amount ($)",
                "Total Accrued Amount ($)",
                "Match Count",
                "Unmatched (Billed Only)",
                "Unmatched (Accrued Only)",
                "Match Rate",
            ],
            "Value": [total_billed_hours, total_accrued_hours, total_billed_amount, total_accrued_amount, match_count, unmatched_billed_count, unmatched_accrual_count, match_rate],
        }
    )

    # Vendor summary (grouped by vendor)
    if not matched_df.empty:
        vendor_summary = matched_df.groupby(["vendor_name"], dropna=False).agg(
            billed_hours=pd.NamedAgg(column="billed_hours", aggfunc="sum"),
            accrued_hours=pd.NamedAgg(column="accrued_hours", aggfunc="sum"),
            hour_variance=pd.NamedAgg(column="hour_variance", aggfunc="sum"),
            billed_amount=pd.NamedAgg(column="billed_amount", aggfunc="sum"),
            accrued_amount=pd.NamedAgg(column="accrued_amount", aggfunc="sum"),
            amount_variance=pd.NamedAgg(column="amount_variance", aggfunc="sum"),
            rate_impact_dollars=pd.NamedAgg(column="rate_impact_$", aggfunc="sum"),
        ).reset_index()
        # match % and revenue vs ap variance
        vendor_summary["match_%"] = (1 - (vendor_summary["hour_variance"].abs() / vendor_summary["billed_hours"].replace(0, 1))) * 100
        vendor_summary["revenue_vs_ap_variance_$"] = vendor_summary["billed_amount"] - vendor_summary["accrued_amount"]
        # status using thresholds from cfg
        def _status(x):
            crit = cfg["variance_severity"]["critical"]["threshold_dollars"]
            high = cfg["variance_severity"]["high"]["threshold_dollars"]
            normal = cfg["variance_severity"]["normal"]["threshold_dollars"]
            if abs(x) >= crit:
                return "Critical"
            if abs(x) >= high:
                return "High"
            if abs(x) >= normal:
                return "Normal"
            return "Balanced"
        vendor_summary["status"] = vendor_summary["amount_variance"].apply(_status)
    else:
        vendor_summary = pd.DataFrame()

    # Auto-accruals (unmatched billed transformed for QB)
    auto_accruals = unmatched_pivot_df.rename(columns={"consultant_id": "Consultant_ID", "consultant_name": "Consultant_Name", "service_month": "Service_Month", "billed_hours": "Hours", "billed_amount": "Amount"}) if not unmatched_pivot_df.empty else pd.DataFrame()

    # Audit trail
    audit_cols = ["consultant_id", "consultant_name", "service_month", "last_run_date", "detected_change", "flag_type", "department_assigned", "credit_applied", "severity"]
    audit_trail = matched_df[audit_cols] if not matched_df.empty and all(c in matched_df.columns for c in audit_cols) else pd.DataFrame()

    return {
        "executive_summary": executive,
        "matched_records": matched_df,
        "unmatched_pivot_ap": unmatched_pivot_df,
        "unmatched_accrual_billing": unmatched_accrual_df,
        "auto_accruals": auto_accruals,
        "vendor_summary": vendor_summary,
        "audit_trail": audit_trail,
    }


# Excel export
def export_to_excel(tables: Dict[str, pd.DataFrame], out_path: str = OUTPUT_XLSX) -> None:
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for name, df in tables.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_excel(writer, sheet_name=name[:31], index=False)
    logger.info(f"Exported workbook: {out_path}")


# Entrypoint
def main():
    pivot = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PIVOT
    accrual = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_ACCRUAL
    config_path = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_CONFIG

    logger.info("=" * 70)
    logger.info(f"🚀 PHASE 6 RECONCILIATION ENGINE - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info("=" * 70)
    cfg = load_config(config_path)
    logger.info(f"Phase: {cfg.get('phase')} | Version: {cfg.get('version')}")
    logger.info(f"Using pivot: {pivot}")
    logger.info(f"Using accrual: {accrual}")

    try:
        pivot_df = load_pivot_table(pivot)
    except Exception as e:
        logger.error(f"❌ Error loading pivot table: {e}")
        raise

    try:
        accrual_df = load_accrual_data(accrual)
    except Exception as e:
        logger.error(f"❌ Error loading accrual data: {e}")
        raise

    tables = reconcile_data(pivot_df, accrual_df, cfg)
    export_to_excel(tables, OUTPUT_XLSX)
    logger.info("=" * 70)
    logger.info("✅ PHASE 6 RECONCILIATION COMPLETE")
    logger.info(f"📊 Output: {OUTPUT_XLSX}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
