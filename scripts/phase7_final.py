#!/usr/bin/env python3
"""
phase7_final.py

Phase 7 generator — reads the latest Phase5 workbook for a given month/year (or an explicit
Phase5 workbook path) and produces a Phase7 7-tab workbook:

 - executive_summary
 - matched_records
 - unmatched_pivot_ap
 - unmatched_accrual_billing
 - auto_accruals
 - vendor_summary
 - audit_trail

Usage:
  python3 subcontractor/scripts/phase7_final.py --month may --year 2025
  python3 subcontractor/scripts/phase7_final.py --input data/output/may_2025/Phase5_Complete_Report_11.10.2025_203000.xlsx

If run with month/year, the script will look for the most recent Phase5 workbook in:
  data/output/{month}_{year}/
It supports Phase5 filenames produced by your Phase5 scripts (Phase5_Report_*, Phase5_Complete_*, Phase5_Reconciliation_Report_*).

Output:
  data/output/{month}_{year}/Phase7_Reconciliation_Report_{month}_{year}_{timestamp}.xlsx
"""
from __future__ import annotations
import argparse
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
import pandas as pd
import yaml

# --- logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("phase7")

# --- defaults
DEFAULT_OUTPUT_DIR_TEMPLATE = Path("data") / "output" / "{month}_{year}"
DEFAULT_PHASE5_PATTERNS = [
    "Phase5_Reconciliation_Report_",
    "Phase5_Report_",
    "Phase5_Complete_Report_",
    "Phase5_Reconciliation_Report",
    "Phase5_Complete_Report",
    "Phase5_Report",
]

DEFAULT_CONFIG_PATH = Path("config") / "variance_thresholds.yaml"


def load_config(cfg_path: Path = DEFAULT_CONFIG_PATH) -> Dict:
    """Load thresholds from YAML if available; fall back to defaults."""
    defaults = {
        "amount_thresholds": {"critical": 5000.0, "high": 1000.0, "normal": 0.0},
        "hours_thresholds": {"critical": 50.0, "high": 20.0, "normal": 0.0},
    }
    try:
        if cfg_path.exists():
            with open(cfg_path, "r") as fh:
                cfg = yaml.safe_load(fh) or {}
            # merge defaults
            for k, v in defaults.items():
                if k not in cfg:
                    cfg[k] = v
                else:
                    for kk, vv in v.items():
                        cfg[k].setdefault(kk, vv)
            logger.info(f"Loaded config: {cfg_path}")
            return cfg
    except Exception as e:
        logger.warning(f"Error loading config {cfg_path}: {e}")
    logger.info("Using default thresholds")
    return defaults


def find_latest_phase5_workbook(output_dir: Path) -> Optional[Path]:
    """Find the latest Phase5 workbook in the given output directory."""
    if not output_dir.exists() or not output_dir.is_dir():
        return None
    candidates = []
    for p in output_dir.iterdir():
        if not p.is_file():
            continue
        name = p.name.lower()
        if any(tok.lower() in name for tok in DEFAULT_PHASE5_PATTERNS):
            candidates.append(p)
    if not candidates:
        return None
    # return most recently modified
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    return latest


def read_possible_sheets(phase5_path: Path) -> Dict[str, pd.DataFrame]:
    """Attempt to read expected sheets from a Phase5 workbook. If missing, try to infer."""
    logger.info(f"Reading Phase5 workbook: {phase5_path}")
    xl = pd.ExcelFile(phase5_path)
    sheets = {}
    for expected in [
        "Executive Summary",
        "executive_summary",
        "Reconciliation",
        "reconciled",
        "matched_records",
        "Matched Records",
        "Unmatched Pivot",
        "unmatched_pivot_ap",
        "Unmatched Accrual",
        "unmatched_accrual_billing",
        "Auto-Accrual Suggestions",
        "auto_accruals",
        "Vendor Summary",
        "vendor_summary",
        "Audit Trail",
        "audit_trail",
    ]:
        if expected in xl.sheet_names:
            try:
                df = pd.read_excel(phase5_path, sheet_name=expected)
                sheets[expected] = df
            except Exception:
                sheets[expected] = pd.DataFrame()
    # Normalize keys for downstream usage
    normalized = {}
    # matched_records
    for key in ("Reconciliation", "reconciled", "matched_records", "Matched Records"):
        if key in sheets and not sheets[key].empty:
            normalized["matched_records"] = sheets[key]
            break
    # unmatched pivot
    for key in ("Unmatched Pivot", "unmatched_pivot_ap"):
        if key in sheets and not sheets[key].empty:
            normalized["unmatched_pivot_ap"] = sheets[key]
            break
    # unmatched accrual
    for key in ("Unmatched Accrual", "unmatched_accrual_billing"):
        if key in sheets and not sheets[key].empty:
            normalized["unmatched_accrual_billing"] = sheets[key]
            break
    # auto accruals
    for key in ("Auto-Accrual Suggestions", "auto_accruals"):
        if key in sheets and not sheets[key].empty:
            normalized["auto_accruals"] = sheets[key]
            break
    # vendor summary
    for key in ("Vendor Summary", "vendor_summary"):
        if key in sheets and not sheets[key].empty:
            normalized["vendor_summary"] = sheets[key]
            break
    # executive & audit
    for key in ("Executive Summary", "executive_summary"):
        if key in sheets and not sheets[key].empty:
            normalized["executive_summary"] = sheets[key]
            break
    for key in ("Audit Trail", "audit_trail"):
        if key in sheets and not sheets[key].empty:
            normalized["audit_trail"] = sheets[key]
            break
    return normalized


def promote_department_and_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure department_assigned exists and apply simple promotion rules."""
    if df is None or df.empty:
        return df
    df = df.copy()
    if "department_assigned" not in df.columns:
        df["department_assigned"] = None
    if "flag_type" not in df.columns:
        df["flag_type"] = None
    # Rules:
    df.loc[df["flag_type"].astype(str).str.contains("Missing", na=False), "department_assigned"] = "Accounts Payable"
    df.loc[df["flag_type"].astype(str).str.contains("Unbilled", na=False), "department_assigned"] = "Billing"
    # credit applied -> Accounting
    df.loc[df.get("credit_applied", "").astype(str).str.upper() == "Y", "department_assigned"] = "Accounting"
    df["department_assigned"] = df["department_assigned"].fillna("Finance/Review")
    return df


def compute_vendor_summary_enhancements(df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    """Add revenue_vs_ap_variance and status/severity to vendor summary if possible."""
    if df is None or df.empty:
        return df
    df = df.copy()
    # Normalize column names we expect
    # Try to detect billed/accrued columns
    if "billed_amount" in df.columns and "accrued_amount" in df.columns:
        df["revenue_vs_ap_variance_$"] = df["billed_amount"] - df["accrued_amount"]
    elif "total_billed" in df.columns and "total_invoiced" in df.columns:
        df["revenue_vs_ap_variance_$"] = df["total_billed"] - df["total_invoiced"]
    elif "billed_amount" in df.columns and "accrued_amount" not in df.columns:
        df["revenue_vs_ap_variance_$"] = df["billed_amount"]
    else:
        df["revenue_vs_ap_variance_$"] = 0.0

    # severity using config thresholds
    amt_thresh = cfg.get("amount_thresholds", {"critical": 5000, "high": 1000})
    crit = amt_thresh.get("critical", 5000)
    high = amt_thresh.get("high", 1000)

    def _status(val):
        try:
            v = float(val or 0)
        except Exception:
            v = 0.0
        if abs(v) >= crit:
            return "Critical"
        if abs(v) >= high:
            return "High"
        return "Normal"

    df["status"] = df["revenue_vs_ap_variance_$"].apply(_status)
    return df


def build_executive_summary_from_sources(sources: Dict[str, pd.DataFrame], out_folder: Path) -> pd.DataFrame:
    """Construct a compact executive summary DataFrame."""
    matched = sources.get("matched_records", pd.DataFrame())
    unmatched_pivot = sources.get("unmatched_pivot_ap", pd.DataFrame())
    unmatched_accrual = sources.get("unmatched_accrual_billing", pd.DataFrame())
    vendor_summary = sources.get("vendor_summary", pd.DataFrame())

    total_billed = 0.0
    total_accrued = 0.0
    if not matched.empty:
        for col in ("amount_billed", "amount_billed".lower(), "Billed $", "Billed"):
            if col in matched.columns:
                total_billed = float(matched[col].sum())
                break
    if not vendor_summary.empty:
        for col in ("total_invoiced", "accrued_amount", "total_accrued", "total_invoiced".lower()):
            if col in vendor_summary.columns:
                try:
                    total_accrued = float(vendor_summary[col].sum())
                    break
                except Exception:
                    total_accrued = 0.0

    matched_count = len(matched) if not matched.empty else 0
    total_records = matched_count + (len(unmatched_pivot) if not unmatched_pivot.empty else 0) + (len(unmatched_accrual) if not unmatched_accrual.empty else 0)
    match_rate = f"{(matched_count / total_records * 100):.1f}%" if total_records > 0 else "0.0%"

    data = {
        "Metric": [
            "Report Generated",
            "Phase5 Source",
            "Total Records (scope)",
            "Matched Records",
            "Match Rate",
            "Total Billed ($)",
            "Total Accrued ($)",
            "Total Variance ($)",
            "Vendor Count (summary)",
            "Notes",
        ],
        "Value": [
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            str(out_folder.resolve()),
            total_records,
            matched_count,
            match_rate,
            f"${total_billed:,.2f}",
            f"${total_accrued:,.2f}",
            f"${(total_billed - total_accrued):,.2f}",
            len(vendor_summary) if not vendor_summary.empty else 0,
            "Phase7: department assignment + vendor revenue vs AP variance",
        ],
    }
    return pd.DataFrame(data)


def write_phase7_workbook(sources: Dict[str, pd.DataFrame], out_path: Path) -> Path:
    """Write the 7-tab Phase7 workbook."""
    ensure_dir = out_path.parent
    ensure_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    workbook = out_path.with_name(f"Phase7_Reconciliation_Report_{out_path.parent.name}_{timestamp}.xlsx")

    exec_summary = build_executive_summary_from_sources(sources, out_path.parent)

    # Prepare all tabs, ensure each is a DataFrame (and non-empty where possible)
    tabs = {
        "executive_summary": exec_summary,
        "matched_records": sources.get("matched_records", pd.DataFrame()),
        "unmatched_pivot_ap": sources.get("unmatched_pivot_ap", pd.DataFrame()),
        "unmatched_accrual_billing": sources.get("unmatched_accrual_billing", pd.DataFrame()),
        "auto_accruals": sources.get("auto_accruals", pd.DataFrame()),
        "vendor_summary": sources.get("vendor_summary", pd.DataFrame()),
        "audit_trail": sources.get("audit_trail", pd.DataFrame()),
    }

    # If audit trail not provided, synthesize a small one
    if tabs["audit_trail"].empty:
        tabs["audit_trail"] = pd.DataFrame(
            [
                {
                    "Check": "Phase7 generation",
                    "Status": "✅ Completed",
                    "Details": f"Built from Phase5 source workbook and aggregated tabs",
                    "Timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                }
            ]
        )

    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        for name, df in tabs.items():
            # ensure there is at least a note row if empty
            if df is None or df.empty:
                pd.DataFrame([{"note": f"{name} - no data"}]).to_excel(writer, sheet_name=name[:31], index=False)
            else:
                # sanitize column names (Excel-friendly)
                safe_df = df.copy()
                safe_df.columns = [str(c)[:120] for c in safe_df.columns]
                try:
                    safe_df.to_excel(writer, sheet_name=name[:31], index=False)
                except Exception:
                    # as fallback, export only first 1000 rows
                    safe_df.head(1000).to_excel(writer, sheet_name=name[:31], index=False)

    logger.info(f"Phase7 workbook written: {workbook}")
    return workbook


def main():
    parser = argparse.ArgumentParser(description="Phase7 Reconciliation Generator")
    parser.add_argument("--month", help="month name (e.g., may)", required=False)
    parser.add_argument("--year", type=int, help="year (e.g., 2025)", required=False)
    parser.add_argument("--input", help="explicit Phase5 workbook path", required=False)
    parser.add_argument("--out", help="explicit Phase7 output path (file)", required=False)
    parser.add_argument("--config", help="path to variance_thresholds.yaml", default=str(DEFAULT_CONFIG_PATH))
    args = parser.parse_args()

    cfg = load_config(Path(args.config))

    phase5_path = None
    out_folder = None

    if args.input:
        phase5_path = Path(args.input)
        if not phase5_path.exists():
            logger.error(f"Provided input workbook not found: {phase5_path}")
            raise SystemExit(2)
        out_folder = phase5_path.parent
    else:
        if not (args.month and args.year):
            logger.error("Either --input or both --month and --year must be provided.")
            raise SystemExit(2)
        out_folder = Path("data") / "output" / f"{args.month}_{args.year}"
        phase5_path = find_latest_phase5_workbook(out_folder)
        if phase5_path is None:
            logger.error(f"No Phase5 workbook found in folder: {out_folder}")
            raise SystemExit(2)

    sources = read_possible_sheets(phase5_path)

    # If matched_records not present, try reading a "Reconciliation" sheet or using entire workbook first sheet
    if "matched_records" not in sources or sources.get("matched_records", pd.DataFrame()).empty:
        # try to read "Reconciliation" explicitly
        try:
            df_try = pd.read_excel(phase5_path, sheet_name="Reconciliation")
            sources["matched_records"] = df_try
            logger.info("Loaded Reconciliation sheet into matched_records")
        except Exception:
            # fallback: first sheet
            try:
                first = pd.read_excel(phase5_path, sheet_name=0)
                sources.setdefault("matched_records", first)
                logger.info("Loaded first sheet of workbook as matched_records (fallback)")
            except Exception:
                logger.warning("Could not load any matched records data from Phase5 workbook")

    # Apply Phase7 enhancements
    # department assignment & flags
    if "matched_records" in sources and not sources["matched_records"].empty:
        sources["matched_records"] = promote_department_and_flags(sources["matched_records"])

    # vendor summary enhancements
    sources["vendor_summary"] = compute_vendor_summary_enhancements(sources.get("vendor_summary", pd.DataFrame()), cfg)

    # if out path explicitly provided, use it; else place into out_folder
    if args.out:
        out_path = Path(args.out)
    else:
        out_path = out_folder / f"Phase7_Reconciliation_Report_{out_folder.name}.xlsx"

    written = write_phase7_workbook(sources, out_path)
    logger.info("Phase7 generation complete.")
    logger.info(f"Output: {written}")


if __name__ == "__main__":
    main()