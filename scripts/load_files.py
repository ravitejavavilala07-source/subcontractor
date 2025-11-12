"""
Load Files Module

Purpose: Import and validate Excel input files for reconciliation pipeline
- Loads UltraStaff Pivot Table (client billing)
- Loads Vendor Accrual Workbook (3 sheets with vendor data)
- Skips invalid QuickBooks GL exports
- Standardizes column names
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class FileLoader:
    """Load and standardize Excel input files for reconciliation"""
    
    def __init__(self, input_folder: str):
        self.input_folder = Path(input_folder)
        self.pivot_data = None
        self.accrual_data = None
        self.tracker_data = None
        self.ytd_billing_data = None
        
        if not self.input_folder.exists():
            raise FileNotFoundError(f"Input folder not found: {self.input_folder}")
    
    def load_pivot_table(self) -> pd.DataFrame:
        """Load UltraStaff Pivot Table (client billing data)"""
        logger.info("Loading Pivot Table...")
        
        file_path = self._find_file("Pivot")
        if not file_path:
            raise FileNotFoundError("Pivot table file not found")
        
        # Read the pivot table - skip first 2 rows (headers)
        df = pd.read_excel(str(file_path), sheet_name="SW Pivot Table", skiprows=2)
        
        # Standardize column names
        df = df.rename(columns={
            "APPL_ID": "applicant_number",
            "Applicant_Name": "consultant_name",
            "Bill_Hours": "hours_billed",
            "Bill_Rate": "bill_rate",
            "Bill_Amount": "amount_billed",
            "Assignment_PeriodEndingDate": "service_month",
            "T_INVOICE_COMPANY_NAME": "vendor_company",
            "CUST_NAME": "customer_name",
            "Invoice_Date": "invoice_date",
            "Invoice_Number": "invoice_number",
            "Period_EndDate": "period_end",
        })
        
        # Filter May data only (if needed)
        df['service_month'] = pd.to_datetime(df['service_month'], errors='coerce')
        
        logger.info(f"✅ Loaded {len(df):,} records from Pivot Table")
        self.pivot_data = df
        return df
    
    def load_accrual_workbook(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load all 3 sheets from Accrual workbook"""
        logger.info("Loading Accrual Workbook...")
        
        file_path = self._find_file("Accrual")
        if not file_path:
            logger.warning("⚠️ Accrual workbook not found")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
        # Sheet 1: Accrual Summary (44 consultants)
        accrual = self._load_accrual_sheet(str(file_path))
        
        # Sheet 2: Tracker (112 transactions)
        tracker = self._load_tracker_sheet(str(file_path))
        
        # Sheet 3: 2025 YTD Billing (2,639 records)
        ytd_billing = self._load_ytd_billing_sheet(str(file_path))
        
        logger.info(f"✅ Loaded Accrual data:")
        logger.info(f"   - Accrual: {len(accrual):,} consultants")
        logger.info(f"   - Tracker: {len(tracker):,} transactions")
        logger.info(f"   - YTD Billing: {len(ytd_billing):,} records")
        
        self.accrual_data = accrual
        self.tracker_data = tracker
        self.ytd_billing_data = ytd_billing
        
        return accrual, tracker, ytd_billing
    
    def _load_accrual_sheet(self, file_path: str) -> pd.DataFrame:
        """Load 'Accrual' sheet (44 consultant summaries)"""
        df = pd.read_excel(file_path, sheet_name='Accrual', skiprows=2)
        
        # Standardize columns
        df = df.rename(columns={
            "Employee": "consultant_name",
            "Ultra-Staff Applicant     Number": "applicant_number",
            "YTD Hours per Sub Tracker": "ytd_hours_invoiced",
            "YTD Profit Sharing Accrued Earnings GROSS": "ytd_amount_invoiced",
            "Actual Subvendor Payables": "ytd_amount_paid",
        })
        
        # Keep only numeric rows with applicant numbers
        df = df.dropna(subset=["applicant_number"])
        df['applicant_number'] = pd.to_numeric(df['applicant_number'], errors='coerce')
        df = df[df['applicant_number'] > 0]
        
        return df
    
    def _load_tracker_sheet(self, file_path: str) -> pd.DataFrame:
        """Load 'SW Subvendor Tracker 2025' sheet (112 transactions)"""
        df = pd.read_excel(file_path, sheet_name='SW Subvendor Tracker 2025', skiprows=2)
        
        # Standardize columns
        df = df.rename(columns={
            "Consultant": "consultant_name",
            "Applicant#": "applicant_number",
            "Client": "customer_name",
            "Vendor": "vendor_name",
            "Bill Rate": "bill_rate",
            "Pay Rate": "pay_rate",
            "Billed Hours": "hours_billed",
            "Vendor Hours": "hours_invoiced",
            "Vendor Invoice Amount": "amount_invoiced",
            "Month": "invoice_month",
            "Owed to vendor": "amount_owed",
        })
        
        # Clean data
        df = df.dropna(subset=["applicant_number"])
        df['applicant_number'] = pd.to_numeric(df['applicant_number'], errors='coerce')
        df = df[df['applicant_number'] > 0]
        
        return df
    
    def _load_ytd_billing_sheet(self, file_path: str) -> pd.DataFrame:
        """Load 'SW 2025 Billable to 05.31' sheet (2,639 YTD records)"""
        df = pd.read_excel(file_path, sheet_name='SW 2025 Billable to 05.31', skiprows=2)
        
        # This sheet has confusing column names - map them properly
        df = df.rename(columns={
            "Column8": "customer_bill_number",
            "T_INVOICE_COMPANY_NAME": "invoice_company",
            "Column1": "customer_id",
            "CUST_NAME": "customer_name",
            "Invoice_Number": "invoice_number",
            "Invoice_Date": "invoice_date",
            "Column2": "hours_billed",
            "Column3": "bill_rate",
            "Column4": "bill_amount",
            "Assignment_PeriodEndingDate": "service_month",
            "Period_EndDate": "period_end",
            "CUST_CITY": "customer_city",
            "CUST_STATE": "customer_state",
            "Column5": "applicant_number",
            "Applicant_Name": "consultant_name",
            "Pay_Rate": "pay_rate",
        })
        
        # Clean data
        df['service_month'] = pd.to_datetime(df['service_month'], errors='coerce')
        
        return df
    
    def _find_file(self, pattern: str) -> Optional[Path]:
        """Find Excel file matching pattern"""
        files = list(self.input_folder.glob(f"*{pattern}*.xlsx"))
        
        if not files:
            logger.warning(f"No file found matching: {pattern}")
            return None
        
        if len(files) > 1:
            logger.warning(f"Multiple files found, using: {files[0].name}")
        
        return files[0]
    
    def load_all_files(self) -> Dict:
        """Load all input files and return dictionary"""
        logger.info("="*80)
        logger.info("LOADING ALL MAY 2025 INPUT FILES")
        logger.info("="*80)
        
        pivot = self.load_pivot_table()
        accrual, tracker, ytd_billing = self.load_accrual_workbook()
        
        logger.info("="*80)
        logger.info("✅ ALL FILES LOADED SUCCESSFULLY")
        logger.info("="*80)
        
        return {
            "pivot": pivot,
            "accrual": accrual,
            "tracker": tracker,
            "ytd_billing": ytd_billing,
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loader = FileLoader("data/input/may_2025")
    data = loader.load_all_files()
    print(f"✅ Pivot: {len(data['pivot']):,} records")
    print(f"✅ Accrual: {len(data['accrual']):,} records")
    print(f"✅ Tracker: {len(data['tracker']):,} records")
    print(f"✅ YTD Billing: {len(data['ytd_billing']):,} records")
