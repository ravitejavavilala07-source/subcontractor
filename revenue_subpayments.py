"""
SmartWorks Sub-Contractor Revenue Reconciliation Automation
Version 2.2 - FIXED vendor_type classification

Author: Ravi Teja Vavilala
Date: 2025-11-08
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from fuzzywuzzy import fuzz
from loguru import logger
import sys
import json
from typing import Dict, List, Optional, Tuple

# ================================================================================
# CONFIGURATION
# ================================================================================

class Config:
    """Project configuration and file paths"""
    
    PROJECT_ROOT = Path(__file__).parent
    DATA_FOLDER = PROJECT_ROOT / "data"
    
    # Reference data paths (static, long-lived)
    REFERENCE_FOLDER = DATA_FOLDER / "reference"
    VENDOR_LISTS_FOLDER = REFERENCE_FOLDER / "vendor_lists"
    ESCALATION_FOLDER = REFERENCE_FOLDER / "escalation_lists"
    
    # Monthly data paths (time-series, updated monthly)
    MONTHLY_FOLDER = DATA_FOLDER / "monthly"
    OUTPUT_FOLDER = DATA_FOLDER / "output"
    
    # Reference files
    TCS_VENDOR_LIST = VENDOR_LISTS_FOLDER / "TCS Sub vendor list.xlsx"
    COGNIZANT_VENDOR_LIST = VENDOR_LISTS_FOLDER / "Cognizant Sub vendor list.xlsx"
    SMARTWORKS_VENDOR_LIST = VENDOR_LISTS_FOLDER / "Smartworks Sub vendor list-2.xlsx"
    COX_VENDOR_LIST = VENDOR_LISTS_FOLDER / "Cox Sub vendor list-2.xlsx"
    RISE_IT_VENDOR_LIST = VENDOR_LISTS_FOLDER / "Rise IT Sub Vendor List.xlsx"
    XELA_VENDOR_LIST = VENDOR_LISTS_FOLDER / "Xela Sub vendor list.xlsx"
    ITECH_VENDOR_LIST = VENDOR_LISTS_FOLDER / "iTech Other Sub vendor list-iTech.xlsx"
    
    TCS_ESCALATION_LIST = ESCALATION_FOLDER / "TCS Subs-escalation list.xlsx"
    COGNIZANT_ESCALATION_LIST = ESCALATION_FOLDER / "Cognizant Sub s-escalation list.xlsx"
    
    # Configuration
    FUZZY_MATCH_THRESHOLD = 85
    SHORT_PAY_THRESHOLD = 1.0
    
    @staticmethod
    def get_monthly_path(year_month: str, data_type: str = "pivot_table"):
        """Get path for monthly data"""
        return Config.MONTHLY_FOLDER / year_month / data_type
    
    @staticmethod
    def get_output_path(year_month: str):
        """Get path for monthly output reports"""
        return Config.OUTPUT_FOLDER / year_month
    
    @staticmethod
    def ensure_folders_exist():
        """Create all necessary folders if they don't exist"""
        Config.REFERENCE_FOLDER.mkdir(parents=True, exist_ok=True)
        Config.VENDOR_LISTS_FOLDER.mkdir(parents=True, exist_ok=True)
        Config.ESCALATION_FOLDER.mkdir(parents=True, exist_ok=True)
        Config.MONTHLY_FOLDER.mkdir(parents=True, exist_ok=True)
        Config.OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)


# ================================================================================
# LOGGER SETUP
# ================================================================================

def setup_logger(month: str = "2025-11"):
    """Setup logging for the run"""
    log_file = Path(f"reconciliation_{month}.log")
    logger.remove()
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level="INFO"
    )
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level="INFO"
    )
    return log_file


# ================================================================================
# UTILITY FUNCTION - CLASSIFY VENDOR TYPE FROM CUSTOMER NAME
# ================================================================================

def classify_vendor_type_from_customer(customer_name: str) -> str:
    """
    Classify vendor type (TCS, Cognizant, etc) from customer name
    
    Examples:
    - "USAA/TCS/eTeam" -> "TCS"
    - "Humana/TCS" -> "TCS"
    - "Cognizant/ Epsilon" -> "Cognizant"
    - "Cognizant Technology Solutions/Casenet/Centene" -> "Cognizant"
    """
    if pd.isna(customer_name):
        return "Unknown"
    
    customer_str = str(customer_name).lower()
    
    if "cognizant" in customer_str:
        return "Cognizant"
    elif "tcs" in customer_str:
        return "TCS"
    elif "cox" in customer_str:
        return "Cox"
    elif "xela" in customer_str:
        return "Xela"
    elif "rise" in customer_str or "rise it" in customer_str:
        return "Rise IT"
    elif "itech" in customer_str or "i-tech" in customer_str:
        return "iTech"
    else:
        return "Other"


# ================================================================================
# REFERENCE DATA MANAGER (Fixed)
# ================================================================================

class ReferenceDataManager:
    """Manages static reference data (vendor lists, escalation lists)"""
    
    def __init__(self, config: Config):
        self.config = config
        self.vendor_maps = {}
        self.vendor_types = {}
        self.escalation_list = {}
        self.all_vendor_data = None
        self.fuzzy_matcher = FuzzyNameMatcher()
        
        logger.info("="*80)
        logger.info("LOADING REFERENCE DATA (Static)")
        logger.info("="*80)
        self._load_all_reference_data()
    
    def _load_all_reference_data(self):
        """Load all reference vendor and escalation lists"""
        
        vendor_files = {
            'TCS': self.config.TCS_VENDOR_LIST,
            'Cognizant': self.config.COGNIZANT_VENDOR_LIST,
            'Smartworks': self.config.SMARTWORKS_VENDOR_LIST,
            'Cox': self.config.COX_VENDOR_LIST,
            'Rise IT': self.config.RISE_IT_VENDOR_LIST,
            'Xela': self.config.XELA_VENDOR_LIST,
            'iTech': self.config.ITECH_VENDOR_LIST,
        }
        
        combined_vendors = []
        
        for vendor_type, file_path in vendor_files.items():
            if not file_path.exists():
                logger.warning(f"⚠️  Vendor list not found: {file_path}")
                continue
            
            try:
                df = pd.read_excel(file_path, sheet_name=0)
                
                # Handle different file formats
                if vendor_type == "Xela":
                    df = df.iloc[1:].reset_index(drop=True)
                    df.columns = ['consultant_name', 'vendor_name', 'pay_rate', 'payment_terms', 'unused1', 'unused2', 'unused3']
                    df = df[['consultant_name', 'vendor_name', 'pay_rate', 'payment_terms']]
                    df['customer'] = 'Xela'
                    
                elif vendor_type == "Cognizant":
                    df = df.iloc[1:].reset_index(drop=True)
                    df.columns = ['consultant_name', 'vendor_name', 'customer', 'payment_terms', 'pay_rate']
                    
                else:
                    df.columns = ['consultant_name', 'vendor_name', 'customer', 'payment_terms', 'pay_rate']
                
                # Remove rows with NaN consultant names
                df = df.dropna(subset=['consultant_name'])
                df = df[df['consultant_name'].astype(str).str.strip() != '']
                
                df['vendor_type'] = vendor_type
                
                # Build lookup maps
                for _, row in df.iterrows():
                    consultant = str(row['consultant_name']).strip().lower()
                    if pd.notna(row['vendor_name']):
                        self.vendor_maps[consultant] = {
                            'vendor_name': row['vendor_name'],
                            'customer': row.get('customer', vendor_type),
                            'vendor_type': vendor_type,
                            'pay_rate': row.get('pay_rate', 0)
                        }
                        self.vendor_types[consultant] = vendor_type
                
                combined_vendors.append(df)
                logger.info(f"✓ Loaded {len(df)} {vendor_type} vendors")
                
            except Exception as e:
                logger.error(f"❌ Error loading {vendor_type} vendor list: {str(e)}")
                continue
        
        self.all_vendor_data = pd.concat(combined_vendors, ignore_index=True) if combined_vendors else pd.DataFrame()
        logger.info(f"✓ Total reference vendors loaded: {len(self.vendor_maps)}")
        
        # Load escalation lists
        self._load_escalation_lists()
    
    def _load_escalation_lists(self):
        """Load escalation lists"""
        escalation_files = {
            'TCS': self.config.TCS_ESCALATION_LIST,
            'Cognizant': self.config.COGNIZANT_ESCALATION_LIST,
        }
        
        total_escalations = 0
        for vendor_type, file_path in escalation_files.items():
            if not file_path.exists():
                logger.warning(f"⚠️  Escalation list not found: {file_path}")
                continue
            
            try:
                df = pd.read_excel(file_path, sheet_name=0)
                escalation_col = df.iloc[:, 0]
                for consultant in escalation_col:
                    if pd.notna(consultant):
                        consultant_key = str(consultant).strip().lower()
                        self.escalation_list[consultant_key] = {
                            'vendor_type': vendor_type,
                            'flagged': True
                        }
                        total_escalations += 1
                
                logger.info(f"✓ Loaded {len(escalation_col)} escalation entries from {vendor_type}")
                
            except Exception as e:
                logger.error(f"❌ Error loading {vendor_type} escalation list: {str(e)}")
        
        logger.info(f"✓ Total escalations flagged: {total_escalations}")
    
    def is_escalated(self, consultant_name: str) -> bool:
        """Check if consultant is escalated"""
        consultant_key = str(consultant_name).strip().lower()
        return consultant_key in self.escalation_list


# ================================================================================
# FUZZY NAME MATCHER
# ================================================================================

class FuzzyNameMatcher:
    """Fuzzy matching for consultant names"""
    
    @staticmethod
    def find_best_match(query: str, candidates: List[str], threshold: int = 85) -> Optional[Tuple[str, int]]:
        """Find best fuzzy match from candidates"""
        best_score = 0
        best_match = None
        
        for candidate in candidates:
            score = fuzz.token_sort_ratio(query, candidate)
            if score > best_score:
                best_score = score
                best_match = candidate
        
        if best_score >= threshold:
            return (best_match, best_score)
        return None


# ================================================================================
# MONTHLY FILE IMPORTER
# ================================================================================

class MonthlyFileImporter:
    """Import monthly data files"""
    
    def __init__(self, config: Config, year_month: str):
        self.config = config
        self.year_month = year_month
        self.pivot_data = None
        self.qb_payments = None
        self.accrual_data = None
    
    def import_pivot_table(self) -> Optional[pd.DataFrame]:
        """Import Pivot Table for the month"""
        logger.info(f"\n[Importing] Pivot Table for {self.year_month}...")
        
        pivot_folder = self.config.get_monthly_path(self.year_month, "pivot_table")
        pivot_files = list(pivot_folder.glob("*.xlsx"))
        
        if not pivot_files:
            logger.warning(f"⚠️  No pivot table files found in {pivot_folder}")
            return None
        
        pivot_file = pivot_files[0]
        
        try:
            df = pd.read_excel(pivot_file, sheet_name="SW Pivot Table", skiprows=2)
            logger.info(f"✓ Loaded {len(df):,} total records")
            self.pivot_data = df
            return df
        except Exception as e:
            logger.error(f"❌ Error importing pivot table: {str(e)}")
            return None
    
    def import_qb_payments(self) -> Optional[pd.DataFrame]:
        """Import QB Payments for the month"""
        logger.info(f"\n[Importing] QB Payments for {self.year_month}...")
        
        qb_folder = self.config.get_monthly_path(self.year_month, "qb_payments")
        qb_files = list(qb_folder.glob("*.xlsx"))
        
        if not qb_files:
            logger.warning(f"⚠️  No QB payment files found in {qb_folder}")
            return None
        
        qb_file = qb_files[0]
        
        try:
            df = pd.read_excel(qb_file)
            logger.info(f"✓ Loaded {len(df):,} payment records")
            self.qb_payments = df
            return df
        except Exception as e:
            logger.error(f"❌ Error importing QB payments: {str(e)}")
            return None
    
    def import_accrual_data(self) -> Optional[pd.DataFrame]:
        """Import accrual data for the month"""
        logger.info(f"\n[Importing] Accrual Data for {self.year_month}...")
        
        accrual_folder = self.config.get_monthly_path(self.year_month, "accrual_data")
        accrual_files = list(accrual_folder.glob("*.xlsx"))
        
        if not accrual_files:
            logger.warning(f"⚠️  No accrual files found in {accrual_folder}")
            return None
        
        accrual_file = accrual_files[0]
        
        try:
            df = pd.read_excel(accrual_file)
            logger.info(f"✓ Loaded {len(df):,} accrual records")
            self.accrual_data = df
            return df
        except Exception as e:
            logger.error(f"❌ Error importing accrual data: {str(e)}")
            return None


# ================================================================================
# DATA CLEANER & ENRICHER - FIXED
# ================================================================================

class DataCleaner:
    """Clean and enrich monthly data using reference data"""
    
    def __init__(self, reference_data: ReferenceDataManager):
        self.reference_data = reference_data
    
    def clean_pivot_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and enrich pivot table data - FIXED vendor classification"""
        logger.info("\n[Cleaning] Pivot Table...")
        
        required_cols = {
            'APPL_ID': 'applicant_number',
            'Applicant_Name': 'consultant_name',
            'Bill_Hours': 'hours_billed',
            'Bill_Rate': 'hourly_rate',
            'Bill_Amount': 'amount_billed',
            'Assignment_PeriodEndingDate': 'service_month',
            'T_INVOICE_COMPANY_NAME': 'vendor_name',
            'CUST_NAME': 'customer'
        }
        
        available_cols = [col for col in required_cols.keys() if col in df.columns]
        cleaned = df[available_cols].rename(columns={k: v for k, v in required_cols.items() if k in available_cols})
        
        # Filter TCS/Cognizant records
        cleaned = cleaned[cleaned['customer'].astype(str).str.contains('TCS|Cognizant', case=False, na=False)]
        
        # Remove nulls
        cleaned = cleaned.dropna(subset=['consultant_name', 'amount_billed'])
        
        # Remove zero amounts
        cleaned = cleaned[cleaned['amount_billed'] > 0]
        
        # FIX: Classify vendor type from CUSTOMER field, not consultant name
        cleaned['vendor_type'] = cleaned['customer'].apply(classify_vendor_type_from_customer)
        
        # Check if consultant is escalated
        cleaned['is_escalation'] = cleaned['consultant_name'].apply(
            lambda x: self.reference_data.is_escalated(x)
        )
        
        logger.info(f"✓ Cleaned {len(cleaned):,} records")
        logger.info(f"  - TCS records: {len(cleaned[cleaned['vendor_type'] == 'TCS']):,}")
        logger.info(f"  - Cognizant records: {len(cleaned[cleaned['vendor_type'] == 'Cognizant']):,}")
        logger.info(f"  - Other records: {len(cleaned[cleaned['vendor_type'] == 'Other']):,}")
        logger.info(f"  - Escalations flagged: {len(cleaned[cleaned['is_escalation'] == True]):,}")
        
        return cleaned
    
    def clean_qb_payments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean QB payment data"""
        logger.info("\n[Cleaning] QB Payments...")
        
        if len(df) == 0:
            return df
        
        required_cols = ['Date', 'Name', 'Debit', 'Credit', 'Memo']
        available_cols = [col for col in required_cols if col in df.columns]
        cleaned = df[available_cols].copy()
        
        if 'Debit' in cleaned.columns and 'Credit' in cleaned.columns:
            cleaned['amount_paid'] = cleaned['Debit'].fillna(0) - cleaned['Credit'].fillna(0)
        elif 'Debit' in cleaned.columns:
            cleaned['amount_paid'] = cleaned['Debit']
        else:
            cleaned['amount_paid'] = 0
        
        cleaned = cleaned[cleaned['amount_paid'] != 0]
        
        logger.info(f"✓ Cleaned {len(cleaned):,} payment records")
        return cleaned


# ================================================================================
# RECONCILIATION ENGINE
# ================================================================================

class ReconciliationEngine:
    """Reconcile invoices to payments"""
    
    @staticmethod
    def reconcile_invoices_to_payments(pivot_df: pd.DataFrame, qb_df: pd.DataFrame) -> pd.DataFrame:
        """Match invoices to payments and calculate variances"""
        logger.info("\n[Reconciliation] Matching invoices to payments...")
        
        reconciled = pivot_df.copy()
        
        if qb_df is None or len(qb_df) == 0:
            logger.warning("⚠️  No QB payment data provided - marking all as missing payments")
            reconciled['amount_paid'] = 0
            reconciled['match_type'] = 'NO_QB_DATA'
        else:
            reconciled['amount_paid'] = 0
            reconciled['match_type'] = 'PENDING_QB_MATCH'
        
        reconciled['variance'] = reconciled['amount_billed'] - reconciled['amount_paid']
        reconciled['variance_pct'] = np.where(
            reconciled['amount_billed'] != 0,
            (reconciled['variance'] / reconciled['amount_billed'] * 100).round(2),
            0
        )
        
        def classify_variance(row):
            if row['amount_paid'] == 0:
                return 'MISSING_PAYMENT'
            elif abs(row['variance']) < 1.0:
                return 'MATCH'
            elif row['variance'] > 0:
                return 'SHORT_PAY'
            else:
                return 'OVERPAY'
        
        reconciled['variance_type'] = reconciled.apply(classify_variance, axis=1)
        
        total_billed = reconciled['amount_billed'].sum()
        total_paid = reconciled['amount_paid'].sum()
        total_variance = total_billed - total_paid
        
        logger.info(f"✓ Reconciliation complete:")
        logger.info(f"  - Total Billed: ${total_billed:,.2f}")
        logger.info(f"  - Total Paid: ${total_paid:,.2f}")
        logger.info(f"  - Total Variance: ${total_variance:,.2f}")
        logger.info(f"  - Missing Payments: {len(reconciled[reconciled['variance_type'] == 'MISSING_PAYMENT']):,}")
        logger.info(f"  - Short-Pays: {len(reconciled[reconciled['variance_type'] == 'SHORT_PAY']):,}")
        
        return reconciled


# ================================================================================
# REPORT GENERATOR
# ================================================================================

class ReportGenerator:
    """Generate Excel reconciliation reports"""
    
    def __init__(self, config: Config):
        self.config = config
    
    def generate_report(self, reconciled_df: pd.DataFrame, year_month: str) -> Path:
        """Generate comprehensive Excel report"""
        logger.info("\n[Reporting] Generating Excel report...")
        
        output_folder = self.config.get_output_path(year_month)
        output_folder.mkdir(parents=True, exist_ok=True)
        
        filename = f"Revenue_vs_SubPayments_Reconciliation_{year_month}.xlsx"
        filepath = output_folder / filename
        
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            self._write_summary_sheet(reconciled_df, writer)
            self._write_tcs_sheets(reconciled_df, writer)
            self._write_cognizant_sheets(reconciled_df, writer)
            self._write_escalations_sheet(reconciled_df, writer)
            self._write_variance_analysis_sheet(reconciled_df, writer)
        
        logger.info(f"✓ Report generated: {filepath}")
        return filepath
    
    @staticmethod
    def _write_summary_sheet(df, writer):
        """Write Summary sheet"""
        logger.info("  Writing Summary sheet...")
        
        tcs_df = df[df['vendor_type'] == 'TCS']
        cog_df = df[df['vendor_type'] == 'Cognizant']
        
        summary_data = {
            'Metric': [
                'Total Billed (TCS)', 'Total Paid (TCS)', 'Variance (TCS)', 'TCS Records', '',
                'Total Billed (Cognizant)', 'Total Paid (Cognizant)', 'Variance (Cognizant)', 'Cognizant Records', '',
                'GRAND TOTAL BILLED', 'GRAND TOTAL PAID', 'GRAND TOTAL VARIANCE', '',
                'Total Records', 'Matched Records', 'Missing Payments', 'Short-Pays', 'Overpays', 'Escalation Cases'
            ],
            'Value': [
                f"${tcs_df['amount_billed'].sum():,.2f}",
                f"${tcs_df['amount_paid'].sum():,.2f}",
                f"${(tcs_df['amount_billed'].sum() - tcs_df['amount_paid'].sum()):,.2f}",
                f"{len(tcs_df):,}", '',
                f"${cog_df['amount_billed'].sum():,.2f}",
                f"${cog_df['amount_paid'].sum():,.2f}",
                f"${(cog_df['amount_billed'].sum() - cog_df['amount_paid'].sum()):,.2f}",
                f"{len(cog_df):,}", '',
                f"${df['amount_billed'].sum():,.2f}",
                f"${df['amount_paid'].sum():,.2f}",
                f"${(df['amount_billed'].sum() - df['amount_paid'].sum()):,.2f}", '',
                f"{len(df):,}",
                f"{len(df[df['variance_type'] == 'MATCH']):,}",
                f"{len(df[df['variance_type'] == 'MISSING_PAYMENT']):,}",
                f"{len(df[df['variance_type'] == 'SHORT_PAY']):,}",
                f"{len(df[df['variance_type'] == 'OVERPAY']):,}",
                f"{len(df[df['is_escalation'] == True]):,}"
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    @staticmethod
    def _write_tcs_sheets(df, writer):
        """Write TCS sheets"""
        tcs_df = df[df['vendor_type'] == 'TCS']
        
        if len(tcs_df) > 0:
            revenue_cols = ['applicant_number', 'consultant_name', 'vendor_name', 'hours_billed', 'hourly_rate', 'amount_billed']
            tcs_df[revenue_cols].to_excel(writer, sheet_name='TCS Revenue', index=False)
            
            reconcile_cols = ['applicant_number', 'consultant_name', 'vendor_name', 'amount_billed', 'amount_paid', 'variance', 'variance_type']
            tcs_df[reconcile_cols].to_excel(writer, sheet_name='TCS Reconciliation', index=False)
    
    @staticmethod
    def _write_cognizant_sheets(df, writer):
        """Write Cognizant sheets"""
        cog_df = df[df['vendor_type'] == 'Cognizant']
        
        if len(cog_df) > 0:
            revenue_cols = ['applicant_number', 'consultant_name', 'vendor_name', 'hours_billed', 'hourly_rate', 'amount_billed']
            cog_df[revenue_cols].to_excel(writer, sheet_name='Cognizant Revenue', index=False)
            
            reconcile_cols = ['applicant_number', 'consultant_name', 'vendor_name', 'amount_billed', 'amount_paid', 'variance', 'variance_type']
            cog_df[reconcile_cols].to_excel(writer, sheet_name='Cognizant Reconciliation', index=False)
    
    @staticmethod
    def _write_escalations_sheet(df, writer):
        """Write Escalations sheet"""
        escalations = df[df['is_escalation'] == True]
        
        if len(escalations) > 0:
            escalation_cols = ['applicant_number', 'consultant_name', 'vendor_name', 'customer', 'amount_billed', 'variance', 'vendor_type']
            escalations[escalation_cols].to_excel(writer, sheet_name='Escalations', index=False)
        else:
            empty_df = pd.DataFrame(columns=['applicant_number', 'consultant_name', 'vendor_name', 'customer', 'amount_billed', 'variance', 'vendor_type'])
            empty_df.to_excel(writer, sheet_name='Escalations', index=False)
    
    @staticmethod
    def _write_variance_analysis_sheet(df, writer):
        """Write Variance Analysis sheet"""
        variance_analysis = df.groupby('variance_type').agg({
            'applicant_number': 'count',
            'amount_billed': 'sum',
            'amount_paid': 'sum',
            'variance': 'sum'
        }).rename(columns={'applicant_number': 'record_count'})
        
        variance_analysis.to_excel(writer, sheet_name='Variance Analysis')


# ================================================================================
# MAIN ORCHESTRATOR
# ================================================================================

class SmartWorksReconciliation:
    """Main reconciliation orchestrator"""
    
    def __init__(self, year_month: str = "2025-11"):
        self.year_month = year_month
        self.config = Config()
        self.config.ensure_folders_exist()
        
        logger.info(f"Initializing reconciliation for {year_month}")
        
        self.reference_data = ReferenceDataManager(self.config)
        self.importer = MonthlyFileImporter(self.config, year_month)
        self.cleaner = DataCleaner(self.reference_data)
        self.reconciler = ReconciliationEngine()
        self.report_gen = ReportGenerator(self.config)
    
    def run(self) -> Optional[Path]:
        """Execute full reconciliation pipeline"""
        logger.info("\n" + "="*80)
        logger.info(f"SMARTWORKS REVENUE RECONCILIATION - {self.year_month}")
        logger.info("="*80)
        
        try:
            logger.info("\n[STEP 1/5] IMPORTING MONTHLY FILES")
            logger.info("-"*80)
            
            pivot_df = self.importer.import_pivot_table()
            if pivot_df is None:
                logger.error("❌ Failed to import pivot table")
                return None
            
            qb_df = self.importer.import_qb_payments()
            accrual_df = self.importer.import_accrual_data()
            
            logger.info("\n[STEP 2/5] CLEANING & ENRICHING DATA")
            logger.info("-"*80)
            
            pivot_clean = self.cleaner.clean_pivot_data(pivot_df)
            qb_clean = self.cleaner.clean_qb_payments(qb_df) if qb_df is not None else pd.DataFrame()
            
            logger.info("\n[STEP 3/5] RECONCILING INVOICES")
            logger.info("-"*80)
            
            reconciled_df = self.reconciler.reconcile_invoices_to_payments(pivot_clean, qb_clean if len(qb_clean) > 0 else None)
            
            logger.info("\n[STEP 4/5] GENERATING REPORTS")
            logger.info("-"*80)
            
            report_path = self.report_gen.generate_report(reconciled_df, self.year_month)
            
            logger.info("\n[STEP 5/5] RECONCILIATION SUMMARY")
            logger.info("-"*80)
            
            tcs_total = reconciled_df[reconciled_df['vendor_type'] == 'TCS']['amount_billed'].sum()
            cog_total = reconciled_df[reconciled_df['vendor_type'] == 'Cognizant']['amount_billed'].sum()
            
            logger.info(f"✓ TCS Total Billed: ${tcs_total:,.2f}")
            logger.info(f"✓ Cognizant Total Billed: ${cog_total:,.2f}")
            logger.info(f"✓ Grand Total Billed: ${reconciled_df['amount_billed'].sum():,.2f}")
            logger.info(f"✓ Total Records Processed: {len(reconciled_df):,}")
            logger.info(f"✓ Escalation Cases: {len(reconciled_df[reconciled_df['is_escalation'] == True]):,}")
            
            logger.info("\n" + "="*80)
            logger.info("✅ RECONCILIATION COMPLETE")
            logger.info("="*80)
            logger.info(f"Report: {report_path}\n")
            
            return report_path
            
        except Exception as e:
            logger.error(f"❌ Reconciliation failed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None


# ================================================================================
# MAIN
# ================================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='SmartWorks Sub-Contractor Revenue Reconciliation')
    parser.add_argument('--month', type=str, default='2025-11', help='Month to reconcile (format: YYYY-MM)')
    args = parser.parse_args()
    
    setup_logger(args.month)
    
    reconciliation = SmartWorksReconciliation(args.month)
    report_path = reconciliation.run()
    
    if report_path:
        print(f"\n✅ SUCCESS: Report generated at {report_path}")
        sys.exit(0)
    else:
        print(f"\n❌ FAILED: Could not generate report")
        sys.exit(1)
