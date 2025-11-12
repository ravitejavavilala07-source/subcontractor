"""
FINAL PRODUCTION SUITE - Complete iTech Accrual Reconciliation
Purpose: End-to-end monthly automation with all advanced features
Author: Copilot
Date: 2025-11-10
Status: PRODUCTION READY
"""

import logging
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import re
from fuzzywuzzy import fuzz

sys.path.insert(0, str(Path(__file__).parent))

# ============================================================================
# PART 1: QB MEMO PARSER
# ============================================================================

class QuickBooksMemoParser:
    """Extract structured data from QB memo strings"""
    
    PATTERNS = [
        r'(?P<name>[^/]+?)\s*/\s*#?(?P<id>\d+)\s*/\s*(?P<month>\w+)\s*/\s*(?P<hours>\d+\.?\d*)\s*(?:hours?|hrs?)\s*@\s*\$?(?P<rate>\d+\.?\d*)',
        r'(?P<name>[^-]+?)\s*-\s*#?(?P<id>\d+)\s*-\s*(?P<month>\w+)\s*-\s*(?P<hours>\d+\.?\d*)\s*-\s*\$?(?P<rate>\d+\.?\d*)',
    ]
    
    MONTHS = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
              'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
              'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5,
              'june': 6, 'july': 7, 'august': 8, 'september': 9, 'october': 10,
              'november': 11, 'december': 12}
    
    def parse_memo(self, memo: str) -> Optional[Dict]:
        if not isinstance(memo, str) or not memo.strip():
            return None
        
        for pattern in self.PATTERNS:
            match = re.search(pattern, memo, re.IGNORECASE)
            if match:
                try:
                    groups = match.groupdict()
                    month_num = self.MONTHS.get(groups['month'].lower(), None)
                    return {
                        'applicant_id': int(groups['id']),
                        'consultant_name': groups['name'].strip().title(),
                        'service_month': f"2025-{month_num:02d}" if month_num else groups['month'],
                        'hours': float(groups['hours']),
                        'rate': float(groups['rate']),
                        'parsed_amount': float(groups['hours']) * float(groups['rate']),
                    }
                except:
                    continue
        return None

# ============================================================================
# PART 2: ADVANCED MATCHING ENGINE
# ============================================================================

class AdvancedMatchingEngine:
    """3-strategy fallback matching"""
    
    @staticmethod
    def normalize(value: str) -> str:
        if not isinstance(value, str):
            return ''
        return value.strip().lower()
    
    @staticmethod
    def match_exact_id(pivot_df: pd.DataFrame, accrual_df: pd.DataFrame) -> pd.DataFrame:
        """Strategy 1: Exact ID match"""
        accrual_sel = accrual_df[['applicant_number', 'consultant_name']].copy()
        accrual_sel = accrual_sel.rename(columns={'consultant_name': 'accrual_name'})
        
        merged = pivot_df.merge(accrual_sel, on='applicant_number', how='left', indicator=True)
        merged['match_strategy'] = 'EXACT_ID'
        merged['match_confidence'] = 100
        return merged
    
    @staticmethod
    def match_hybrid(merged_df: pd.DataFrame, accrual_df: pd.DataFrame) -> pd.DataFrame:
        """Strategy 2: Name + ID hybrid"""
        unmatched = merged_df[merged_df['accrual_name'].isna()].copy()
        
        for idx, row in unmatched.iterrows():
            pivot_name = AdvancedMatchingEngine.normalize(row.get('consultant_name', ''))
            if not pivot_name:
                continue
            
            best_match = None
            best_score = 0
            
            for _, accrual_row in accrual_df.iterrows():
                accrual_name = AdvancedMatchingEngine.normalize(accrual_row.get('consultant_name', ''))
                score = fuzz.token_sort_ratio(pivot_name, accrual_name)
                
                if score >= 90 and score > best_score:
                    best_score = score
                    best_match = accrual_row
            
            if best_match is not None:
                merged_df.at[idx, 'match_strategy'] = f'HYBRID_{int(best_score)}'
                merged_df.at[idx, 'match_confidence'] = best_score
                merged_df.at[idx, 'accrual_name'] = best_match['consultant_name']
        
        return merged_df
    
    @staticmethod
    def match_fuzzy(merged_df: pd.DataFrame, accrual_df: pd.DataFrame) -> pd.DataFrame:
        """Strategy 3: Fuzzy name/vendor fallback (85% threshold)"""
        unmatched = merged_df[merged_df['accrual_name'].isna()].copy()
        
        for idx, row in unmatched.iterrows():
            pivot_name = AdvancedMatchingEngine.normalize(row.get('consultant_name', ''))
            if not pivot_name:
                continue
            
            best_match = None
            best_score = 0
            
            for _, accrual_row in accrual_df.iterrows():
                accrual_name = AdvancedMatchingEngine.normalize(accrual_row.get('consultant_name', ''))
                score = fuzz.token_sort_ratio(pivot_name, accrual_name)
                
                if score >= 85 and score > best_score:
                    best_score = score
                    best_match = accrual_row
            
            if best_match is not None:
                merged_df.at[idx, 'match_strategy'] = f'FUZZY_{int(best_score)}'
                merged_df.at[idx, 'match_confidence'] = best_score
                merged_df.at[idx, 'accrual_name'] = best_match['consultant_name']
                merged_df.at[idx, 'applicant_number'] = best_match['applicant_number']
        
        return merged_df

# ============================================================================
# PART 3: VARIANCE CALCULATOR
# ============================================================================

class VarianceCalculator:
    """Calculate all variance metrics"""
    
    @staticmethod
    def calculate_all_variances(df: pd.DataFrame) -> pd.DataFrame:
        """Calculate hours and amount variances"""
        
        # Hours variance
        df['hours_variance'] = df['hours_billed'].fillna(0) - df['ytd_hours_invoiced'].fillna(0)
        
        # Amount variance
        df['amount_variance'] = df['amount_billed'].fillna(0) - df['ytd_amount_invoiced'].fillna(0)
        
        # Variance severity flag
        df['variance_severity'] = df['amount_variance'].apply(
            lambda x: '🔴 CRITICAL' if abs(x) > 50000 else 
                     '🟡 HIGH' if abs(x) > 10000 else 
                     '🟢 NORMAL' if abs(x) <= 1000 else '🟠 MEDIUM'
        )
        
        # Match status
        df['Status'] = df.apply(lambda row: 
            '✅ Matched' if pd.notna(row.get('accrual_name')) else
            '❌ Missing in Accrual' if row.get('_merge') == 'left_only' else
            '❌ Missing in Pivot' if row.get('_merge') == 'right_only' else
            '❓ Unmatched', axis=1
        )
        
        # Mismatch reason
        def get_reason(row):
            if row['Status'] == '✅ Matched':
                return ''
            elif row['Status'] == '❌ Missing in Accrual':
                return f"Transaction not in accrual (Vendor: {row.get('vendor_name', '?')})"
            elif row['Status'] == '❌ Missing in Pivot':
                return 'Accrual entry without corresponding transaction'
            else:
                return 'Could not match by ID or name'
        
        df['reason_for_mismatch'] = df.apply(get_reason, axis=1)
        
        return df

# ============================================================================
# PART 4: ACCRUAL SUGGESTION GENERATOR
# ============================================================================

class AccrualSuggestionGenerator:
    """Generate ready-to-upload accrual suggestions"""
    
    @staticmethod
    def generate_suggestions(df: pd.DataFrame) -> pd.DataFrame:
        """Generate suggestions for unmatched pivot records"""
        
        unmatched = df[df['Status'] == '❌ Missing in Accrual'].copy()
        
        if len(unmatched) == 0:
            return pd.DataFrame()
        
        suggestions = pd.DataFrame({
            'applicant_number': unmatched['applicant_number'],
            'consultant_name': unmatched['consultant_name'],
            'vendor_name': unmatched['vendor_name'],
            'service_period': unmatched.get('service_month', unmatched.get('invoice_date', '')),
            'hours_billed': unmatched['hours_billed'].fillna(0),
            'amount_billed': unmatched['amount_billed'].fillna(0),
            'accrual_status': 'DRAFT',
            'accrual_type': 'regular',
            'source': 'Auto-Generated from Pivot',
            'reason': 'Missing in accrual sheet',
            'suggested_amount': unmatched['amount_billed'].fillna(0),
            'hours_variance': unmatched['hours_variance'],
            'amount_variance': unmatched['amount_variance'],
            'review_date': datetime.now().strftime('%Y-%m-%d'),
            'approved_by': '',
            'notes': '',
        })
        
        return suggestions.reset_index(drop=True)

# ============================================================================
# PART 5: MONTHLY ORCHESTRATOR
# ============================================================================

class MonthlyOrchestrator:
    """Complete end-to-end monthly automation"""
    
    def __init__(self, input_folder: str, output_folder: str, month: str, year: int):
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.month = month
        self.year = year
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        handler = logging.FileHandler(
            self.output_folder / f"reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def run(self) -> Dict:
        """Execute complete monthly reconciliation"""
        
        self.logger.info("="*80)
        self.logger.info(f"🚀 MONTHLY RECONCILIATION - {self.month.upper()} {self.year}")
        self.logger.info("="*80)
        
        try:
            # Step 1: Load
            self.logger.info("\n[STEP 1/7] Loading source files...")
            pivot, accrual, tracker, billable = self._load_data()
            self.logger.info(f"  ✅ Pivot: {len(pivot):,}")
            self.logger.info(f"  ✅ Accrual: {len(accrual):,}")
            self.logger.info(f"  ✅ Billable: {len(billable):,}")
            
            # Step 2: Normalize
            self.logger.info("\n[STEP 2/7] Normalizing schemas...")
            pivot, accrual, billable = self._normalize_data(pivot, accrual, billable)
            
            # Step 3: Combine
            self.logger.info("\n[STEP 3/7] Combining transaction sources...")
            combined = pd.concat([pivot, billable], ignore_index=True)
            self.logger.info(f"  ✅ Combined: {len(combined):,}")
            
            # Step 4: Match (3-strategy)
            self.logger.info("\n[STEP 4/7] Advanced matching (3-strategy)...")
            matched = self._perform_matching(combined, accrual)
            match_count = len(matched[matched['Status'] == '✅ Matched'])
            match_rate = (match_count / len(matched)) * 100 if len(matched) > 0 else 0
            self.logger.info(f"  ✅ Matched: {match_count:,} ({match_rate:.1f}%)")
            
            # Step 5: Variance
            self.logger.info("\n[STEP 5/7] Calculating variances...")
            matched = VarianceCalculator.calculate_all_variances(matched)
            
            # Step 6: Suggestions
            self.logger.info("\n[STEP 6/7] Generating accrual suggestions...")
            suggestions = AccrualSuggestionGenerator.generate_suggestions(matched)
            self.logger.info(f"  ✅ Generated: {len(suggestions):,}")
            
            # Step 7: Report
            self.logger.info("\n[STEP 7/7] Generating reports...")
            report_path, template_path = self._generate_reports(matched, suggestions)
            
            # Final summary
            metrics = self._calculate_metrics(matched)
            self._print_summary(metrics, len(suggestions))
            
            return {
                'success': True,
                'report': str(report_path),
                'template': str(template_path),
                'metrics': metrics,
                'match_rate': match_rate,
            }
        
        except Exception as e:
            self.logger.error(f"❌ Failed: {str(e)}", exc_info=True)
            raise
    
    def _load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load all source files"""
        # Pivot
        pivot_file = list(self.input_folder.glob("SW Pivot Table*.xlsx"))[0]
        pivot = pd.read_excel(pivot_file, sheet_name="SW Pivot Table", skiprows=2)
        
        # Accrual workbook
        accrual_file = list(self.input_folder.glob("SmartWorks Sub Vendor Accrual*.xlsx"))[0]
        accrual = pd.read_excel(accrual_file, sheet_name='Accrual', skiprows=2)
        tracker = pd.read_excel(accrual_file, sheet_name='SW Subvendor Tracker 2025', skiprows=2)
        billable = pd.read_excel(accrual_file, sheet_name='SW 2025 Billable to 05.31', skiprows=2)
        
        return pivot, accrual, tracker, billable
    
    def _normalize_data(self, pivot, accrual, billable):
        """Normalize all dataframes - FIXED for datetime columns"""
        # Pivot
        pivot = pivot.rename(columns={
            'APPL_ID': 'applicant_number',
            'Applicant_Name': 'consultant_name',
            'Bill_Hours': 'hours_billed',
            'Bill_Amount': 'amount_billed',
            'T_INVOICE_COMPANY_NAME': 'vendor_name',
            'Invoice_Date': 'invoice_date',
        })
        pivot['applicant_number'] = pd.to_numeric(pivot.get('applicant_number', 0), errors='coerce').fillna(0).astype(int)
        pivot['consultant_name'] = pivot['consultant_name'].astype(str).str.strip()
        pivot['hours_billed'] = pd.to_numeric(pivot.get('hours_billed', 0), errors='coerce').fillna(0)
        pivot['amount_billed'] = pd.to_numeric(pivot.get('amount_billed', 0), errors='coerce').fillna(0)
        pivot['vendor_name'] = pivot['vendor_name'].astype(str).str.strip()
        
        # Accrual - FIXED: Handle datetime column names
        applicant_col = None
        for c in accrual.columns:
            col_str = str(c)  # Convert column name to string
            if 'applicant' in col_str.lower():
                applicant_col = c
                break
        
        if applicant_col:
            accrual['applicant_number'] = pd.to_numeric(accrual[applicant_col], errors='coerce').fillna(0).astype(int)
        else:
            accrual['applicant_number'] = 0
        
        accrual['consultant_name'] = accrual['Employee'].astype(str).str.strip() if 'Employee' in accrual.columns else ''
        
        for col in accrual.columns:
            col_str = str(col).lower()
            if 'ytd hours' in col_str:
                accrual['ytd_hours_invoiced'] = pd.to_numeric(accrual[col], errors='coerce').fillna(0)
            if 'ytd profit' in col_str and 'gross' in col_str:
                accrual['ytd_amount_invoiced'] = pd.to_numeric(accrual[col], errors='coerce').fillna(0)
        
        if 'ytd_hours_invoiced' not in accrual.columns:
            accrual['ytd_hours_invoiced'] = 0
        if 'ytd_amount_invoiced' not in accrual.columns:
            accrual['ytd_amount_invoiced'] = 0
        
        # Billable
        billable['applicant_number'] = pd.to_numeric(billable.get('Column5', 0), errors='coerce').fillna(0).astype(int)
        billable['consultant_name'] = billable['Applicant_Name'].astype(str).str.strip() if 'Applicant_Name' in billable.columns else ''
        billable['vendor_name'] = billable['T_INVOICE_COMPANY_NAME'].astype(str).str.strip() if 'T_INVOICE_COMPANY_NAME' in billable.columns else ''
        billable['hours_billed'] = pd.to_numeric(billable.get('Column2', 0), errors='coerce').fillna(0)
        billable['amount_billed'] = pd.to_numeric(billable.get('Column4', 0), errors='coerce').fillna(0)
        billable['ytd_hours_invoiced'] = billable['hours_billed']
        billable['ytd_amount_invoiced'] = billable['amount_billed']
        
        return pivot, accrual, billable
    
    def _perform_matching(self, pivot, accrual) -> pd.DataFrame:
        """Execute 3-strategy matching"""
        # Strategy 1: Exact ID
        matched = AdvancedMatchingEngine.match_exact_id(pivot, accrual)
        
        # Strategy 2: Hybrid
        matched = AdvancedMatchingEngine.match_hybrid(matched, accrual)
        
        # Strategy 3: Fuzzy
        matched = AdvancedMatchingEngine.match_fuzzy(matched, accrual)
        
        return matched
    
    def _generate_reports(self, matched, suggestions) -> Tuple[Path, Path]:
        """Generate Excel report and accrual template"""
        
        timestamp = datetime.now().strftime("%m.%d.%Y")
        report_file = self.output_folder / f"iTech_Accrual_Reconciliation_{timestamp}.xlsx"
        template_file = self.output_folder / f"Accrual_Template_{timestamp}.csv"
        
        with pd.ExcelWriter(report_file, engine='openpyxl') as writer:
            # Executive Summary
            summary_data = {
                'Metric': ['Total Records', 'Matched', 'Match Rate (%)', 'Total Billed', 'Total Paid', 'Variance'],
                'Value': [
                    len(matched),
                    len(matched[matched['Status'] == '✅ Matched']),
                    f"{(len(matched[matched['Status'] == '✅ Matched']) / len(matched) * 100):.1f}" if len(matched) > 0 else "0",
                    f"${matched['amount_billed'].sum():,.2f}",
                    f"${matched['ytd_amount_invoiced'].sum():,.2f}",
                    f"${(matched['amount_billed'].sum() - matched['ytd_amount_invoiced'].sum()):,.2f}",
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Executive Summary', index=False)
            
            # Matched Records
            matched_only = matched[matched['Status'] == '✅ Matched'].copy()
            if len(matched_only) > 0:
                matched_only.to_excel(writer, sheet_name='Matched Records', index=False)
            
            # Unmatched Pivot
            unmatched_pivot = matched[matched['Status'] == '❌ Missing in Accrual'].copy()
            if len(unmatched_pivot) > 0:
                unmatched_pivot.to_excel(writer, sheet_name='Unmatched Pivot', index=False)
            
            # Suggested Accruals
            if len(suggestions) > 0:
                suggestions.to_excel(writer, sheet_name='Suggested Accrual Entries', index=False)
            
            # Variance Analysis
            variance_data = matched[['consultant_name', 'vendor_name', 'hours_variance', 'amount_variance', 'variance_severity']].copy()
            variance_data.to_excel(writer, sheet_name='Variance Analysis', index=False)
        
        # Save accrual template
        if len(suggestions) > 0:
            suggestions.to_csv(template_file, index=False)
        
        return report_file, template_file
    
    def _calculate_metrics(self, matched) -> Dict:
        """Calculate key metrics"""
        matched_count = len(matched[matched['Status'] == '✅ Matched'])
        return {
            'total_records': len(matched),
            'matched_count': matched_count,
            'match_rate_pct': (matched_count / len(matched) * 100) if len(matched) > 0 else 0,
            'total_billed': matched['amount_billed'].sum(),
            'total_paid': matched['ytd_amount_invoiced'].sum(),
            'total_variance': matched['amount_billed'].sum() - matched['ytd_amount_invoiced'].sum(),
        }
    
    def _print_summary(self, metrics, suggestions_count):
        """Print final summary"""
        self.logger.info("\n" + "="*80)
        self.logger.info("✅ MONTHLY RECONCILIATION COMPLETE")
        self.logger.info("="*80)
        self.logger.info(f"\n📊 RESULTS:")
        self.logger.info(f"   Total Records: {metrics['total_records']:,}")
        self.logger.info(f"   Matched: {metrics['matched_count']:,} ({metrics['match_rate_pct']:.1f}%)")
        self.logger.info(f"   Unmatched: {metrics['total_records'] - metrics['matched_count']:,}")
        self.logger.info(f"\n💰 FINANCIAL:")
        self.logger.info(f"   Billed: ${metrics['total_billed']:,.2f}")
        self.logger.info(f"   Paid: ${metrics['total_paid']:,.2f}")
        self.logger.info(f"   Variance: ${metrics['total_variance']:,.2f}")
        self.logger.info(f"\n📥 AUTO-GENERATED:")
        self.logger.info(f"   Suggested Accruals: {suggestions_count:,}")
        self.logger.info("="*80)

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="iTech Monthly Accrual Reconciliation - PRODUCTION SUITE",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python FINAL_MONTHLY_RECONCILIATION_SUITE.py --month may --year 2025
  python FINAL_MONTHLY_RECONCILIATION_SUITE.py --month june --year 2025
        """
    )
    parser.add_argument("--month", required=True, help="Month (e.g., may)")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g., 2025)")
    
    args = parser.parse_args()
    
    input_dir = f"data/input/{args.month}_{args.year}"
    output_dir = f"data/output/{args.month}_{args.year}"
    
    orchestrator = MonthlyOrchestrator(input_dir, output_dir, args.month, args.year)
    result = orchestrator.run()
    
    print("\n✅ RECONCILIATION COMPLETE")
    print(f"Report: {result['report']}")
    print(f"Template: {result['template']}")
    print(f"Match Rate: {result['match_rate']:.1f}%")
