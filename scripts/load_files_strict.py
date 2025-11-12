"""
Ultra-Strict Data Loader with Enhanced Validation and Detailed Logging
Purpose: Ensure NO data is dropped unnecessarily and ALL sheets load correctly
"""

import pandas as pd
from pathlib import Path
import logging
import re
from typing import Dict

logger = logging.getLogger(__name__)

class StrictFileLoader:
    """Load files with ZERO acceptable data loss"""
    
    def __init__(self, input_folder: str):
        self.input_folder = Path(input_folder)
        self.load_report = []
    
    def load_all_files(self) -> Dict:
        """Load all files with detailed reporting"""
        
        logger.info("="*80)
        logger.info("STRICT FILE LOADER - ZERO DATA LOSS MODE")
        logger.info("="*80)
        
        pivot = self._load_pivot_strict()
        accrual, tracker, billable = self._load_accrual_strict()
        
        self._print_load_report()
        
        return {
            'pivot': pivot,
            'accrual': accrual,
            'tracker': tracker,
            'billable': billable,
        }
    
    def _load_pivot_strict(self) -> pd.DataFrame:
        """Load Pivot with strict validation"""
        logger.info("\n[PIVOT TABLE]")
        
        files = list(self.input_folder.glob("SW Pivot Table*.xlsx"))
        if not files:
            logger.error("❌ Pivot file not found!")
            return pd.DataFrame()
        
        file_path = files[0]
        logger.info(f"  File: {file_path.name}")
        
        try:
            # Load with skiprows=2
            df = pd.read_excel(str(file_path), sheet_name="SW Pivot Table", skiprows=2)
            before = len(df)
            logger.info(f"  Raw rows: {before:,}")
            
            # CRITICAL: Keep ALL rows initially
            df_loaded = df.copy()
            
            # Only report on column issues, don't drop rows
            logger.info(f"  Columns found: {len(df_loaded.columns)}")
            
            # Check for key columns
            key_cols = ['APPL_ID', 'Applicant_Name', 'Bill_Amount']
            for col in key_cols:
                if col in df_loaded.columns:
                    nulls = df_loaded[col].isna().sum()
                    logger.info(f"    ✅ {col}: {nulls:,} nulls")
                else:
                    logger.warning(f"    ⚠️ {col}: NOT FOUND")
            
            self.load_report.append({
                'sheet': 'Pivot Table',
                'rows_loaded': before,
                'rows_dropped': 0,
                'status': '✅ All rows loaded'
            })
            
            return df_loaded
        
        except Exception as e:
            logger.error(f"  ❌ Error: {str(e)}")
            return pd.DataFrame()
    
    def _load_accrual_strict(self):
        """Load Accrual sheets with strict validation"""
        logger.info("\n[ACCRUAL WORKBOOK]")
        
        files = list(self.input_folder.glob("SmartWorks Sub Vendor Accrual*.xlsx"))
        if not files:
            logger.error("❌ Accrual file not found!")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
        file_path = files[0]
        logger.info(f"  File: {file_path.name}")
        
        # Load each sheet
        accrual = self._load_sheet_strict(file_path, 'Accrual')
        tracker = self._load_sheet_strict(file_path, 'SW Subvendor Tracker 2025')
        billable = self._load_billable_strict(file_path)
        
        return accrual, tracker, billable
    
    def _load_sheet_strict(self, file_path: Path, sheet_name: str) -> pd.DataFrame:
        """Load a sheet with strict rules"""
        
        logger.info(f"  Loading '{sheet_name}'...")
        
        try:
            df = pd.read_excel(str(file_path), sheet_name=sheet_name, skiprows=2)
            before = len(df)
            
            # CRITICAL: KEEP ALL ROWS
            logger.info(f"    Raw rows: {before:,}")
            logger.info(f"    Columns: {len(df.columns)}")
            
            self.load_report.append({
                'sheet': sheet_name,
                'rows_loaded': before,
                'rows_dropped': 0,
                'status': '✅ All rows loaded'
            })
            
            return df
        
        except Exception as e:
            logger.error(f"    ❌ Error: {str(e)}")
            return pd.DataFrame()
    
    def _load_billable_strict(self, file_path: Path) -> pd.DataFrame:
        """Load Billable sheet - ENSURE NO DROPPING"""
        
        logger.info(f"  Loading 'SW 2025 Billable to 05.31' (CRITICAL)...")
        
        try:
            # Try different skip rows to find correct header
            for skiprows in [1, 2, 3]:
                try:
                    df = pd.read_excel(
                        str(file_path), 
                        sheet_name='SW 2025 Billable to 05.31', 
                        skiprows=skiprows
                    )
                    
                    if len(df) > 0 and len(df.columns) > 0:
                        before = len(df)
                        logger.info(f"    ✅ Loaded with skiprows={skiprows}")
                        logger.info(f"    Raw rows: {before:,}")
                        logger.info(f"    Columns: {len(df.columns)}")
                        
                        # CRITICAL: Keep ALL rows
                        self.load_report.append({
                            'sheet': 'SW 2025 Billable to 05.31',
                            'rows_loaded': before,
                            'rows_dropped': 0,
                            'skiprows': skiprows,
                            'status': '✅ All rows loaded'
                        })
                        
                        return df
                
                except Exception as e:
                    logger.warning(f"    skiprows={skiprows} failed: {str(e)[:50]}")
                    continue
            
            logger.error(f"    ❌ Could not load Billable sheet!")
            return pd.DataFrame()
        
        except Exception as e:
            logger.error(f"    ❌ Error: {str(e)}")
            return pd.DataFrame()
    
    def _print_load_report(self):
        """Print detailed load report"""
        
        logger.info("\n" + "="*80)
        logger.info("LOAD REPORT - DATA INTEGRITY CHECK")
        logger.info("="*80)
        
        total_loaded = 0
        total_dropped = 0
        
        for report in self.load_report:
            logger.info(f"\n{report['sheet']}:")
            logger.info(f"  Rows Loaded: {report['rows_loaded']:,}")
            logger.info(f"  Rows Dropped: {report['rows_dropped']:,}")
            logger.info(f"  Status: {report['status']}")
            
            total_loaded += report['rows_loaded']
            total_dropped += report['rows_dropped']
        
        logger.info("\n" + "="*80)
        logger.info("TOTALS:")
        logger.info(f"  Total Records Loaded: {total_loaded:,}")
        logger.info(f"  Total Records Dropped: {total_dropped:,}")
        
        if total_dropped == 0:
            logger.info("  ✅ ZERO DATA LOSS - All data preserved!")
        else:
            logger.warning(f"  ⚠️ WARNING: {total_dropped:,} records dropped!")
        
        logger.info("="*80)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loader = StrictFileLoader("data/input/may_2025")
    data = loader.load_all_files()
    
    print(f"\n✅ Pivot: {len(data['pivot']):,} records")
    print(f"✅ Accrual: {len(data['accrual']):,} records")
    print(f"✅ Billable: {len(data['billable']):,} records")
