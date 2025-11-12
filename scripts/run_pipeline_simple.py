"""
Simple Pipeline - Works with actual data structure
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from load_files import FileLoader
from reconciler_simple import SimpleReconciler
from report_generator_v2 import EnhancedReportGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%m_%d_%Y_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_pipeline(month: str, year: int):
    """Run simple reconciliation pipeline"""
    
    logger.info("="*80)
    logger.info(f"SIMPLE RECONCILIATION PIPELINE - {month.upper()} {year}")
    logger.info("="*80)
    
    try:
        # Step 1: Load files
        logger.info("\n[STEP 1/3] Loading input files...")
        input_folder = f"data/input/{month}_{year}"
        loader = FileLoader(input_folder)
        data = loader.load_all_files()
        
        # Step 2: Reconcile
        logger.info("\n[STEP 2/3] Reconciling...")
        reconciler = SimpleReconciler(data['pivot'], data['accrual'], data['tracker'])
        reconciled_df = reconciler.reconcile()
        exceptions_df = reconciler.get_exceptions()
        vendor_summary_df = reconciler.get_vendor_summary()
        
        reconciler.print_qa_report()
        
        # Step 3: Generate report
        logger.info("\n[STEP 3/3] Generating report...")
        output_folder = f"data/output/{month}_{year}"
        report_gen = EnhancedReportGenerator(output_folder)
        report_path = report_gen.generate_report(
            reconciled_df,
            exceptions_df,
            vendor_summary_df,
            reconciler.qa_metrics
        )
        
        logger.info("\n" + "="*80)
        logger.info("✅ PIPELINE COMPLETE")
        logger.info(f"Report: {report_path}")
        logger.info("="*80)
        
        return report_path
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True)
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    
    run_pipeline(args.month, args.year)
