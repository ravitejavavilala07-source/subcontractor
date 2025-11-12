"""
Monthly Orchestrator - Complete Automation Flow
Purpose: End-to-end monthly reconciliation automation
"""

import logging
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from load_files_strict import StrictFileLoader
from data_normalizer import DataNormalizer
from quickbooks_memo_parser import QuickBooksMemoParser
from advanced_reconciler_final import AdvancedReconcilerFinal
from accrual_suggester import AccrualSuggester
from report_generator_final import FinalReportGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/monthly_run_{datetime.now().strftime('%m_%d_%Y_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_monthly_orchestration(month: str, year: int):
    """Complete monthly automation flow"""
    
    logger.info("="*80)
    logger.info(f"🚀 MONTHLY ORCHESTRATION - {month.upper()} {year}")
    logger.info("="*80)
    
    try:
        input_folder = f"data/input/{month}_{year}"
        
        # STEP 1: Load
        logger.info("\n[STEP 1/8] Loading source files...")
        loader = StrictFileLoader(input_folder)
        data = loader.load_all_files()
        logger.info(f"  ✅ Loaded {len(data['pivot']):,} pivot, {len(data['accrual']):,} accrual")
        
        # STEP 2: Normalize
        logger.info("\n[STEP 2/8] Normalizing schemas...")
        normalizer = DataNormalizer()
        pivot_norm = normalizer.normalize_pivot(data['pivot'])
        accrual_norm = normalizer.normalize_accrual(data['accrual'])
        billable_norm = normalizer.normalize_billable(data['billable'])
        
        # STEP 3: Parse memos (optional)
        logger.info("\n[STEP 3/8] Parsing QB memos (if present)...")
        if 'memo' in pivot_norm.columns:
            parser = QuickBooksMemoParser()
            memos_parsed = parser.parse_batch(pivot_norm['memo'].dropna().tolist())
            logger.info(f"  ✅ Parsed {len(memos_parsed)} memos")
        
        # STEP 4: Combine
        logger.info("\n[STEP 4/8] Combining transaction sources...")
        combined = pd.concat([pivot_norm, billable_norm], ignore_index=True)
        logger.info(f"  ✅ Combined: {len(combined):,} transactions")
        
        # STEP 5: Reconcile (advanced matching)
        logger.info("\n[STEP 5/8] Advanced reconciliation with 3-strategy matching...")
        reconciler = AdvancedReconcilerFinal(combined, accrual_norm, data['tracker'])
        reconciler.reconcile()
        
        matched = reconciler.get_reconciled_matched()
        unmatched_pivot = reconciler.get_reconciled_unmatched_pivot()
        unmatched_accrual = reconciler.get_reconciled_unmatched_accrual()
        metrics = reconciler.get_metrics()
        
        logger.info(f"  ✅ Matched: {metrics['matched']:,} ({metrics['match_rate_pct']:.1f}%)")
        logger.info(f"  ⚠️ Unmatched: {metrics['unmatched']:,}")
        
        # STEP 6: Calculate variances & suggestions
        logger.info("\n[STEP 6/8] Generating variance analysis & accrual suggestions...")
        suggester = AccrualSuggester(reconciler.reconciled)
        suggestions = suggester.generate_suggestions()
        logger.info(f"  ✅ Generated {len(suggestions):,} suggestions")
        
        # STEP 7: Generate reports
        logger.info("\n[STEP 7/8] Generating comprehensive reports...")
        output_folder = f"data/output/{month}_{year}"
        report_gen = FinalReportGenerator(output_folder)
        report_path = report_gen.generate_final_report(
            matched, unmatched_pivot, unmatched_accrual, suggestions,
            pd.DataFrame(), metrics
        )
        
        # Save accrual template
        accrual_template_path = Path(output_folder) / f"00_Accrual_Template_{datetime.now().strftime('%m_%d_%Y')}.csv"
        suggestions.to_csv(accrual_template_path, index=False)
        logger.info(f"  ✅ Accrual template: {accrual_template_path.name}")
        
        # STEP 8: Summary
        logger.info("\n[STEP 8/8] Generating final summary...")
        
        logger.info("\n" + "="*80)
        logger.info("✅ MONTHLY ORCHESTRATION COMPLETE")
        logger.info("="*80)
        logger.info(f"\n📊 RECONCILIATION METRICS:")
        logger.info(f"   Total Records: {metrics['total_records']:,}")
        logger.info(f"   ✅ Matched: {metrics['matched']:,} ({metrics['match_rate_pct']:.1f}%)")
        logger.info(f"   ⚠️ Unmatched: {metrics['unmatched']:,}")
        logger.info(f"\n💰 FINANCIAL SUMMARY:")
        logger.info(f"   Total Billed: ${metrics['total_billed']:,.2f}")
        logger.info(f"   Total Paid: ${metrics['total_paid']:,.2f}")
        logger.info(f"   Net Variance: ${metrics['total_variance']:,.2f}")
        logger.info(f"\n📥 AUTO-GENERATED TEMPLATES:")
        logger.info(f"   Suggested Accruals: {len(suggestions):,} entries")
        logger.info(f"   Ready for Upload: {accrual_template_path.name}")
        logger.info(f"\n📁 OUTPUTS:")
        logger.info(f"   Main Report: {report_path}")
        logger.info(f"   Accrual Template: {accrual_template_path}")
        logger.info("="*80)
        
        return {
            'report': report_path,
            'accrual_template': str(accrual_template_path),
            'metrics': metrics,
            'suggestions_count': len(suggestions),
        }

    except Exception as e:
        logger.error(f"❌ Orchestration failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="iTech Monthly Accrual Reconciliation Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python monthly_orchestrator.py --month may --year 2025
  python monthly_orchestrator.py --month june --year 2025
        """
    )
    parser.add_argument("--month", required=True, help="Month name (e.g., may)")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g., 2025)")
    
    args = parser.parse_args()
    result = run_monthly_orchestration(args.month.lower(), args.year)
    
    print("\n✅ MONTHLY ORCHESTRATION COMPLETE")
    print(f"Report: {result['report']}")
    print(f"Accrual Template: {result['accrual_template']}")
    print(f"Match Rate: {result['metrics']['match_rate_pct']:.1f}%")
