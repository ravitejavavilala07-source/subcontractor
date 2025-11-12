"""
QuickBooks Memo Parser
Purpose: Extract structured data from QB memo strings
Patterns: "John Smith / #112233 / May / 160 hours @ $85/hr"
"""

import re
import pandas as pd
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class QuickBooksMemoParser:
    """Parse QB memos into structured fields"""
    
    MEMO_PATTERNS = [
        # Pattern 1: "Name / #ID / Month / Hours @ Rate"
        {
            'regex': r'(?P<name>[^/]+?)\s*/\s*#?(?P<id>\d+)\s*/\s*(?P<month>\w+)\s*/\s*(?P<hours>\d+\.?\d*)\s*(?:hours?|hrs?)\s*@\s*\$?(?P<rate>\d+\.?\d*)',
            'description': 'Name/ID/Month/Hours@Rate'
        },
        # Pattern 2: "Name - ID - Month - Hours - Rate"
        {
            'regex': r'(?P<name>[^-]+?)\s*-\s*#?(?P<id>\d+)\s*-\s*(?P<month>\w+)\s*-\s*(?P<hours>\d+\.?\d*)\s*-\s*\$?(?P<rate>\d+\.?\d*)',
            'description': 'Name-ID-Month-Hours-Rate'
        },
        # Pattern 3: "Applicant #ID (Name) - Month - Hours"
        {
            'regex': r'Applicant\s*#?(?P<id>\d+)\s*\((?P<name>[^)]+)\)\s*-\s*(?P<month>\w+)\s*-\s*(?P<hours>\d+\.?\d*)',
            'description': 'Applicant ID (Name) - Month - Hours'
        },
    ]
    
    MONTH_MAP = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
        'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }
    
    def __init__(self):
        self.parsed_count = 0
        self.failed_count = 0
        self.failures = []
    
    def parse_memo(self, memo: str) -> Optional[Dict]:
        """Parse a single memo string"""
        
        if not isinstance(memo, str) or not memo.strip():
            return None
        
        # Try each pattern
        for pattern_config in self.MEMO_PATTERNS:
            pattern = pattern_config['regex']
            match = re.search(pattern, memo, re.IGNORECASE)
            
            if match:
                try:
                    groups = match.groupdict()
                    
                    # Parse month
                    month_str = groups.get('month', '').lower()
                    month_num = self.MONTH_MAP.get(month_str, None)
                    
                    parsed = {
                        'applicant_id': int(groups['id']),
                        'consultant_name': groups['name'].strip().title(),
                        'service_month': f"2025-{month_num:02d}" if month_num else groups['month'],
                        'hours': float(groups['hours']),
                        'rate': float(groups['rate']),
                        'amount': float(groups['hours']) * float(groups['rate']),
                        'source': 'memo_parsed',
                        'pattern_matched': pattern_config['description'],
                    }
                    
                    self.parsed_count += 1
                    return parsed
                
                except Exception as e:
                    self.failed_count += 1
                    self.failures.append(f"Parse error: {str(e)} for memo: {memo[:50]}")
                    continue
        
        # No pattern matched
        self.failed_count += 1
        self.failures.append(f"No pattern match: {memo[:50]}")
        return None
    
    def parse_batch(self, memos: List[str]) -> pd.DataFrame:
        """Parse batch of memos"""
        
        logger.info(f"Parsing {len(memos):,} memos...")
        
        results = []
        for memo in memos:
            parsed = self.parse_memo(memo)
            if parsed:
                results.append(parsed)
        
        df = pd.DataFrame(results) if results else pd.DataFrame()
        
        logger.info(f"  ✅ Parsed: {self.parsed_count:,}")
        logger.info(f"  ❌ Failed: {self.failed_count:,}")
        
        if self.failures and len(self.failures) <= 5:
            for failure in self.failures[:5]:
                logger.warning(f"    - {failure}")
        
        return df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    test_memos = [
        "John Smith / #112233 / May / 160 hours @ $85/hr",
        "Jane Doe - 445566 - June - 120 - $95",
        "Applicant #778899 (Bob Johnson) - April - 140 hours",
    ]
    
    parser = QuickBooksMemoParser()
    df = parser.parse_batch(test_memos)
    print(df)
