import re
import pandas as pd
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class QBMemoParserV1:
    """QB memo parser"""
    
    PATTERNS = [
        r'(?P<name>[^/]+?)\s*/\s*#?(?P<id>\d+)\s*/\s*(?P<month>\w+)\s*/\s*(?P<hours>\d+\.?\d*)\s*(?:hours?|hrs?)\s*@\s*\$?(?P<rate>\d+\.?\d*)',
        r'(?P<name>[^-]+?)\s*-\s*#?(?P<id>\d+)\s*-\s*(?P<month>\w+)\s*-\s*(?P<hours>\d+\.?\d*)\s*-\s*\$?(?P<rate>\d+\.?\d*)',
    ]
    
    MONTHS = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
              'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
    
    def __init__(self):
        self.parsed = 0
        self.failed = 0
    
    def parse_memo(self, memo: str, year: int = 2025) -> Optional[Dict]:
        if not isinstance(memo, str) or not memo.strip():
            return None
        
        for pattern in self.PATTERNS:
            match = re.search(pattern, memo, re.IGNORECASE)
            if match:
                try:
                    g = match.groupdict()
                    month_num = self.MONTHS.get(g['month'].lower(), None)
                    
                    result = {
                        'qb_applicant_id': int(g['id']),
                        'qb_consultant_name': g['name'].strip().title(),
                        'qb_service_month': f"{year}-{month_num:02d}" if month_num else g['month'],
                        'qb_hours': float(g['hours']),
                        'qb_rate': float(g['rate']),
                        'qb_parsed_amount': float(g['hours']) * float(g['rate']),
                    }
                    self.parsed += 1
                    return result
                except:
                    self.failed += 1
                    continue
        
        self.failed += 1
        return None
    
    def parse_batch(self, memos: List[str], year: int = 2025) -> pd.DataFrame:
        results = []
        for memo in memos:
            parsed = self.parse_memo(memo, year)
            if parsed:
                results.append(parsed)
        
        return pd.DataFrame(results) if results else pd.DataFrame()
