"""
Data Quality Utilities

Check continuity (gaps) and compute a quality score from validation and continuity results.

Functions:
    check_data_continuity(symbol, data, interval, max_gap_days) -> Dict
        Detect gaps in datetime index; skip weekends for 1d. Returns is_continuous, gaps, total_gaps, data_points, date_range, message.
    calculate_data_quality_score(data, validation_result, continuity_result) -> float
        Score 0–100: deduct for missing values, gaps, and validation issues.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


def check_data_continuity(
    symbol: str,
    data: pd.DataFrame,
    interval: str = "1d",
    max_gap_days: int = 7,
) -> Dict[str, Any]:
    """
    Check data continuity and detect gaps
    
    Args:
        symbol: Stock symbol
        data: DataFrame with datetime index
        interval: Time interval
        max_gap_days: Maximum acceptable gap in days
    
    Returns:
        Dict with continuity report
    """
    if data is None or data.empty:
        return {
            "is_continuous": False,
            "gaps": [],
            "total_gaps": 0,
            "message": "No data to check",
        }
    
    gaps = []
    
    # Sort by index
    data = data.sort_index()
    
    # Check gaps between consecutive dates
    for i in range(1, len(data)):
        prev_date = data.index[i-1]
        curr_date = data.index[i]
        
        # Calculate expected next date based on interval
        if interval == "1d":
            expected_gap = timedelta(days=1)
            # Skip weekends for daily data
            while True:
                next_expected = prev_date + expected_gap
                if next_expected.weekday() < 5:  # Monday = 0, Friday = 4
                    break
                expected_gap += timedelta(days=1)
            
            actual_gap = (curr_date - prev_date).days
            
            # Allow for weekends and holidays (up to max_gap_days)
            if actual_gap > max_gap_days:
                gaps.append({
                    "from": prev_date.date() if hasattr(prev_date, 'date') else prev_date,
                    "to": curr_date.date() if hasattr(curr_date, 'date') else curr_date,
                    "days": actual_gap,
                })
    
    is_continuous = len(gaps) == 0
    
    result = {
        "is_continuous": is_continuous,
        "gaps": gaps,
        "total_gaps": len(gaps),
        "data_points": len(data),
        "date_range": {
            "start": data.index.min().date() if hasattr(data.index.min(), 'date') else data.index.min(),
            "end": data.index.max().date() if hasattr(data.index.max(), 'date') else data.index.max(),
        },
    }
    
    if is_continuous:
        result["message"] = f"Data is continuous: {len(data)} records"
    else:
        result["message"] = f"Found {len(gaps)} gaps in data"
        logger.warning(f"{symbol}: Data continuity check failed - {len(gaps)} gaps detected")
        for gap in gaps:
            logger.warning(f"  Gap: {gap['from']} to {gap['to']} ({gap['days']} days)")
    
    return result


def calculate_data_quality_score(
    data: pd.DataFrame,
    validation_result: Dict[str, Any],
    continuity_result: Dict[str, Any],
) -> float:
    """
    Calculate overall data quality score (0-100)
    
    Args:
        data: DataFrame with historical data
        validation_result: Result from data validation
        continuity_result: Result from continuity check
    
    Returns:
        Quality score (0-100)
    """
    if data is None or data.empty:
        return 0.0
    
    score = 100.0
    
    # Deduct for missing values
    missing_ratio = data.isnull().sum().sum() / (len(data) * len(data.columns))
    score -= missing_ratio * 20
    
    # Deduct for data gaps
    if not continuity_result.get("is_continuous", True):
        gap_penalty = min(continuity_result.get("total_gaps", 0) * 5, 30)
        score -= gap_penalty
    
    # Deduct for validation issues
    if not validation_result.get("success", True):
        issue_count = len(validation_result.get("issues", []))
        issue_penalty = min(issue_count * 10, 40)
        score -= issue_penalty
    
    return max(0.0, min(100.0, score))
