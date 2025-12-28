"""Shared utility functions for data processing."""
import re
import pandas as pd
from datetime import date, timedelta
from typing import List, Optional


def numeric_series(s: pd.Series) -> pd.Series:
    """
    Convert a series to numeric, handling various Taiwan stock data formats.
    Handles: commas, parentheses for negatives, UTF-8 minus signs, fullwidth symbols.
    """
    def clean(x):
        if pd.isna(x):
            return None
        x = str(x).strip()
        if x in ('', '-', '--', 'N/A'):
            return None
        # Handle parentheses for negative numbers: (1,234) -> -1234
        if x.startswith('(') and x.endswith(')'):
            x = '-' + x[1:-1]
        # Remove commas and spaces
        x = x.replace(',', '').replace(' ', '')
        # Replace fullwidth and UTF-8 minus signs
        x = x.replace('－', '-').replace('−', '-').replace('＋', '+')
        return x

    cleaned = s.apply(clean)
    return pd.to_numeric(cleaned, errors='coerce')


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten MultiIndex columns from TWSE CSV responses.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(str(c).strip() for c in col if str(c).strip())
                      for col in df.columns.values]
    else:
        df.columns = [str(c).strip() for c in df.columns]
    return df


def find_col_any(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """
    Find the first column that contains any of the candidate substrings.
    Case-insensitive matching.
    """
    for cand in candidates:
        for col in df.columns:
            if cand.lower() in col.lower():
                return col
    return None


def iter_trading_days(start: date, end: date) -> List[date]:
    """
    Generate a list of potential trading days (weekdays) between start and end.
    """
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Monday=0 to Friday=4
            days.append(current)
        current += timedelta(days=1)
    return days


def to_roc_date(d: date) -> str:
    """
    Convert a date to ROC (Republic of China) date format: YYY/MM/DD
    Used by TPEX APIs.
    """
    roc_year = d.year - 1911
    return f"{roc_year}/{d.month:02d}/{d.day:02d}"


def parse_roc_date(roc_str: str) -> Optional[date]:
    """
    Parse ROC date string (YYY/MM/DD or YYY-MM-DD) to Python date.
    """
    if not roc_str:
        return None
    roc_str = roc_str.strip().replace('-', '/')
    parts = roc_str.split('/')
    if len(parts) != 3:
        return None
    try:
        year = int(parts[0]) + 1911
        month = int(parts[1])
        day = int(parts[2])
        return date(year, month, day)
    except (ValueError, TypeError):
        return None
