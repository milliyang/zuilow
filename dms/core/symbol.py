"""
Canonical symbol format for DMS: one format on write and read, no fallback queries.

Used for: InfluxDB writer/reader; aligns with PPT/ZuiLow/Futu (US.AAPL, HK.00700, SH.600519, SZ.000001).

Functions:
    normalize_symbol(symbol: str) -> str   Normalize to canonical form; empty string if invalid input.

Features:
    - Accepts no prefix (AAPL, 00700), yfinance (0700.HK, 600519.SS), or Futu-style (US.AAPL, HK.00700).
    - Output is always one of US.*, HK.00700, SH.*, SZ.* so storage and queries use a single key.
"""


def _pad_hk_code(code: str) -> str:
    """Pad HK code to 5 digits (e.g. 700 -> 00700)."""
    code = code.lstrip("0") or "0"
    return code.zfill(5)


def normalize_symbol(symbol: str) -> str:
    """Normalize to DMS/InfluxDB canonical form; return empty string if input is invalid or not a string."""
    if not symbol or not isinstance(symbol, str):
        return ""
    s = symbol.strip().upper()
    if "." not in s:
        if s.isdigit() and (len(s) <= 5 or s.startswith("0")):
            return "HK." + _pad_hk_code(s)
        return "US." + s
    parts = s.split(".", 1)
    prefix, suffix = parts[0], parts[1]
    if prefix in ("US", "HK", "SH", "SZ"):
        code = suffix
        if prefix == "HK":
            code = _pad_hk_code(code)
        return f"{prefix}.{code}"
    if suffix == "HK":
        return "HK." + _pad_hk_code(prefix)
    if suffix == "SS":
        return "SH." + prefix
    if suffix == "SZ":
        return "SZ." + prefix
    return s
