"""
Rule-based security classification for 13F holdings rows (pilot / Phase 1 extension).
Adds: security_class, confidence_score, confidence_category.
"""

from __future__ import annotations

import re
from typing import Tuple

import pandas as pd


def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).upper().strip()


def classify_position(
    issuer_name: str,
    class_title: str,
    put_call: str,
    shares_type: str,
) -> Tuple[str, float, str]:
    """
    Return (security_class, confidence_score, confidence_category).
    Categories: HIGH / MEDIUM / LOW
    """
    iss = _norm(issuer_name)
    cls = _norm(class_title)
    pc = _norm(put_call)
    st = _norm(shares_type)

    if pc in ("CALL", "CALLS"):
        return ("OPTION_CALL", 1.0, "HIGH")
    if pc in ("PUT", "PUTS"):
        return ("OPTION_PUT", 1.0, "HIGH")

    if "WARRANT" in cls or "WARRANT" in iss:
        return ("WARRANT", 0.85, "MEDIUM")

    if "SPONSORED ADR" in cls or re.search(r"\bADR\b", cls) or " ADR" in cls:
        return ("ADR", 1.0, "HIGH")

    if "PRN" in st and "PREFERRED" not in cls:
        # principal — often bonds / notes; title is more informative
        pass

    if any(x in iss for x in ("SPDR ", "ISHARES ", "VANGUARD ", "INVESCO ", "ETF", "ISHARES")) or "ETF" in cls:
        if "OPTION" not in cls and "PUT" not in cls and "CALL" not in cls:
            return ("ETF", 0.9, "HIGH")

    if "GOLD" in iss and "TR" in iss:
        return ("GOLD_SILVER_TRUST", 1.0, "HIGH")

    if "PFD" in cls or "PREFERRED" in cls or "PRFD" in cls:
        return ("PREFERRED_STOCK", 0.85, "MEDIUM")

    if any(k in cls for k in ("NOTE", "DEBT", "BD ", "BOND")):
        return ("DEBT", 0.8, "MEDIUM")

    if re.search(r"\bCOM(M)?\b", cls) or "ORD" in cls or "COMMON" in cls or cls in ("SHS", "STOCK"):
        return ("COMMON_STOCK", 0.8, "MEDIUM")

    if re.search(r"\bCL\s*[AB]\b", cls) or "CL A" in cls or "CL B" in cls:
        return ("COMMON_STOCK", 0.75, "MEDIUM")

    if "UNIT" in cls and "TR" in cls:
        return ("ETF_ETN_TRUST_UNIT", 0.75, "MEDIUM")

    return ("UNCLASSIFIED", 0.4, "LOW")


def classify_holdings_df(df: pd.DataFrame) -> pd.DataFrame:
    """Append classification columns; does not mutate input."""
    out = df.copy()
    triples = out.apply(
        lambda r: classify_position(
            r.get("issuer_name", ""),
            r.get("class_title", ""),
            r.get("put_call", ""),
            r.get("shares_type", ""),
        ),
        axis=1,
        result_type="expand",
    )
    out["security_class"] = triples[0]
    out["confidence_score"] = triples[1]
    out["confidence_category"] = triples[2]

    if "report_date" in out.columns and pd.api.types.is_datetime64_any_dtype(out["report_date"]):
        out["quarter"] = out["report_date"].dt.to_period("Q").astype(str)
    elif "report_date" in out.columns:
        out["quarter"] = pd.to_datetime(out["report_date"], errors="coerce").dt.to_period("Q").astype(str)
    else:
        out["quarter"] = ""

    return out


def build_security_master(df_classified: pd.DataFrame) -> pd.DataFrame:
    """One row per CUSIP × class_title with aggregated classification (pilot summary)."""
    if df_classified.empty:
        return pd.DataFrame()
    gcols = ["cusip", "class_title"]
    agg = (
        df_classified.groupby(gcols, dropna=False)
        .agg(
            issuer_name=("issuer_name", "first"),
            security_class=("security_class", lambda s: s.mode().iloc[0] if len(s.mode()) else s.iloc[0]),
            confidence_score=("confidence_score", "mean"),
            confidence_category=("confidence_category", lambda s: s.mode().iloc[0] if len(s.mode()) else s.iloc[0]),
            filing_count=("accession", lambda s: s.nunique()),
            total_value_usd_k=("value_1000s", "sum"),
        )
        .reset_index()
    )
    if "quarter" in df_classified.columns:
        qmin = df_classified.groupby(gcols)["quarter"].min().rename("first_seen_quarter")
        qmax = df_classified.groupby(gcols)["quarter"].max().rename("last_seen_quarter")
        agg = agg.merge(qmin, on=gcols, how="left").merge(qmax, on=gcols, how="left")
    agg["classification_rule"] = "heuristic_v1"
    return agg.sort_values(["cusip", "class_title"])
