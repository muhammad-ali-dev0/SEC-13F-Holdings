"""
run_pipeline.py — Phase 1 EDGAR parse + rule-based security classification exports.

Modes:
  - `pilot`       : live EDGAR, `report_date` filtered to year range (default 2010–2012)
  - `pilot_sample`: offline embedded filings (2010–2012 quarters; mixed legacy / XML)
  - `live`        : live EDGAR for provided CIKs (see `sec_13f_parser` CLI for year filters)
"""
import sys, logging, argparse, json
from pathlib import Path
from datetime import datetime
from typing import Optional, List

sys.path.insert(0, str(Path(__file__).parent / "scripts"))
import sec_13f_parser as p1
import classify_holdings as clf

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger("pipeline")
OUT_BASE = Path(__file__).parent / "output"


def _load_ciks(ciks: Optional[List[str]], ciks_file: Optional[str]) -> List[str]:
    if ciks_file:
        p = Path(ciks_file)
        raw = p.read_text(encoding="utf-8").splitlines()
        file_ciks = [
            line.strip()
            for line in raw
            if line.strip() and not line.strip().startswith("#")
        ]
        return file_ciks
    return ciks or []


def run_all(
    mode: str = "pilot",
    ciks: Optional[List[str]] = None,
    max_filings: int = 50,
    start_year: int = 2010,
    end_year: int = 2012,
):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log.info("SEC 13F Pipeline  |  mode=%s  |  %s", mode.upper(), ts)

    # Phase 1
    log.info("── Phase 1: Parse Filings ──")
    if mode == "pilot":
        if not (ciks and len(ciks) > 0):
            raise ValueError("pilot mode requires --ciks or --ciks-file (at least 1 CIK)")
        filers, holdings = p1.run_live_with_year_range(
            ciks or [],
            max_filings_per_cik=max_filings,
            start_year=start_year,
            end_year=end_year,
        )
    elif mode == "pilot_sample":
        filers, holdings = p1.run_pilot()
    else:
        if not (ciks and len(ciks) > 0):
            raise ValueError("live mode requires --ciks or --ciks-file (at least 1 CIK)")
        filers, holdings = p1.run_live(ciks or [], max_filings)
    df_filer, df_holdings = p1.build_dataframes(filers, holdings)
    out_phase1 = OUT_BASE / "phase1"
    p1.save_outputs(df_filer, df_holdings, out_phase1)

    df_classified = clf.classify_holdings_df(df_holdings)
    df_classified.to_csv(out_phase1 / "holdings_classified.csv", index=False)
    df_master = clf.build_security_master(df_classified)
    if not df_master.empty:
        df_master.to_csv(out_phase1 / "security_master.csv", index=False)

    # Summary
    summary = {
        "run_timestamp": ts,
        "phase1": {
            "filer_count": int(len(df_filer)),
            "holdings_count": int(len(df_holdings)),
            "unique_cusips": int(df_holdings["cusip"].nunique()),
            "total_value_usd": float(df_holdings["value_usd"].sum()),
            "date_range": {
                "min": str(df_holdings["report_date"].min()),
                "max": str(df_holdings["report_date"].max()),
            },
            "unique_security_classes": int(df_classified["security_class"].nunique()) if len(df_classified) else 0,
        }
    }
    sp = OUT_BASE / f"pipeline_summary_{ts}.json"
    with open(sp, "w") as f:
        json.dump(summary, f, indent=2, default=str)

    _print_summary(summary)
    return df_filer, df_holdings, df_classified


def _print_summary(s):
    print(f"\n{'='*65}")
    print(f"  SEC 13F Pipeline — Complete")
    print(f"{'='*65}")
    p1s = s["phase1"]
    print(f"\n  Phase 1 — Filings")
    print(f"    Filers:        {p1s['filer_count']}  |  Holdings: {p1s['holdings_count']}  |  CUSIPs: {p1s['unique_cusips']}")
    print(f"    Date range:    {p1s['date_range']['min']}  →  {p1s['date_range']['max']}")
    print(f"    Total USD:     ${p1s['total_value_usd']:>20,.0f}")
    print(f"\n  Outputs → ./output/")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["pilot", "pilot_sample", "live"], default="pilot")
    ap.add_argument("--ciks", nargs="+", default=["1067983", "1350694"], help="Space-separated CIKs (e.g. 1067983 1350694)")
    ap.add_argument("--ciks-file", default=None, help="Path to text file with one CIK per line")
    ap.add_argument("--max-filings", type=int, default=80, help="Max filings per CIK to scan")
    ap.add_argument("--pilot-start-year", type=int, default=2010)
    ap.add_argument("--pilot-end-year", type=int, default=2012)
    args = ap.parse_args()
    ciks = _load_ciks(args.ciks, args.ciks_file)
    run_all(
        mode=args.mode,
        ciks=ciks,
        max_filings=args.max_filings,
        start_year=args.pilot_start_year,
        end_year=args.pilot_end_year,
    )
