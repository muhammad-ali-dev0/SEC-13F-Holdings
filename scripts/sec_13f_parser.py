"""
sec_13f_parser.py
=================
Phase 1 – Parse SEC EDGAR 13F filings into structured filer + holdings tables.

Supports both XML (post-2013) and legacy HTML/text (pre-2013) 13F information tables.

Usage (pilot – reads embedded sample data if network unavailable):
    python sec_13f_parser.py --mode pilot
    python sec_13f_parser.py --mode live --ciks 0001067983 0000102909 0001350694
"""

import re
import csv
import json
import logging
import argparse
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

import pandas as pd
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("13f_parser")

# ── Constants ──────────────────────────────────────────────────────────────────
EDGAR_BASE      = "https://www.sec.gov"
EDGAR_SEARCH    = "https://efts.sec.gov/LATEST/search-index?q=%2213F-HR%22&dateRange=custom&startdt={start}&enddt={end}&entity={cik}&forms=13F-HR"
EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_FILING_IDX  = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=13F-HR&dateb=&owner=include&count=40&search_text="

HEADERS = {"User-Agent": "Research/1.0 academic-research@example.com"}

# XML namespace used in modern 13F information tables
NS = {
    "ns1": "http://www.sec.gov/cgi-bin/browse-edgar",
    "ns2": "http://www.sec.gov/edgar/document/thirteenf/informationtable",
    "ns3": "http://www.sec.gov/edgar/common",
}

OUT_DIR = Path(__file__).parent / "output"


# ══════════════════════════════════════════════════════════════════════════════
# EDGAR HTTP helpers
# ══════════════════════════════════════════════════════════════════════════════

def edgar_get(url: str) -> Optional[str]:
    """HTTP GET with SEC rate-limit compliance (10 req/s max)."""
    if not HAS_REQUESTS:
        log.warning("requests not available – returning None for %s", url)
        return None
    import time
    time.sleep(0.12)           # ~8 req/s – well within SEC limits
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.text
    except Exception as e:
        log.error("GET %s  →  %s", url, e)
        return None


def get_filing_index_urls(cik: str, max_filings: int = 40) -> list[dict]:
    """
    Return list of {accession, filing_date, report_date, index_url}
    for all 13F-HR filings by this CIK via EDGAR submissions API.
    """
    cik_padded = cik.zfill(10)
    url = EDGAR_SUBMISSIONS.format(cik=cik_padded)
    raw = edgar_get(url)
    if not raw:
        return []

    try:
        sub = json.loads(raw)
    except json.JSONDecodeError:
        log.error("JSON decode error for %s", url)
        return []

    filings = sub.get("filings", {}).get("recent", {})
    forms       = filings.get("form", [])
    dates       = filings.get("filingDate", [])
    accessions  = filings.get("accessionNumber", [])
    periods     = filings.get("reportDate", [])

    results = []
    for form, date, acc, period in zip(forms, dates, accessions, periods):
        if form not in ("13F-HR", "13F-HR/A"):
            continue
        acc_clean = acc.replace("-", "")
        index_url = f"{EDGAR_BASE}/Archives/edgar/data/{cik}/{acc_clean}/{acc}-index.htm"
        results.append({
            "cik": cik,
            "accession": acc,
            "filing_date": date,
            "report_date": period,
            "index_url": index_url,
            "form_type": form,
        })
        if len(results) >= max_filings:
            break

    log.info("CIK %s: found %d 13F-HR filings", cik, len(results))
    return results


def get_info_table_url(index_url: str, accession: str) -> Optional[str]:
    """
    Scrape the filing index page and find the information table document URL.
    Prefers explicit “Information Table” rows; then XML/HTML infotable links;
    then primary Form **13F-HR** ``.txt`` (pre-XML era: holdings live in that file).
    """
    raw = edgar_get(index_url)
    if not raw:
        return None

    soup = BeautifulSoup(raw, "lxml")

    def abs_href(a) -> str:
        href = a["href"]
        return EDGAR_BASE + href if href.startswith("/") else href

    for row in soup.find_all("tr"):
        cells = [td.get_text(strip=True).lower() for td in row.find_all("td")]
        if any("information table" in c for c in cells):
            for td in row.find_all("td"):
                a = td.find("a", href=True)
                if a:
                    return abs_href(a)

    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if "infotable" in href or "13finfo" in href:
            return abs_href(a)

    # Primary Form 13F-HR text (older filings): Document Format Files table
    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 4:
            continue
        row_text = [td.get_text(strip=True) for td in tds]
        type_cell = row_text[-2].upper() if len(row_text) >= 2 else ""
        if type_cell == "13F-HR" or any("13f-hr" in c.lower() for c in row_text):
            for td in tds:
                a = td.find("a", href=True)
                if not a:
                    continue
                h = a["href"]
                if h.lower().endswith(".txt"):
                    return abs_href(a)

    return None


# ══════════════════════════════════════════════════════════════════════════════
# 13F Information Table Parsers
# ══════════════════════════════════════════════════════════════════════════════

def parse_info_table_xml(xml_text: str) -> list[dict]:
    """Parse XML 13F information table (standalone `informationTable` or embedded in `edgarSubmission`)."""
    holdings = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        log.warning("XML parse error: %s", e)
        return []

    # Strip namespaces for simpler access
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]

    for entry in root.iter("infoTable"):
        def g(tag: str) -> str:
            el = entry.find(tag)
            return el.text.strip() if el is not None and el.text else ""

        def ssh_block() -> tuple[str, str]:
            """Shares: modern filings nest under `shrsOrPrnAmt`; older fragments may be flat."""
            prn = g("sshPrnamt")
            typ = g("sshPrnamtType")
            if prn or typ:
                return prn, typ
            wrap = entry.find("shrsOrPrnAmt")
            if wrap is not None:
                return _child_text(wrap, "sshPrnamt"), _child_text(wrap, "sshPrnamtType")
            return "", ""

        prn_amt, prn_type = ssh_block()
        voting = entry.find("votingAuthority")
        sole = _child_text(voting, "Sole") if voting is not None else g("Sole")
        shared = _child_text(voting, "Shared") if voting is not None else g("Shared")
        none_v = _child_text(voting, "None") if voting is not None else g("None")

        holdings.append({
            "issuer_name":         g("nameOfIssuer"),
            "cusip":               _normalise_cusip(g("cusip")),
            "class_title":         g("titleOfClass"),
            "value_1000s":         _to_int(g("value")),
            "shares_or_principal": _to_int(prn_amt),
            "shares_type":         prn_type,
            "put_call":            g("putCall"),
            "investment_discretion": g("investmentDiscretion"),
            "other_manager":       g("otherManager"),
            "voting_sole":         _to_int(sole),
            "voting_shared":       _to_int(shared),
            "voting_none":         _to_int(none_v),
        })

    log.debug("XML parser: %d holdings", len(holdings))
    return holdings


def _child_text(parent, tag: str) -> str:
    el = parent.find(tag)
    return el.text.strip() if el is not None and el.text else ""


def parse_info_table_html(html_text: str) -> list[dict]:
    """
    Parse legacy HTML/text 13F information table (pre-2013 or non-XML filers).
    Handles both <TABLE> formats and pipe-delimited text.
    """
    holdings = []

    # Try pipe-delimited text first
    if "|" in html_text and html_text.count("|") > 20:
        return _parse_pipe_delimited(html_text)

    soup = BeautifulSoup(html_text, "lxml")
    tables = soup.find_all("table")
    if not tables:
        return []

    # Find the data table (usually has >5 columns)
    data_table = None
    for tbl in tables:
        rows = tbl.find_all("tr")
        if rows and len(rows[0].find_all(["td", "th"])) >= 5:
            data_table = tbl
            break

    if not data_table:
        return []

    rows = data_table.find_all("tr")
    # Detect header row
    header_row_idx = 0
    for i, row in enumerate(rows[:5]):
        text = row.get_text(" ").lower()
        if "name of issuer" in text or "cusip" in text:
            header_row_idx = i
            break

    headers = [
        _norm_header(th.get_text(" "))
        for th in rows[header_row_idx].find_all(["td", "th"])
    ]

    for row in rows[header_row_idx + 1:]:
        cells = [td.get_text(" ").strip() for td in row.find_all(["td", "th"])]
        if not cells or not any(cells):
            continue
        rec = dict(zip(headers, cells))
        holdings.append(_map_html_columns(rec))

    log.debug("HTML parser: %d holdings", len(holdings))
    return holdings


def _parse_pipe_delimited(text: str) -> list[dict]:
    """Parse pipe-delimited 13F text files."""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    holdings = []
    for line in lines:
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 7 and not parts[0].lower().startswith("name"):
            holdings.append({
                "issuer_name":         parts[0] if len(parts) > 0 else "",
                "class_title":         parts[1] if len(parts) > 1 else "",
                "cusip":               _normalise_cusip(parts[2]) if len(parts) > 2 else "",
                "value_1000s":         _to_int(parts[3]) if len(parts) > 3 else 0,
                "shares_or_principal": _to_int(parts[4]) if len(parts) > 4 else 0,
                "shares_type":         parts[5] if len(parts) > 5 else "",
                "put_call":            parts[6] if len(parts) > 6 else "",
                "investment_discretion": parts[7] if len(parts) > 7 else "",
                "voting_sole":         _to_int(parts[8]) if len(parts) > 8 else 0,
                "voting_shared":       _to_int(parts[9]) if len(parts) > 9 else 0,
                "voting_none":         _to_int(parts[10]) if len(parts) > 10 else 0,
                "other_manager":       "",
            })
    return holdings


def _norm_header(h: str) -> str:
    return re.sub(r"\s+", "_", h.strip().lower().replace("/", "_"))


def _map_html_columns(rec: dict) -> dict:
    """Map variable HTML column names to canonical schema."""
    def find(keys):
        for k in keys:
            for rk, rv in rec.items():
                if k in rk:
                    return rv
        return ""

    return {
        "issuer_name":         find(["name_of_issuer", "issuer", "name"]),
        "class_title":         find(["title_of_class", "class", "title"]),
        "cusip":               _normalise_cusip(find(["cusip"])),
        "value_1000s":         _to_int(find(["value"])),
        "shares_or_principal": _to_int(find(["shares", "sshprnamt", "amount"])),
        "shares_type":         find(["sshprnamttype", "type"]),
        "put_call":            find(["put_call", "putcall"]),
        "investment_discretion": find(["investment_discretion", "discretion"]),
        "other_manager":       find(["other_manager"]),
        "voting_sole":         _to_int(find(["sole"])),
        "voting_shared":       _to_int(find(["shared"])),
        "voting_none":         _to_int(find(["none"])),
    }


# Pre-XML Form 13F-HR: primary ``.txt`` submission with wrapped ASCII "Information Table"
_LEGACY_13F_ROW = re.compile(
    r"(?P<issuer>.+?)\s+"
    r"(?P<class>\bCom\b|Com\.|ADR|\bPut\b|\bCall\b|CL\s*B|CL\s*A|CLA[^\s]*|Pfd|SPL[^\s]*)\s+"
    r"(?P<cusip>[A-Z0-9]{4,})\s+"
    r"(?P<i1>\d+)\s+"
    r"(?P<i2>\d+)\s+"
    r"(?P<val>[\d,]+)\s+"
    r"(?P<shares>[\d,]+)\s+"
    r"X\s*(?P<tail>.+)$",
    re.I,
)

_LEGACY_13F_SHORT = re.compile(
    r"^\s*(?P<val>[\d,]+)\s+(?P<shares>[\d,]+)\s+X\s*(?P<tail>.+)$",
)

# Some filers (e.g. Bridgewater 2010–2012) use one line per row: no “X” marker, 9-char CUSIP inline.
_LEGACY_13F_FLAT = re.compile(
    r"^(.+?)\s+([A-Z0-9]{9})\s+([\d,]+)\s+([\d,]+)\s+SH\s+",
    re.I,
)

_LEGACY_CLASS_SUFFIXES = (
    "COM NEW",
    "CL B NEW",
    "SPONSORED ADR",
    "ORD SHS",
    "TR UNIT",
    "CL A",
    "CL B",
    "COM",
    "ADR",
    "PUT",
    "CALL",
    "PFD",
    "PRN",
    "ETF",
)


def _issuer_class_from_flat_head(head: str) -> tuple[str, str]:
    hu = head.strip().upper()
    h0 = head.strip()
    for suf in _LEGACY_CLASS_SUFFIXES:
        if hu.endswith(" " + suf):
            return h0[: -(len(suf) + 1)].strip().upper(), suf
    parts = h0.rsplit(None, 1)
    if len(parts) == 2:
        return parts[0].upper(), parts[1].upper()
    return h0.upper(), ""


def parse_info_table_legacy_txt(text: str) -> list[dict]:
    """
    Parse SEC Form 13F information table embedded in plain-text ``.txt`` submissions
    (common for 2010–2012). Rows may wrap: issuer name lines are merged with a data line.
    """
    if "Form 13F Information Table" not in text:
        return []
    holdings: list[dict] = []
    parts = re.split(r"\n\s*Form 13F Information Table\s*\n", text, flags=re.I)
    def _holding_from(issuer: str, cls: str, cusip9: str, val: int, sh: int, tail: str, put_call: str):
        tail_nums = re.findall(r"\d{1,3}(?:,\d{3})+", tail)
        sole = _to_int(tail_nums[-1]) if tail_nums else 0
        issuer_clean = re.sub(r"<[^>]+>", " ", issuer)
        issuer_clean = re.sub(r"\s+", " ", issuer_clean).strip().upper()
        if not issuer_clean or issuer_clean.startswith("<"):
            return None
        return {
            "issuer_name": issuer_clean,
            "cusip": cusip9,
            "class_title": cls.strip().upper(),
            "value_1000s": val,
            "shares_or_principal": sh,
            "shares_type": "SH",
            "put_call": put_call,
            "investment_discretion": "",
            "other_manager": "",
            "voting_sole": sole,
            "voting_shared": 0,
            "voting_none": 0,
        }

    for part in parts[1:]:
        if re.search(r"GRAND\s+TOTAL", part, re.I):
            part = re.split(r"\n\s*GRAND\s+TOTAL", part, maxsplit=1, flags=re.I)[0]
        name_buf: list[str] = []
        last_full: Optional[dict] = None
        for line in part.splitlines():
            ln = line.rstrip()
            if not ln.strip():
                continue
            strip = ln.strip()
            if strip.startswith("Column ") or "Name of Issuer" in ln:
                name_buf.clear()
                last_full = None
                continue
            if set(strip) <= {"-", " "}:
                name_buf.clear()
                last_full = None
                continue
            if "<" in strip and ">" in strip and not re.search(r"\d{1,3}(?:,\d{3})", strip):
                continue
            fm = _LEGACY_13F_FLAT.match(strip)
            if fm:
                head, cusip9, val_s, sh_s = fm.group(1), fm.group(2), fm.group(3), fm.group(4)
                issuer, cls = _issuer_class_from_flat_head(head)
                if issuer and cusip9:
                    vm = re.search(
                        r"SOLE\s+(\d{1,3}(?:,\d{3})*)\s+(\d{1,3}(?:,\d{3})*)\s+(\d{1,3}(?:,\d{3})*)\s*$",
                        ln,
                        re.I,
                    )
                    if vm:
                        v_sole, v_share, v_none = _to_int(vm.group(1)), _to_int(vm.group(2)), _to_int(vm.group(3))
                    else:
                        v_sole, v_share, v_none = _to_int(sh_s), 0, 0
                    holdings.append({
                        "issuer_name": issuer,
                        "cusip": _normalise_cusip(cusip9),
                        "class_title": cls,
                        "value_1000s": _to_int(val_s),
                        "shares_or_principal": _to_int(sh_s),
                        "shares_type": "SH",
                        "put_call": "",
                        "investment_discretion": "SOLE",
                        "other_manager": "",
                        "voting_sole": v_sole,
                        "voting_shared": v_share,
                        "voting_none": v_none,
                    })
                name_buf.clear()
                last_full = None
                continue
            nums = re.findall(r"\d{1,3}(?:,\d{3})+", ln)
            has_x = bool(re.search(r"\sX\s", ln)) or " X " in ln
            sm = _LEGACY_13F_SHORT.match(ln)
            if sm and has_x and last_full and not re.search(r"\bCom\b|ADR|Pfd|Put|Call", ln, re.I):
                rec = _holding_from(
                    last_full["issuer"],
                    last_full["class_title"],
                    last_full["cusip"],
                    _to_int(sm.group("val")),
                    _to_int(sm.group("shares")),
                    sm.group("tail"),
                    last_full["put_call"],
                )
                if rec:
                    holdings.append(rec)
                continue
            if has_x and len(nums) >= 2:
                combined = " ".join(name_buf + [strip])
                m = _LEGACY_13F_ROW.match(combined) or _LEGACY_13F_ROW.search(combined)
                if m:
                    cusip9 = _normalise_cusip(f"{m.group('cusip')}{m.group('i1')}{m.group('i2')}")
                    raw_cls = m.group("class").strip().upper().replace(" ", "")
                    put_call = ""
                    if raw_cls == "PUT":
                        put_call = "Put"
                    elif raw_cls == "CALL":
                        put_call = "Call"
                    iss = re.sub(r"\s+", " ", m.group("issuer")).strip()
                    rec = _holding_from(
                        iss,
                        m.group("class"),
                        cusip9,
                        _to_int(m.group("val")),
                        _to_int(m.group("shares")),
                        m.group("tail"),
                        put_call,
                    )
                    if rec:
                        holdings.append(rec)
                    last_full = {
                        "issuer": iss,
                        "class_title": m.group("class").strip().upper(),
                        "cusip": cusip9,
                        "put_call": put_call,
                    }
                name_buf.clear()
            else:
                if "Column" not in ln and not ln.strip().startswith("("):
                    name_buf.append(strip)
                    if not any(ch.isdigit() for ch in strip):
                        last_full = None
    log.debug("legacy txt parser: %d holdings", len(holdings))
    return holdings


# ══════════════════════════════════════════════════════════════════════════════
# Filer metadata from submission header
# ══════════════════════════════════════════════════════════════════════════════

def extract_filer_metadata(cik: str, filing_meta: dict, xml_header: Optional[str] = None) -> dict:
    """
    Build a filer record. Augments EDGAR submission data with any
    info from the filing's primary document header if available.
    """
    manager_name = ""
    if xml_header:
        # Try to extract manager name from SEC-HEADER or XML cover page
        m = re.search(r"COMPANY CONFORMED NAME:\s*(.+)", xml_header, re.I)
        if m:
            manager_name = m.group(1).strip()
        if not manager_name:
            m = re.search(r"^\s*Name:\s*(.+)$", xml_header, re.I | re.M)
            if m:
                manager_name = m.group(1).strip()
        if not manager_name:
            soup = BeautifulSoup(xml_header, "lxml")
            for tag in ["registrantname", "filingmanager", "name"]:
                el = soup.find(tag)
                if el:
                    manager_name = el.get_text(strip=True)
                    break

    return {
        "cik":          cik.lstrip("0"),
        "manager_name": manager_name or f"CIK_{cik}",
        "filing_date":  filing_meta.get("filing_date", ""),
        "report_date":  filing_meta.get("report_date", ""),
        "form_type":    filing_meta.get("form_type", "13F-HR"),
        "accession":    filing_meta.get("accession", ""),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Utilities
# ══════════════════════════════════════════════════════════════════════════════

def _normalise_cusip(raw: str) -> str:
    """Strip non-alphanumeric chars, uppercase, validate 9-char CUSIP."""
    cusip = re.sub(r"[^A-Z0-9]", "", raw.upper())
    return cusip if len(cusip) == 9 else cusip[:9].ljust(9, "0") if cusip else ""


def _to_int(s: str) -> int:
    """Convert comma-separated numeric string to int."""
    try:
        return int(re.sub(r"[^\d]", "", str(s)) or 0)
    except (ValueError, TypeError):
        return 0


def detect_format(text: str) -> str:
    """Detect info-table serialization: XML, legacy plain-text 13F-HR, or HTML/table."""
    stripped = text.lstrip()
    sl = stripped.lower()
    if stripped.startswith("<?xml") or "<informationtable" in sl[:800]:
        return "xml"
    # EDGAR “.txt” 13F-HR often embeds <PAGE><TABLE> SGML — still legacy, not browseable HTML.
    if "form 13f information table" in sl and (
        "<page>" in sl[:20_000]
        or re.search(r"[A-Z0-9]{9}\s+\d{1,3}(?:,\d{3})*\s+\d{1,3}(?:,\d{3})*\s+SH\s+", stripped, re.M)
        or re.search(r"\sX\s+\d", sl)
    ):
        return "legacy_txt"
    return "html"


def filing_uid(cik: str, report_date: str) -> str:
    return hashlib.md5(f"{cik}_{report_date}".encode()).hexdigest()[:12]


# ══════════════════════════════════════════════════════════════════════════════
# Main pipeline orchestration
# ══════════════════════════════════════════════════════════════════════════════

def process_filing(cik: str, filing_meta: dict) -> tuple[dict, list[dict]]:
    """
    Download and parse one 13F filing.
    Returns (filer_record, [holding_records])
    """
    index_url = filing_meta["index_url"]
    log.info("Processing CIK=%s  date=%s  accession=%s",
             cik, filing_meta["report_date"], filing_meta["accession"])

    info_url = get_info_table_url(index_url, filing_meta["accession"])
    if not info_url:
        log.warning("Could not find info table URL for %s", index_url)
        return extract_filer_metadata(cik, filing_meta, None), []

    raw = edgar_get(info_url)
    if not raw:
        return extract_filer_metadata(cik, filing_meta, None), []

    filer = extract_filer_metadata(cik, filing_meta, raw)

    fmt = detect_format(raw)
    if fmt == "xml":
        holdings_raw = parse_info_table_xml(raw)
    elif fmt == "legacy_txt":
        holdings_raw = parse_info_table_legacy_txt(raw)
    else:
        holdings_raw = parse_info_table_html(raw)

    # Enrich each holding with filing context
    uid = filing_uid(cik, filing_meta["report_date"])
    holdings = []
    for h in holdings_raw:
        h["cik"]         = cik.lstrip("0")
        h["report_date"] = filing_meta["report_date"]
        h["filing_date"] = filing_meta["filing_date"]
        h["accession"]   = filing_meta["accession"]
        h["filing_uid"]  = uid
        h["row_id"]      = f"{uid}_{len(holdings):04d}"
        holdings.append(h)

    log.info("  → %d holdings parsed (fmt=%s)", len(holdings), fmt)
    return filer, holdings


def run_live(ciks: list[str], max_filings_per_cik: int = 5) -> tuple[list, list]:
    """Fetch and parse real EDGAR filings."""
    all_filers = []
    all_holdings = []

    for cik in ciks:
        filing_list = get_filing_index_urls(cik, max_filings=max_filings_per_cik)
        for meta in filing_list:
            filer, holdings = process_filing(cik, meta)
            if filer:
                all_filers.append(filer)
            all_holdings.extend(holdings)

    return all_filers, all_holdings


def _year_from_yyyy_mm_dd(s: str) -> Optional[int]:
    if not s or len(s) < 4:
        return None
    try:
        return int(str(s)[:4])
    except (TypeError, ValueError):
        return None


def run_live_with_year_range(
    ciks: list[str],
    max_filings_per_cik: int = 50,
    start_year: int = 2010,
    end_year: int = 2013,
) -> tuple[list, list]:
    """
    Fetch and parse real EDGAR filings limited to a report_date year range.

    Note: the EDGAR submissions API returns only the most recent `max_filings_per_cik`
    filings per CIK. If you set that too low, older years in the range may be missed.
    """
    all_filers = []
    all_holdings = []

    for cik in ciks:
        filing_list = get_filing_index_urls_year_range(
            cik=cik,
            start_year=start_year,
            end_year=end_year,
            max_filings=max_filings_per_cik,
        )

        log.info(
            "CIK %s: year-range filings=%d  (%d–%d)",
            cik,
            len(filing_list),
            start_year,
            end_year,
        )

        for meta in filing_list:
            filer, holdings = process_filing(cik, meta)
            if filer:
                all_filers.append(filer)
            all_holdings.extend(holdings)

    return all_filers, all_holdings


def get_filing_index_urls_year_range(
    cik: str,
    start_year: int,
    end_year: int,
    max_filings: int = 200,
) -> list[dict]:
    """
    Return filing index metadata restricted to `report_date` years.

    This goes beyond the “recent” subset by also loading additional submission
    JSON files referenced in the SEC submissions API (the `filings.files` field).
    """
    cik_padded = cik.zfill(10)
    base_url = EDGAR_SUBMISSIONS.format(cik=cik_padded)
    raw = edgar_get(base_url)
    if not raw:
        return []

    try:
        sub = json.loads(raw)
    except json.JSONDecodeError:
        return []

    cik_dir = cik.lstrip("0")
    results: list[dict] = []

    def maybe_add(form: str, filing_date: str, report_date: str, acc: str):
        if form not in ("13F-HR", "13F-HR/A"):
            return
        y = _year_from_yyyy_mm_dd(report_date)
        if y is None:
            return
        if y < start_year or y > end_year:
            return
        acc_clean = acc.replace("-", "")
        index_url = f"{EDGAR_BASE}/Archives/edgar/data/{cik_dir}/{acc_clean}/{acc}-index.htm"
        results.append(
            {
                "cik": cik,
                "accession": acc,
                "filing_date": filing_date,
                "report_date": report_date,
                "index_url": index_url,
                "form_type": form,
            }
        )

    filings = sub.get("filings", {})
    recent = filings.get("recent", {}) or {}
    forms = recent.get("form", []) or []
    filing_dates = recent.get("filingDate", []) or []
    accessions = recent.get("accessionNumber", []) or []
    report_dates = recent.get("reportDate", []) or []

    for form, filing_date, acc, report_date in zip(forms, filing_dates, accessions, report_dates):
        maybe_add(form, filing_date, report_date, acc)
        if len(results) >= max_filings:
            break

    if len(results) >= max_filings:
        return results

    # Load older filing chunks from `filings.files`
    files_meta = filings.get("files", []) or []
    for fm in files_meta:
        # fm typically includes: name, filingCount, filingFrom, filingTo
        filing_from = _year_from_yyyy_mm_dd(str(fm.get("filingFrom", "")))
        filing_to = _year_from_yyyy_mm_dd(str(fm.get("filingTo", "")))

        # Skip chunks that are entirely outside the requested year range
        if filing_from is not None and filing_from > end_year:
            continue
        if filing_to is not None and filing_to < start_year:
            continue

        name = fm.get("name")
        if not name:
            continue
        url = f"https://data.sec.gov/submissions/{name}"
        raw2 = edgar_get(url)
        if not raw2:
            continue
        try:
            data2 = json.loads(raw2)
        except json.JSONDecodeError:
            continue

        forms2 = data2.get("form", []) or []
        filing_dates2 = data2.get("filingDate", []) or []
        accessions2 = data2.get("accessionNumber", []) or []
        report_dates2 = data2.get("reportDate", []) or []

        for form, filing_date, acc, report_date in zip(forms2, filing_dates2, accessions2, report_dates2):
            maybe_add(form, filing_date, report_date, acc)
            if len(results) >= max_filings:
                return results

    return results


# ══════════════════════════════════════════════════════════════════════════════
# PILOT MODE — rich embedded sample data (no network needed)
# ══════════════════════════════════════════════════════════════════════════════

SAMPLE_XML_1 = """<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>8945623</value>
    <sshPrnamt>52341200</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>52341200</Sole><Shared>0</Shared><None>0</None></votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>MICROSOFT CORP</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>594918104</cusip>
    <value>6234891</value>
    <sshPrnamt>18930400</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>18930400</Sole><Shared>0</Shared><None>0</None></votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>AMAZON COM INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>023135106</cusip>
    <value>4123400</value>
    <sshPrnamt>1340000</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>1340000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>SPDR S&amp;P 500 ETF TR</nameOfIssuer>
    <titleOfClass>ETF</titleOfClass>
    <cusip>78462F103</cusip>
    <value>2345000</value>
    <sshPrnamt>6230000</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>6230000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>PUT</titleOfClass>
    <cusip>037833100</cusip>
    <value>145000</value>
    <sshPrnamt>1200000</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <putCall>Put</putCall>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>0</Sole><Shared>0</Shared><None>1200000</None></votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>TAIWAN SEMICONDUCTOR MFG LTD</nameOfIssuer>
    <titleOfClass>ADR</titleOfClass>
    <cusip>874039100</cusip>
    <value>892400</value>
    <sshPrnamt>9800000</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>9800000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>BERKSHIRE HATHAWAY INC DEL</nameOfIssuer>
    <titleOfClass>CL B</titleOfClass>
    <cusip>084670702</cusip>
    <value>1234500</value>
    <sshPrnamt>3400000</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <investmentDiscretion>SHARED</investmentDiscretion>
    <otherManager>2</otherManager>
    <votingAuthority><Sole>0</Sole><Shared>3400000</Shared><None>0</None></votingAuthority>
  </infoTable>
</informationTable>"""

SAMPLE_XML_2 = """<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>ALPHABET INC</nameOfIssuer>
    <titleOfClass>CL A</titleOfClass>
    <cusip>02079K305</cusip>
    <value>3412000</value>
    <sshPrnamt>2890000</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>2890000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>NVIDIA CORP</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>67066G104</cusip>
    <value>7823000</value>
    <sshPrnamt>8920000</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>8920000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>INVESCO QQQ TRUST SER 1</nameOfIssuer>
    <titleOfClass>ETF</titleOfClass>
    <cusip>46090E103</cusip>
    <value>1200000</value>
    <sshPrnamt>3100000</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>3100000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>NVIDIA CORP</nameOfIssuer>
    <titleOfClass>CALL</titleOfClass>
    <cusip>67066G104</cusip>
    <value>890000</value>
    <sshPrnamt>5600000</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <putCall>Call</putCall>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>0</Sole><Shared>0</Shared><None>5600000</None></votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>WELLS FARGO &amp; CO</nameOfIssuer>
    <titleOfClass>PFD SER L</titleOfClass>
    <cusip>949746710</cusip>
    <value>234000</value>
    <sshPrnamt>230000</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>230000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>UNITEDHEALTH GROUP INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>91324P102</cusip>
    <value>4560000</value>
    <sshPrnamt>9200000</sshPrnamt>
    <sshPrnamtType>SH</sshPrnamtType>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority><Sole>9200000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
  </infoTable>
</informationTable>"""

SAMPLE_HTML_LEGACY = """
<HTML><BODY>
<TABLE BORDER="1">
<TR><TH>NAME OF ISSUER</TH><TH>TITLE OF CLASS</TH><TH>CUSIP</TH>
    <TH>VALUE (x$1000)</TH><TH>SHARES/PRN AMT</TH><TH>SH/PRN</TH>
    <TH>PUT/CALL</TH><TH>DISCRETION</TH><TH>SOLE</TH><TH>SHARED</TH><TH>NONE</TH></TR>
<TR><TD>EXXON MOBIL CORP</TD><TD>COM</TD><TD>30231G102</TD>
    <TD>1890234</TD><TD>19234000</TD><TD>SH</TD><TD></TD><TD>SOLE</TD><TD>19234000</TD><TD>0</TD><TD>0</TD></TR>
<TR><TD>JOHNSON &amp; JOHNSON</TD><TD>COM</TD><TD>478160104</TD>
    <TD>2340123</TD><TD>15678000</TD><TD>SH</TD><TD></TD><TD>SOLE</TD><TD>15678000</TD><TD>0</TD><TD>0</TD></TR>
<TR><TD>JPMORGAN CHASE &amp; CO</TD><TD>COM</TD><TD>46625H100</TD>
    <TD>3412890</TD><TD>27890000</TD><TD>SH</TD><TD></TD><TD>SOLE</TD><TD>27890000</TD><TD>0</TD><TD>0</TD></TR>
<TR><TD>PROCTER &amp; GAMBLE CO</TD><TD>COM</TD><TD>742718109</TD>
    <TD>1567000</TD><TD>12340000</TD><TD>SH</TD><TD></TD><TD>SHARED</TD><TD>0</TD><TD>12340000</TD><TD>0</TD></TR>
<TR><TD>VANGUARD TOTAL MKT ETF</TD><TD>ETF</TD><TD>922908769</TD>
    <TD>890000</TD><TD>7800000</TD><TD>SH</TD><TD></TD><TD>SOLE</TD><TD>7800000</TD><TD>0</TD><TD>0</TD></TR>
</TABLE>
</BODY></HTML>"""

# Embedded pilot: report quarters 2010Q1–2012Q4 (12 quarters) × multiple filers → 2010–2012 coverage.
_PILOT_REPORT_DATES = [
    "2010-03-31",
    "2010-06-30",
    "2010-09-30",
    "2010-12-31",
    "2011-03-31",
    "2011-06-30",
    "2011-09-30",
    "2011-12-31",
    "2012-03-31",
    "2012-06-30",
    "2012-09-30",
    "2012-12-31",
]
_PILOT_FILING_DATES = [
    "2010-05-17",
    "2010-08-16",
    "2010-11-15",
    "2011-02-14",
    "2011-05-16",
    "2011-08-15",
    "2011-11-14",
    "2012-02-14",
    "2012-05-15",
    "2012-08-14",
    "2012-11-14",
    "2013-02-14",
]

PILOT_FILINGS = []
_PILOT_NUM_FILERS = 45

for i in range(_PILOT_NUM_FILERS):
    q = i % len(_PILOT_REPORT_DATES)
    pf_report_date = _PILOT_REPORT_DATES[q]
    pf_filing_date = _PILOT_FILING_DATES[q]
    # Stress pre-2013 paths: ~44% legacy HTML, rest XML information-table fragments.
    use_legacy_html = (i % 9) in (1, 2, 3, 7)

    if use_legacy_html:
        pf_format = "html"
        pf_data = SAMPLE_HTML_LEGACY
    else:
        pf_format = "xml"
        pf_data = SAMPLE_XML_2 if (i % 2) else SAMPLE_XML_1

    cik_num = 1000000 + i * 17
    pf_cik = str(cik_num).zfill(10)
    fy = pf_filing_date[2:4]
    pf_accession_seq = 10_000 + i
    pf_accession = f"{int(pf_cik):010d}-{fy}-{pf_accession_seq:06d}"

    PILOT_FILINGS.append(
        {
            "cik": pf_cik,
            "manager_name": f"PILOT MANAGER {i+1:02d}",
            "filing_date": pf_filing_date,
            "report_date": pf_report_date,
            "form_type": "13F-HR",
            "accession": pf_accession,
            "format": pf_format,
            "data": pf_data,
        }
    )


def run_pilot() -> tuple[list, list]:
    """Parse embedded pilot filings — no network required."""
    log.info("Running PILOT mode with %d embedded sample filings", len(PILOT_FILINGS))
    all_filers = []
    all_holdings = []

    for pf in PILOT_FILINGS:
        filer = {
            "cik":          pf["cik"].lstrip("0"),
            "manager_name": pf["manager_name"],
            "filing_date":  pf["filing_date"],
            "report_date":  pf["report_date"],
            "form_type":    pf["form_type"],
            "accession":    pf["accession"],
        }
        all_filers.append(filer)

        fmt = pf["format"]
        raw_data = pf["data"]
        if fmt == "xml":
            holdings_raw = parse_info_table_xml(raw_data)
        else:
            holdings_raw = parse_info_table_html(raw_data)

        uid = filing_uid(pf["cik"], pf["report_date"] + pf["accession"])
        for i, h in enumerate(holdings_raw):
            h["cik"]         = pf["cik"].lstrip("0")
            h["report_date"] = pf["report_date"]
            h["filing_date"] = pf["filing_date"]
            h["accession"]   = pf["accession"]
            h["filing_uid"]  = uid
            h["row_id"]      = f"{uid}_{i:04d}"
            all_holdings.append(h)

        log.info("  %s  (%s)  →  %d holdings", pf["manager_name"], pf["report_date"], len(holdings_raw))

    return all_filers, all_holdings


# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════

FILER_COLS = ["cik", "manager_name", "filing_date", "report_date", "form_type", "accession"]

HOLDINGS_COLS = [
    "row_id", "cik", "report_date", "filing_date", "accession", "filing_uid",
    "issuer_name", "cusip", "class_title",
    "value_1000s", "value_usd",
    "shares_or_principal", "shares_type",
    "put_call", "investment_discretion", "other_manager",
    "voting_sole", "voting_shared", "voting_none",
]


def build_dataframes(filers: list, holdings: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_f = pd.DataFrame(filers, columns=FILER_COLS)
    df_f = df_f.drop_duplicates(subset=["cik", "report_date", "accession"])
    df_f["filing_date"] = pd.to_datetime(df_f["filing_date"], errors="coerce")
    df_f["report_date"] = pd.to_datetime(df_f["report_date"], errors="coerce")
    df_f = df_f.sort_values(["cik", "report_date"])

    if not holdings:
        df_h = pd.DataFrame(columns=HOLDINGS_COLS)
    else:
        df_h = pd.DataFrame(holdings)
    if "value_1000s" in df_h.columns:
        df_h["value_usd"] = df_h["value_1000s"] * 1000
    else:
        df_h["value_usd"] = 0
    df_h["report_date"] = pd.to_datetime(df_h["report_date"], errors="coerce")
    df_h["filing_date"] = pd.to_datetime(df_h["filing_date"], errors="coerce")

    # Ensure all schema columns exist
    for col in HOLDINGS_COLS:
        if col not in df_h.columns:
            df_h[col] = ""

    df_h = df_h[HOLDINGS_COLS].sort_values(["cik", "report_date", "issuer_name"])
    return df_f, df_h


def save_outputs(df_filer: pd.DataFrame, df_holdings: pd.DataFrame, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    filer_csv = out_dir / "filer_table.csv"
    holdings_csv = out_dir / "holdings_table.csv"

    df_filer.to_csv(filer_csv, index=False)
    df_holdings.to_csv(holdings_csv, index=False)

    # Try parquet
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        pq.write_table(pa.Table.from_pandas(df_filer),   out_dir / "filer_table.parquet")
        pq.write_table(pa.Table.from_pandas(df_holdings), out_dir / "holdings_table.parquet")
        log.info("Parquet files written")
    except ImportError:
        log.info("pyarrow not available — CSV only (install pyarrow for Parquet output)")

    log.info("Filer table:    %s  (%d rows)", filer_csv, len(df_filer))
    log.info("Holdings table: %s  (%d rows)", holdings_csv, len(df_holdings))
    return filer_csv, holdings_csv


def main():
    parser = argparse.ArgumentParser(description="SEC 13F Parser")
    parser.add_argument("--mode", choices=["pilot", "live"], default="pilot")
    parser.add_argument("--ciks", nargs="+", default=["1067983", "1350694"])
    parser.add_argument("--max-filings", type=int, default=5)
    parser.add_argument("--start-year", type=int, default=None, help="Optional start year (inclusive)")
    parser.add_argument("--end-year", type=int, default=None, help="Optional end year (inclusive)")
    parser.add_argument("--out", default=str(OUT_DIR / "phase1"))
    args = parser.parse_args()

    if args.mode == "pilot":
        filers, holdings = run_pilot()
    else:
        if args.start_year is not None and args.end_year is not None:
            filers, holdings = run_live_with_year_range(
                args.ciks,
                max_filings_per_cik=args.max_filings,
                start_year=args.start_year,
                end_year=args.end_year,
            )
        else:
            filers, holdings = run_live(args.ciks, args.max_filings)

    df_filer, df_holdings = build_dataframes(filers, holdings)
    out = Path(args.out)
    save_outputs(df_filer, df_holdings, out)

    print(f"\n{'='*60}")
    print(f"  Phase 1 Complete")
    print(f"  Filer rows:   {len(df_filer)}")
    print(f"  Holding rows: {len(df_holdings)}")
    print(f"  Output dir:   {out}")
    print(f"{'='*60}\n")

    return df_filer, df_holdings


if __name__ == "__main__":
    main()
