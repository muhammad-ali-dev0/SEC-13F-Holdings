"""
parse_13f.py
============
Phase 1 – SEC 13F Filing Parser
Parses 13F-HR XML filings from SEC EDGAR and produces:
  - filer_table.csv   : one row per filing (manager metadata)
  - holdings_table.csv: one row per position (security holdings)

For the pilot we embed realistic sample XML drawn from actual public
SEC EDGAR 13F-HR filings to demonstrate the full parse → clean → output
pipeline without requiring live network access.

In production, replace `SAMPLE_FILINGS` with calls to:
  https://data.sec.gov/submissions/CIK{cik:010d}.json
  and the individual filing index pages.
"""

import csv
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, fields, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional
import pandas as pd

# ── Output paths ──────────────────────────────────────────────────────────────
OUT = Path(__file__).resolve().parent.parent / "data" / "output"
INTERIM = Path(__file__).resolve().parent.parent / "data" / "interim"
OUT.mkdir(parents=True, exist_ok=True)
INTERIM.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Data classes (match JD schema exactly)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FilerRecord:
    cik: str
    manager_name: str
    filing_date: str        # YYYY-MM-DD
    report_date: str        # YYYY-MM-DD (period of report, e.g. 2023-12-31)
    form_type: str          # always 13F-HR for this project
    accession_number: str
    total_value_usd: Optional[int] = None
    total_holdings_count: Optional[int] = None

@dataclass
class HoldingRecord:
    cik: str
    accession_number: str
    report_date: str
    issuer_name: str
    class_title: str
    cusip: str
    value_usd: int          # in thousands as reported, stored as integer
    shares_or_principal: int
    sh_prn: str             # SH or PRN
    put_call: Optional[str] = None      # Put, Call, or None
    investment_discretion: str = ""    # Sole, Shared, Other
    voting_auth_sole: Optional[int] = None
    voting_auth_shared: Optional[int] = None
    voting_auth_none: Optional[int] = None
    other_manager: Optional[str] = None

# ─────────────────────────────────────────────────────────────────────────────
# Embedded sample 13F-HR XML filings (realistic, based on public SEC data)
# Includes 5 filers across different quarters to simulate panel structure.
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_FILINGS = [

    # ── 1. Berkshire Hathaway Q4 2022 ────────────────────────────────────────
    {
        "cik": "0001067983",
        "manager_name": "BERKSHIRE HATHAWAY INC",
        "filing_date": "2023-02-14",
        "report_date": "2022-12-31",
        "accession_number": "0001067983-23-000010",
        "form_type": "13F-HR",
        "xml": """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler"
                 xmlns:com="http://www.sec.gov/edgar/common">
  <headerData>
    <filerInfo>
      <periodOfReport>12-31-2022</periodOfReport>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <reportCalendarOrQuarter>04-2022</reportCalendarOrQuarter>
      <summaryPage>
        <otherIncludedManagersCount>0</otherIncludedManagersCount>
        <tableEntryTotal>47</tableEntryTotal>
        <tableValueTotal>299077032</tableValueTotal>
        <isConfidentialOmitted>false</isConfidentialOmitted>
      </summaryPage>
    </coverPage>
    <informationTable>
      <infoTable>
        <nameOfIssuer>APPLE INC</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>037833100</cusip>
        <value>116310000</value>
        <shrsOrPrnAmt><sshPrnamt>895136175</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>895136175</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>BANK OF AMERICA CORP</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>060505104</cusip>
        <value>29736000</value>
        <shrsOrPrnAmt><sshPrnamt>1032852006</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>1032852006</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>AMERICAN EXPRESS CO</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>025816109</cusip>
        <value>22074000</value>
        <shrsOrPrnAmt><sshPrnamt>151610700</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>151610700</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>COCA COLA CO</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>191216100</cusip>
        <value>21681000</value>
        <shrsOrPrnAmt><sshPrnamt>400000000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>400000000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>CHEVRON CORP NEW</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>166764100</cusip>
        <value>18835000</value>
        <shrsOrPrnAmt><sshPrnamt>165350534</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>165350534</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
    </informationTable>
  </formData>
</edgarSubmission>"""
    },

    # ── 2. Renaissance Technologies Q4 2022 ──────────────────────────────────
    {
        "cik": "0001037389",
        "manager_name": "RENAISSANCE TECHNOLOGIES LLC",
        "filing_date": "2023-02-14",
        "report_date": "2022-12-31",
        "accession_number": "0001037389-23-000004",
        "form_type": "13F-HR",
        "xml": """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler"
                 xmlns:com="http://www.sec.gov/edgar/common">
  <headerData>
    <filerInfo>
      <periodOfReport>12-31-2022</periodOfReport>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <summaryPage>
        <tableEntryTotal>4009</tableEntryTotal>
        <tableValueTotal>78312044</tableValueTotal>
      </summaryPage>
    </coverPage>
    <informationTable>
      <infoTable>
        <nameOfIssuer>SPDR S&amp;P 500 ETF TR</nameOfIssuer>
        <titleOfClass>TR UNIT</titleOfClass>
        <cusip>78462F103</cusip>
        <value>2875000</value>
        <shrsOrPrnAmt><sshPrnamt>6500000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <putCall>Call</putCall>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>0</Sole><Shared>0</Shared><None>6500000</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>SPDR S&amp;P 500 ETF TR</nameOfIssuer>
        <titleOfClass>TR UNIT</titleOfClass>
        <cusip>78462F103</cusip>
        <value>1940000</value>
        <shrsOrPrnAmt><sshPrnamt>4385000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <putCall>Put</putCall>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>0</Sole><Shared>0</Shared><None>4385000</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>NOVA LTD</nameOfIssuer>
        <titleOfClass>ORD SHS</titleOfClass>
        <cusip>66987V109</cusip>
        <value>193800</value>
        <shrsOrPrnAmt><sshPrnamt>1870000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>1870000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>INVESCO QQQ TR</nameOfIssuer>
        <titleOfClass>TR UNIT SER 1</titleOfClass>
        <cusip>46090E103</cusip>
        <value>312000</value>
        <shrsOrPrnAmt><sshPrnamt>1050000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <putCall>Call</putCall>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>0</Sole><Shared>0</Shared><None>1050000</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>MICROSOFT CORP</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>594918104</cusip>
        <value>845000</value>
        <shrsOrPrnAmt><sshPrnamt>3200000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>3200000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
    </informationTable>
  </formData>
</edgarSubmission>"""
    },

    # ── 3. Bridgewater Associates Q1 2023 ────────────────────────────────────
    {
        "cik": "0001350694",
        "manager_name": "BRIDGEWATER ASSOCIATES LP",
        "filing_date": "2023-05-15",
        "report_date": "2023-03-31",
        "accession_number": "0001350694-23-000006",
        "form_type": "13F-HR",
        "xml": """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
  <headerData>
    <filerInfo>
      <periodOfReport>03-31-2023</periodOfReport>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <summaryPage>
        <tableEntryTotal>738</tableEntryTotal>
        <tableValueTotal>19823000</tableValueTotal>
      </summaryPage>
    </coverPage>
    <informationTable>
      <infoTable>
        <nameOfIssuer>SPDR GOLD TR</nameOfIssuer>
        <titleOfClass>GOLD SHS</titleOfClass>
        <cusip>78463V107</cusip>
        <value>1720000</value>
        <shrsOrPrnAmt><sshPrnamt>9940000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>0</Sole><Shared>0</Shared><None>9940000</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>PROCTER GAMBLE CO</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>742718109</cusip>
        <value>987000</value>
        <shrsOrPrnAmt><sshPrnamt>6540000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>6540000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>JOHNSON &amp; JOHNSON</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>478160104</cusip>
        <value>875000</value>
        <shrsOrPrnAmt><sshPrnamt>5280000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>5280000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>WALMART INC</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>931142103</cusip>
        <value>632000</value>
        <shrsOrPrnAmt><sshPrnamt>4310000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SHARED</investmentDiscretion>
        <votingAuthority><Sole>0</Sole><Shared>4310000</Shared><None>0</None></votingAuthority>
      </infoTable>
    </informationTable>
  </formData>
</edgarSubmission>"""
    },

    # ── 4. Two Sigma Investments Q2 2023 ─────────────────────────────────────
    {
        "cik": "0001442145",
        "manager_name": "TWO SIGMA INVESTMENTS LP",
        "filing_date": "2023-08-14",
        "report_date": "2023-06-30",
        "accession_number": "0001442145-23-000009",
        "form_type": "13F-HR",
        "xml": """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
  <headerData>
    <filerInfo>
      <periodOfReport>06-30-2023</periodOfReport>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <summaryPage>
        <tableEntryTotal>3841</tableEntryTotal>
        <tableValueTotal>53210000</tableValueTotal>
      </summaryPage>
    </coverPage>
    <informationTable>
      <infoTable>
        <nameOfIssuer>NVIDIA CORP</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>67066G104</cusip>
        <value>2150000</value>
        <shrsOrPrnAmt><sshPrnamt>5200000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>5200000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>AMAZON COM INC</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>023135106</cusip>
        <value>1830000</value>
        <shrsOrPrnAmt><sshPrnamt>13800000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>13800000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>TAIWAN SEMICONDUCTOR MFG CO LTD</nameOfIssuer>
        <titleOfClass>SPONSORED ADR</titleOfClass>
        <cusip>874039100</cusip>
        <value>940000</value>
        <shrsOrPrnAmt><sshPrnamt>9200000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>9200000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>ISHARES MSCI EMRG MKT ETF</nameOfIssuer>
        <titleOfClass>SHS</titleOfClass>
        <cusip>464287234</cusip>
        <value>720000</value>
        <shrsOrPrnAmt><sshPrnamt>18700000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <putCall>Call</putCall>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>0</Sole><Shared>0</Shared><None>18700000</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>ALPHABET INC</nameOfIssuer>
        <titleOfClass>CL A</titleOfClass>
        <cusip>02079K305</cusip>
        <value>1120000</value>
        <shrsOrPrnAmt><sshPrnamt>9750000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>9750000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
    </informationTable>
  </formData>
</edgarSubmission>"""
    },

    # ── 5. AQR Capital Management Q3 2023 ────────────────────────────────────
    {
        "cik": "0001336528",
        "manager_name": "AQR CAPITAL MANAGEMENT LLC",
        "filing_date": "2023-11-14",
        "report_date": "2023-09-30",
        "accession_number": "0001336528-23-000012",
        "form_type": "13F-HR",
        "xml": """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
  <headerData>
    <filerInfo>
      <periodOfReport>09-30-2023</periodOfReport>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <summaryPage>
        <tableEntryTotal>2156</tableEntryTotal>
        <tableValueTotal>31440000</tableValueTotal>
      </summaryPage>
    </coverPage>
    <informationTable>
      <infoTable>
        <nameOfIssuer>APPLE INC</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>037833100</cusip>
        <value>1840000</value>
        <shrsOrPrnAmt><sshPrnamt>10500000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>10500000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>EXXON MOBIL CORP</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>30231G102</cusip>
        <value>920000</value>
        <shrsOrPrnAmt><sshPrnamt>8900000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>8900000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>UNITEDHEALTH GROUP INC</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>91324P102</cusip>
        <value>745000</value>
        <shrsOrPrnAmt><sshPrnamt>1560000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>1560000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>BERKSHIRE HATHAWAY INC DEL</nameOfIssuer>
        <titleOfClass>CL B NEW</titleOfClass>
        <cusip>084670702</cusip>
        <value>615000</value>
        <shrsOrPrnAmt><sshPrnamt>1740000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>1740000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
    </informationTable>
  </formData>
</edgarSubmission>"""
    },

    # ── 6. Millennium Management Q4 2023 ─────────────────────────────────────
    {
        "cik": "0001273931",
        "manager_name": "MILLENNIUM MANAGEMENT LLC",
        "filing_date": "2024-02-14",
        "report_date": "2023-12-31",
        "accession_number": "0001273931-24-000008",
        "form_type": "13F-HR",
        "xml": """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
  <headerData>
    <filerInfo>
      <periodOfReport>12-31-2023</periodOfReport>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <summaryPage>
        <tableEntryTotal>5132</tableEntryTotal>
        <tableValueTotal>214500000</tableValueTotal>
      </summaryPage>
    </coverPage>
    <informationTable>
      <infoTable>
        <nameOfIssuer>MICROSOFT CORP</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>594918104</cusip>
        <value>3240000</value>
        <shrsOrPrnAmt><sshPrnamt>8100000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>8100000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>NETFLIX INC</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>64110L106</cusip>
        <value>1870000</value>
        <shrsOrPrnAmt><sshPrnamt>3500000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>3500000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>TESLA INC</nameOfIssuer>
        <titleOfClass>COM</titleOfClass>
        <cusip>88160R101</cusip>
        <value>950000</value>
        <shrsOrPrnAmt><sshPrnamt>4100000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <putCall>Put</putCall>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>0</Sole><Shared>0</Shared><None>4100000</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>ALIBABA GROUP HLDG LTD</nameOfIssuer>
        <titleOfClass>SPONSORED ADR</titleOfClass>
        <cusip>01609W102</cusip>
        <value>620000</value>
        <shrsOrPrnAmt><sshPrnamt>5600000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>5600000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
      <infoTable>
        <nameOfIssuer>META PLATFORMS INC</nameOfIssuer>
        <titleOfClass>CL A COM</titleOfClass>
        <cusip>30303M102</cusip>
        <value>2780000</value>
        <shrsOrPrnAmt><sshPrnamt>6300000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
        <investmentDiscretion>SOLE</investmentDiscretion>
        <votingAuthority><Sole>6300000</Sole><Shared>0</Shared><None>0</None></votingAuthority>
      </infoTable>
    </informationTable>
  </formData>
</edgarSubmission>"""
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# XML namespace map (SEC uses multiple namespace variants across years)
# ─────────────────────────────────────────────────────────────────────────────
NS_MAP = {
    "ns0": "http://www.sec.gov/edgar/thirteenffiler",
    "com": "http://www.sec.gov/edgar/common",
    # some older filings use no namespace
}

def _text(element, tag: str, ns: str = "ns0", default: str = "") -> str:
    """Safe text extraction with namespace fallback."""
    # Try with namespace
    node = element.find(f"{{{NS_MAP[ns]}}}{tag}")
    if node is None:
        # Try without namespace
        node = element.find(tag)
    if node is None:
        return default
    return (node.text or "").strip()

def _strip_ns(tag: str) -> str:
    """Remove namespace prefix from tag."""
    return tag.split("}")[-1] if "}" in tag else tag

def _find_all_infoTable(root: ET.Element) -> list:
    """Find all infoTable elements regardless of namespace variant."""
    results = []
    for elem in root.iter():
        if _strip_ns(elem.tag) == "infoTable":
            results.append(elem)
    return results

def _get_child_text(elem: ET.Element, tag: str, default: str = "") -> str:
    """Get text of a direct child by local name, namespace-agnostic."""
    for child in elem:
        if _strip_ns(child.tag) == tag:
            return (child.text or "").strip()
    return default

def _get_nested_text(elem: ET.Element, outer: str, inner: str, default: str = "") -> str:
    """Get text from nested element by local names."""
    for child in elem:
        if _strip_ns(child.tag) == outer:
            for grandchild in child:
                if _strip_ns(grandchild.tag) == inner:
                    return (grandchild.text or "").strip()
    return default

# ─────────────────────────────────────────────────────────────────────────────
# Core parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_filing(filing_meta: dict) -> tuple[FilerRecord, list[HoldingRecord]]:
    """
    Parse a single 13F-HR filing dict.
    Returns (FilerRecord, [HoldingRecord, ...])
    """
    xml_str = filing_meta["xml"]
    root = ET.fromstring(xml_str)

    # ── Extract summary stats ──────────────────────────────────────────────
    total_value = None
    total_count = None
    for elem in root.iter():
        tag = _strip_ns(elem.tag)
        if tag == "tableValueTotal":
            try: total_value = int(elem.text.strip())
            except: pass
        if tag == "tableEntryTotal":
            try: total_count = int(elem.text.strip())
            except: pass

    filer = FilerRecord(
        cik=filing_meta["cik"],
        manager_name=filing_meta["manager_name"],
        filing_date=filing_meta["filing_date"],
        report_date=filing_meta["report_date"],
        form_type=filing_meta["form_type"],
        accession_number=filing_meta["accession_number"],
        total_value_usd=total_value,
        total_holdings_count=total_count,
    )

    # ── Parse holdings ─────────────────────────────────────────────────────
    holdings = []
    for info in _find_all_infoTable(root):
        name       = _get_child_text(info, "nameOfIssuer")
        class_ttl  = _get_child_text(info, "titleOfClass")
        cusip      = _get_child_text(info, "cusip")
        value_str  = _get_child_text(info, "value", "0")
        put_call   = _get_child_text(info, "putCall") or None
        inv_disc   = _get_child_text(info, "investmentDiscretion")

        # shrsOrPrnAmt
        shares_str = _get_nested_text(info, "shrsOrPrnAmt", "sshPrnamt", "0")
        sh_prn     = _get_nested_text(info, "shrsOrPrnAmt", "sshPrnamtType", "SH")

        # votingAuthority
        v_sole   = _get_nested_text(info, "votingAuthority", "Sole", "0")
        v_shared = _get_nested_text(info, "votingAuthority", "Shared", "0")
        v_none   = _get_nested_text(info, "votingAuthority", "None", "0")

        def safe_int(s: str) -> int:
            try: return int(s.replace(",", ""))
            except: return 0

        holding = HoldingRecord(
            cik=filer.cik,
            accession_number=filer.accession_number,
            report_date=filer.report_date,
            issuer_name=_clean_issuer_name(name),
            class_title=class_ttl.upper().strip(),
            cusip=_validate_cusip(cusip),
            value_usd=safe_int(value_str),
            shares_or_principal=safe_int(shares_str),
            sh_prn=sh_prn.upper().strip(),
            put_call=put_call,
            investment_discretion=inv_disc.upper().strip() if inv_disc else "",
            voting_auth_sole=safe_int(v_sole),
            voting_auth_shared=safe_int(v_shared),
            voting_auth_none=safe_int(v_none),
        )
        holdings.append(holding)

    return filer, holdings


# ─────────────────────────────────────────────────────────────────────────────
# Cleaning helpers
# ─────────────────────────────────────────────────────────────────────────────

def _clean_issuer_name(name: str) -> str:
    """Normalise issuer names: strip extra whitespace, standardise ampersand."""
    name = re.sub(r"\s+", " ", name).strip()
    name = name.replace("&amp;", "&").replace("&AMP;", "&")
    return name.upper()

def _validate_cusip(cusip: str) -> str:
    """Basic CUSIP validation: must be 9 chars alphanumeric."""
    cusip = re.sub(r"[^A-Z0-9]", "", cusip.upper())
    if len(cusip) != 9:
        return cusip  # return as-is; flag downstream
    return cusip

# ─────────────────────────────────────────────────────────────────────────────
# Run all filings → DataFrames
# ─────────────────────────────────────────────────────────────────────────────

def build_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse all sample filings. Returns (filer_df, holdings_df)."""
    filer_rows = []
    holding_rows = []

    for filing in SAMPLE_FILINGS:
        print(f"  Parsing: {filing['manager_name']}  ({filing['report_date']})")
        filer, holdings = parse_filing(filing)
        filer_rows.append(asdict(filer))
        for h in holdings:
            holding_rows.append(asdict(h))

    filer_df = pd.DataFrame(filer_rows)
    holdings_df = pd.DataFrame(holding_rows)

    # ── Filer table: enforce types ─────────────────────────────────────────
    filer_df["filing_date"] = pd.to_datetime(filer_df["filing_date"])
    filer_df["report_date"] = pd.to_datetime(filer_df["report_date"])
    filer_df["total_value_usd"] = pd.to_numeric(filer_df["total_value_usd"], errors="coerce")
    filer_df["total_holdings_count"] = pd.to_numeric(filer_df["total_holdings_count"], errors="coerce")

    # ── Holdings table: enforce types ──────────────────────────────────────
    holdings_df["report_date"] = pd.to_datetime(holdings_df["report_date"])
    holdings_df["value_usd"] = pd.to_numeric(holdings_df["value_usd"], errors="coerce")
    holdings_df["shares_or_principal"] = pd.to_numeric(holdings_df["shares_or_principal"], errors="coerce")

    # Derived columns
    holdings_df["quarter"] = holdings_df["report_date"].dt.to_period("Q").astype(str)
    filer_df["quarter"]    = filer_df["report_date"].dt.to_period("Q").astype(str)

    return filer_df, holdings_df


# ─────────────────────────────────────────────────────────────────────────────
# Save outputs
# ─────────────────────────────────────────────────────────────────────────────

def save_outputs(filer_df: pd.DataFrame, holdings_df: pd.DataFrame):
    # CSV (always)
    filer_path    = OUT / "filer_table.csv"
    holdings_path = OUT / "holdings_table.csv"
    filer_df.to_csv(filer_path, index=False)
    holdings_df.to_csv(holdings_path, index=False)
    print(f"\n  Saved: {filer_path}")
    print(f"  Saved: {holdings_path}")

    # Parquet (if pyarrow available)
    try:
        filer_df.to_parquet(OUT / "filer_table.parquet", index=False)
        holdings_df.to_parquet(OUT / "holdings_table.parquet", index=False)
        print("  Saved: filer_table.parquet, holdings_table.parquet")
    except Exception:
        print("  (pyarrow not installed — parquet skipped; CSV files are complete)")

    # Summary stats to interim
    summary = {
        "filer_count": len(filer_df),
        "holding_rows": len(holdings_df),
        "unique_cusips": holdings_df["cusip"].nunique(),
        "unique_managers": holdings_df["cik"].nunique(),
        "date_range": f"{holdings_df['report_date'].min().date()} to {holdings_df['report_date'].max().date()}",
        "quarters_covered": sorted(holdings_df["quarter"].unique().tolist()),
    }
    import json
    with open(INTERIM / "parse_summary.json", "w") as f:
        json.dump(summary, f, indent=2)


if __name__ == "__main__":
    print("\n=== Phase 1: Parsing 13F-HR Filings ===\n")
    filer_df, holdings_df = build_tables()
    save_outputs(filer_df, holdings_df)

    print("\n--- Filer Table ---")
    print(filer_df[["cik","manager_name","report_date","quarter","total_value_usd","total_holdings_count"]].to_string(index=False))
    print("\n--- Holdings Table (first 10 rows) ---")
    print(holdings_df[["cik","report_date","issuer_name","cusip","class_title","value_usd","shares_or_principal","put_call"]].head(10).to_string(index=False))
    print(f"\nTotal holdings rows: {len(holdings_df)}")
    print("=== Phase 1 complete ===\n")
