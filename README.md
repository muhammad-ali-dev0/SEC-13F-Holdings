# SEC 13F Holdings (2010–2012)

Research-grade extraction from SEC Form **13F-HR** / **13F-HR/A** with emphasis on **pre-XML EDGAR** (roughly 2010–2012), where holdings live in plain-text submissions rather than a standalone information-table XML file.

## Deliverables (per run)

Written to `output/phase1/`:

| File                                | Description                                                                                       |
| ----------------------------------- | ------------------------------------------------------------------------------------------------- |
| `filer_table.csv`                   | One row per filing: `cik`, `manager_name`, `filing_date`, `report_date`, `form_type`, `accession` |
| `holdings_table.csv`                | One row per position (see schema below)                                                           |
| `holdings_classified.csv`           | Same as holdings plus `security_class`, `confidence_score`, `confidence_category`, `quarter`      |
| `security_master.csv`               | CUSIP × `class_title` roll-up with aggregated classification (when holdings non-empty)            |
| `pipeline_summary_<timestamp>.json` | Run metadata (row counts, date span, etc.)                                                        |

Parquet mirrors are written if `pyarrow` is installed.

### Holdings schema (CSV columns)

`row_id`, `cik`, `report_date`, `filing_date`, `accession`, `filing_uid`, `issuer_name`, `cusip`, `class_title`, `value_1000s`, `value_usd`, `shares_or_principal`, `shares_type`, `put_call`, `investment_discretion`, `other_manager`, `voting_sole`, `voting_shared`, `voting_none`

## How to run

```bash
python -m venv .venv && source .venv/bin/activate   # optional
pip install -r requirements.txt
```

**Live pilot (recommended for the revised client sample):** ~45 managers from `cik_list.txt`, **report_date** years **2010–2012**:

```bash
python run_pipeline.py --mode pilot --ciks-file cik_list.txt \
  --pilot-start-year 2010 --pilot-end-year 2012 --max-filings 35
```

- `--max-filings` caps how many **13F-HR(/A) filings per CIK** are considered after filtering by `report_date` year (raise it if you need fuller quarter coverage).
- Respect SEC traffic limits; the parser sleeps ~0.12s between requests.

**Offline demo (no network):** 45 synthetic filers with quarters spanning **2010Q1–2012Q4**, mixing embedded XML fragments and legacy HTML table templates:

```bash
python run_pipeline.py --mode pilot_sample
```

## Pre-2013 / legacy parsing (what we implemented)

1. **Index discovery**  
   Modern filings expose a separate “Information Table” XML. Older filings often list a single **`13F-HR` `.txt`** document on the filing index page. The runner prefers an explicit information-table link, then any `*infotable*` XML/HTML, then the primary **`13F-HR` `.txt`**.

2. **Plain-text / SGML-era `.txt` (2010–2012)**
   - **“X” convention:** Many filers wrap issuer names across lines; continuation rows repeat `value` / `shares` with an `X` marker and voting footers (validated against Berkshire 2010–2011-style filings; row totals align with summary `Form 13F Information Table Value Total`).
   - **Flat one-line rows:** Other filers use a single line per holding: `… ISSUER … CLASS 9-char CUSIP value shares SH …` (validated against Bridgewater-style 2010 filings; row count matches summary entry total).
   - Filings may contain `<PAGE>` / `<TABLE>` **inside** the `.txt`; these are **not** treated as browser HTML.

3. **HTML `<TABLE>` and pipe-delimited**  
   Still supported as in earlier pilot code paths.

4. **Post-2013 XML**  
   `informationTable` / `infoTable` XML: parses **flat** and **nested** `shrsOrPrnAmt` (`sshPrnamt`) structures (fixes zero `value`/`shares` on many live extracts).

## Parsing challenges & known gaps

- **Heterogeneous legacy layouts:** Issuers changed spacing, CUSIP grouping (6+2+check vs inline 9-char), voting footers, and optional `<PAGE>` blocks. New variants may still yield `0` holdings until a template-specific rule is added.
- **Accession / document choice:** Some years use secondary submitter accession folders; we follow EDGAR’s index `13F-HR` row. Amendments and combination reports can duplicate economic positions; we do not yet de-duplicate across amended files.
- **HTML `fmt=html` with 0 rows:** Usually means the downloaded file is not a parseable table (e.g. different MIME or a redirect stub). Logs list `fmt=` for diagnosis.
- **CUSIP normalization:** Non-alphanumeric characters stripped; shortened IDs are left-padded/truncated to 9 characters for joinability — **not** full CUSIP check-digit validation.

## Assumptions

- **Pilot window** defaults to **2010–2012** `report_date` (inclusive), per revised scope.
- **`value_usd` = `value_1000s` × 1000** (SEC reports value in thousands of USD).
- **Classification** is **rule-based** (`scripts/classify_holdings.py`): options, ADR/ETF heuristics, `put_call`, and `class_title` keywords; `confidence_score` / `confidence_category` are **not** ML probabilities.
- **User-Agent:** Set a descriptive `User-Agent` in `scripts/sec_13f_parser.py` (`HEADERS`) for production use per [SEC fair access](https://www.sec.gov/os/webmaster-faq#code-support).

## File structure note (JSON vs tabular)

- **`pipeline_summary_*.json`** — **True JSON** (nested dict; safe for `jq` / `json.load`).
- **`data/interim/parse_summary.json`** (if generated by `src/parse_13f.py`) — also JSON statistics from the **embedded sample** path in `src/parse_13f.py`, not live EDGAR.
- **All `*.csv` files** — **comma-separated tables**; open in Excel / pandas / DuckDB regardless of sibling filenames.
- If any artifact was previously shared with a `.json` extension but contains CSV-style rows, **rename to `.csv`** or treat as delimiter-separated text; in this repo the **canonical** machine-readable summaries are the `pipeline_summary_*.json` files under `output/`.

## Layout

- `run_pipeline.py` — CLI entry; Phase 1 parse + classification export.
- `scripts/sec_13f_parser.py` — EDGAR fetch, format detection, XML/HTML/legacy TXT parsers.
- `scripts/classify_holdings.py` — `holdings_classified.csv` / `security_master.csv`.
- `cik_list.txt` — pilot CIK list (comment lines `#` ignored).
- `src/parse_13f.py` — Standalone **embedded XML sample** demo (not used by `run_pipeline.py`).
