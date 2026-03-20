# Internal Notes (Phase 1 only)

**Revised pilot (2010–2012, ~45 filers):**

```bash
python run_pipeline.py --mode pilot --ciks-file cik_list.txt \
  --pilot-start-year 2010 --pilot-end-year 2012 --max-filings 35
```

Offline smoke test (embedded legacy + XML, 2010–2012 quarters):

```bash
python run_pipeline.py --mode pilot_sample
```

Outputs under `output/phase1/`: `filer_table.csv`, `holdings_table.csv`, `holdings_classified.csv`, optional `security_master.csv`.

See root **`README.md`** for legacy parsing details, assumptions, and JSON vs CSV clarification.

