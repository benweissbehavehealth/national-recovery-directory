# DuckDB UI Manual Start Instructions

Since the automated UI start is having issues, please follow these manual steps:

## Option 1: Terminal Method

1. Open a new terminal window
2. Navigate to the database directory:
   ```bash
   cd /Users/benweiss/Code/narr_extractor/04_processed_data/duckdb/databases
   ```

3. Start DuckDB:
   ```bash
   duckdb recovery_directory.duckdb
   ```

4. In the DuckDB prompt, run these commands:
   ```sql
   INSTALL ui;
   LOAD ui;
   CALL start_ui_server();
   ```

5. You should see: "UI started at http://localhost:4213/"

6. Keep this terminal window open and visit http://localhost:4213/ in your browser

## Option 2: One-liner Method

Run this single command in terminal:
```bash
cd /Users/benweiss/Code/narr_extractor/04_processed_data/duckdb/databases && echo -e "INSTALL ui;\nLOAD ui;\nCALL start_ui_server();\n.timer on\nSELECT 'UI running at http://localhost:4213/';" | duckdb recovery_directory.duckdb
```

## Troubleshooting

If port 4213 is blocked or in use:
1. Check if another process is using it: `lsof -i :4213`
2. Kill any conflicting process: `kill -9 <PID>`
3. Try a different port by modifying the start command

## What You'll See in the UI

Once running, you can explore:
- **41,097 organizations** with full data lineage
- **7 data sources** (NARR, SAMHSA, Oxford Houses, etc.)
- **9 certification authorities** (5 NARR affiliates)
- **13 organization types** in hierarchical structure

## Useful Queries for the UI

```sql
-- View all data sources
SELECT * FROM data_source_summary;

-- Check organization freshness
SELECT * FROM organization_data_freshness LIMIT 20;

-- See certification authorities
SELECT * FROM certification_authority_summary;

-- Find NARR affiliates
SELECT * FROM certification_authorities WHERE is_narr_affiliate = TRUE;
```