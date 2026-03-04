# LAB_REPORT2 - CSV to Supabase ETL

This repo contains an ETL script that:

- Reads all `.csv` files from `src/1-etl/csv-files/` (by default)
- Cleans and transforms data (auto dtype inference per column)
- Creates matching tables in Supabase Postgres (optional, requires DB credentials)
- Uploads the cleaned rows to Supabase

## 1) Prerequisites

- Windows
- Python interpreter (either):
  - Project virtualenv at `.venv` (recommended), or
  - Anaconda Python

Dependencies are listed in `lib.txt`.

## 2) Install Dependencies

### Option A: Use `.venv` (recommended)

From repo root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r .\lib.txt
```

### Option B: Use Anaconda Python

From repo root:

```powershell
C:/Users/Jed/anaconda3/python.exe -m pip install -r .\lib.txt
```

Important: packages must be installed into the same interpreter you use to run the script.

## 3) Configure `.env`

Create/update `.env` in the repo root with:

```bash
SUPABASE_URL="https://<project_ref>.supabase.co"

# Preferred for ETL (bypasses RLS issues). If you only have SUPABASE_KEY, keep using it.
SUPABASE_SERVICE_ROLE_KEY=""
SUPABASE_KEY=""

# For automatic CREATE TABLE in Supabase Postgres:
# Provide either the DB password (recommended) OR a full Postgres connection string.
SUPABASE_DB_PASSWORD=""
SUPABASE_DB_URL=""
```

Notes:

- Do not set `SUPABASE_DB_URL` to your HTTPS project URL. A valid DB URL must start with `postgres://` or `postgresql://`.
- If you set `SUPABASE_DB_PASSWORD`, the script auto-builds:
  - `postgresql://postgres:<password>@db.<project_ref>.supabase.co:5432/postgres`

## 4) Put CSV Files Here

Place your CSV files in:

- `src/1-etl/csv-files/`

The script scans that directory recursively for `*.csv` by default.

## 5) Run The ETL

### Run with `.venv`

```powershell
.\.venv\Scripts\Activate.ps1
python .\src\1-etl\data_etl.py
```

### Run with Anaconda

```powershell
C:/Users/Jed/anaconda3/python.exe .\src\1-etl\data_etl.py
```

Optional: override the input location (file or directory):

```powershell
python .\src\1-etl\data_etl.py --input-path "path\to\csv-or-folder"
```

## What The Script Does

File: `src/1-etl/data_etl.py`

- Table name: derived from CSV filename (sanitized to `snake_case`)
- Column names: sanitized to safe identifiers
- Cleaning:
  - trims whitespace for string-like columns
  - normalizes empty/null-like strings to null
  - drops fully empty rows/columns
  - de-duplicates rows
- Transformation / type inference:
  - `boolean`, `bigint`, `double precision`, `date`, `timestamptz`, `text`
  - Columns with `id` in the column name are forced to `varchar` (string) for key compatibility
- Loading:
  - inserts in batches
  - retries briefly if Supabase schema cache is not ready right after table creation

## Troubleshooting

- `ValueError` about DB URL:
  - Add `SUPABASE_DB_PASSWORD` or a valid `SUPABASE_DB_URL` (postgres DSN) to `.env`.
- Insert error like `PGRST205` (schema cache):
  - The script retries automatically; re-run if needed.
- Insert fails saying the table does not exist:
  - You are likely running without DB credentials (table creation disabled), or RLS is blocking inserts. Use `SUPABASE_SERVICE_ROLE_KEY` and set DB credentials.
