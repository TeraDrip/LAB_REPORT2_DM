# LAB_REPORT2 - CSV to Supabase ETL

This repo contains an ETL script that:

- Reads all `.csv` files from `src/1-etl/csv-files/` (by default)
- Cleans and transforms data (auto dtype inference per column)
- Creates matching tables in Supabase Postgres (recommended, requires DB credentials)
- Uploads the cleaned rows to Supabase

## 1) Prerequisites

- Windows
- Python 3.10+ (recommended)

Dependencies are listed in `requirements.txt` (and mirrored in `lib.txt`).

## 2) Install Dependencies

From repo root:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
```

Note: You don't need to activate the venv (avoids PowerShell execution-policy issues).

## 3) Configure `.env`

Create/update `.env` in the repo root with:

```bash
# Recommended (best): full Postgres connection string from Supabase Dashboard
SUPABASE_DB_URL="postgresql://postgres:<password>@db.<project_ref>.supabase.co:5432/postgres?sslmode=require"

# Alternative: build DB URL automatically (requires SUPABASE_URL too)
SUPABASE_URL="https://<project_ref>.supabase.co"
SUPABASE_DB_PASSWORD=""

# REST fallback (only used if no DB URL/password is set). Tables must already exist.
SUPABASE_SERVICE_ROLE_KEY=""
SUPABASE_KEY=""
```

Notes:

- Do not set `SUPABASE_DB_URL` to your HTTPS project URL. A valid DB URL must start with `postgres://` or `postgresql://`.
- If you set `SUPABASE_DB_PASSWORD`, the script auto-builds a DB URL using your `SUPABASE_URL`.
- If you get a connection timeout on port `5432`, use Supabase's connection pooler port `6543` (or set `SUPABASE_DB_PORT=6543`).
- `.env` is ignored by git via `.gitignore`.

## 4) Put CSV Files Here

Place your CSV files in:

- `src/1-etl/csv-files/`

The script scans that directory recursively for `*.csv` by default.

## 5) Run The ETL

```powershell
.\.venv\Scripts\python.exe .\src\1-etl\data_etl.py
```

Optional: override the input location (file or directory):

```powershell
.\.venv\Scripts\python.exe .\src\1-etl\data_etl.py --input-path "path\to\csv-or-folder"
```

## 6) Run Full App (Frontend + Backend)

```powershell
.\.venv\Scripts\python.exe .\src\2-front-end\teradrip_salon_gui.py
```

This launcher starts `src/3-back-end/backend.py` and opens `http://localhost:5000`.

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
  - preferred: direct Postgres insert (requires `SUPABASE_DB_URL` or `SUPABASE_DB_PASSWORD`)
  - fallback: Supabase REST insert (table must already exist; may retry briefly on `PGRST205`)

## Troubleshooting

- `ValueError` about DB URL:
  - Add `SUPABASE_DB_PASSWORD` or a valid `SUPABASE_DB_URL` (postgres DSN) to `.env`.
- Insert error like `PGRST205` (schema cache):
  - You are running in REST fallback mode (no DB URL). Prefer setting `SUPABASE_DB_URL` so the ETL creates tables and inserts directly.
- Insert fails saying the table does not exist:
  - Prefer setting `SUPABASE_DB_URL` so the ETL can create the table automatically.

## 7) Iteration Evidence (3 Learning Phases)

The MBA engine runs **3 explicit learning phases** and updates outputs automatically.

| Phase | Data Slice Used | Automated Updates | Evidence Fields |
|---|---|---|---|
| 1 | Early sample (`~50%`, min 100 rows) | Initial thresholds + candidate rules | `metrics.phase`, `metrics.thresholds`, `metrics.rule_count`, `metrics.score` |
| 2 | Mid sample (`~75%`, min 150 rows) | Re-tuned thresholds + re-scored rules | `metrics.thresholds`, `metrics.loop_trace`, `metrics.target_rule_count` |
| 3 | Full dataset (`100%`) | Final rules, governance checks, recommendation payload | `payload.rules`, `governance.anomalies`, `governance.manual_review_required` |

Where this is implemented:

- `src/3-back-end/machine-learning/teradrip_ml.py`
  - `for phase in (1, 2, 3)`
  - optimization loop + scoring (`_optimization_loop`, `_score_rules`)
  - governance/stability report (`_stability_report`)

Where evidence is stored:

- Supabase output table: `ml_recommendations` (JSON fields: `payload`, `metrics`, `governance`)
- API response: `POST /api/ml/analyze` returns final run summary plus logs

## 8) Edge Case Validation

The ETL + MBA pipeline handles common failure and data-quality edge cases.

| Edge Case | Handling | Expected Behavior |
|---|---|---|
| Empty columns / empty rows | Drop all-null columns and rows | Prevents invalid inserts and noisy features |
| Duplicate records | `drop_duplicates()` | Avoids inflated support/confidence |
| Null-like strings (`"", "nan", "none", "null"`) | Normalized to null | Cleaner type inference and DB consistency |
| Mixed-type numeric columns | `pd.to_numeric(..., errors="coerce")` | Correct numeric typing when possible |
| Date/time in mixed formats | `pd.to_datetime(..., format="mixed")` | Robust datetime parsing |
| Identifier columns (`*_id`) | Forced to `varchar` | Prevents key corruption from numeric coercion |
| CSV cleans to no usable rows/columns | File is skipped safely | Pipeline continues without crash |
| DB direct insert unavailable | Fallback to REST mode | Upload can still proceed when possible |
| Missing price for recommendation items | Price shown as unavailable (`N/A`) | Recommendations remain usable with transparent gaps |

Edge-case implementation references:

- `src/1-etl/data_etl.py` (`clean_dataframe`, `infer_sql_type_and_transform`, load fallbacks)
- `src/3-back-end/backend.py` (upload diagnostics + price coverage reporting)
