# LAB_REPORT2 - CSV to Supabase ETL

This repo contains an ETL script that:

- Reads all `.csv` files from `src/1-etl/csv/` (by default)
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

If you have the Python launcher installed, `py -3 -m venv .venv` also works.

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

- `src/1-etl/csv/`

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

## 9) End-to-End Technical Architecture

Pipeline flow:

1. Source -> 2. Extract -> 3. Transform and Validation -> 4. Supabase (PostgreSQL) -> 5. Machine Learning -> 6. Frontend Dashboard

### 9.1 Source Layer

Supported sources:

- CSV files (batch uploads)
- REST APIs (JSON payloads)

Input data formats:

- CSV (UTF-8, comma-delimited)
- JSON objects/arrays from APIs

Recommended ingestion metadata per batch:

- `run_id`
- `source_name`
- `source_type` (`csv` or `api`)
- `extracted_at`
- `schema_version`

### 9.2 Extraction Layer (Python + Pandas + Requests)

CSV extraction (Pandas):

- `pd.read_csv(..., low_memory=False)` to preserve consistent type behavior
- chunked reads for larger files (`chunksize`) when needed
- immediate normalization of null-like strings and column names

API extraction (Requests):

- `requests.get/post` with explicit `timeout`
- retry with exponential backoff for `429/5xx`
- pagination handling (`page`, `cursor`, or `next` token)
- flatten nested JSON into tabular records before transformation

Extraction output contract:

- tabular records (DataFrame/list of dict rows)
- metadata envelope attached to every batch

Extraction error handling:

- network/API failures: retry N times then fail task
- malformed CSV rows: quarantine to `bad_records` log
- schema drift from API: record warning and continue with known fields when safe

### 9.3 Transform and Validation Layer

Transform:

- sanitize table/column names to safe identifiers
- trim whitespace, normalize null-like strings (`""`, `nan`, `none`, `null`)
- infer and cast types (boolean, integer, float, date, timestamp, text)
- drop fully empty rows/columns and duplicates

Validation option A: Great Expectations

- non-null checks for required columns
- uniqueness checks for business keys
- range checks (for example `price >= 0`)
- allowed category checks
- row count freshness checks to detect abnormal volume shifts

Validation option B: Pydantic

- strict model schemas for row-level validation
- typed fields plus custom validators for business rules
- explicit validation error objects for failed rows

Validation result handling before load:

- PASS: write to Supabase
- WARN: write with quality warnings logged
- FAIL: stop load and mark run failed

Validation report format (JSON):

```json
{
  "run_id": "...",
  "suite": "customer_dataset_checks",
  "status": "pass|warn|fail",
  "checks": [
    {"name": "not_null_customer_id", "success": true, "failed_count": 0}
  ]
}
```

### 9.4 Supabase Storage Layer (PostgreSQL)

Recommended logical zones:

- `bronze_raw_ingestion`
- `silver_cleaned_dataset`
- `gold_feature_store`
- `ml_recommendations` / `ml_predictions`
- `pipeline_runs` / `data_quality_results`

Load pattern:

- batch insert/upsert
- idempotency via `run_id` + business key
- transaction boundaries for consistency

Storage error handling:

- unique conflicts: upsert (`ON CONFLICT`)
- transient DB errors: retry with backoff
- schema mismatch: fail load and alert

### 9.5 Machine Learning Layer (Scikit-learn or XGBoost)

Feature pull from Supabase:

- query cleaned/feature tables via SQL or Supabase client
- enforce feature schema and null policy before training/scoring

Model training/scoring options:

- Scikit-learn (classification/regression pipelines)
- XGBoost (`xgboost.XGBClassifier` / `XGBRegressor`) for boosted-tree performance

Typical ML flow:

1. fetch features from Supabase
2. split/train/evaluate
3. generate predictions/recommendations
4. push outputs back to Supabase (`ml_predictions`, `ml_recommendations`)

Prediction output format (JSON row):

```json
{
  "run_id": "...",
  "entity_id": "cust_001",
  "prediction": 0.87,
  "confidence": 0.91,
  "model_version": "xgb_v3",
  "scored_at": "2026-03-10T10:00:00Z"
}
```

ML error handling:

- missing features: hard fail with missing column list
- model drift/performance drop: flag for review, keep previous stable model
- write-back failure: retry, then persist local fallback artifact

### 9.6 Orchestration Layer (Apache Airflow or Prefect)

Purpose:

- manage dependencies, retries, schedules, and observability across the full pipeline

Reference task graph:

1. `extract_csv`
2. `extract_api`
3. `transform_data`
4. `validate_data_quality`
5. `load_supabase`
6. `build_features`
7. `train_or_score_model`
8. `write_predictions`
9. `publish_dashboard_updates`

Dependency model:

- extraction tasks can run in parallel
- validation waits for all extractions
- load waits for successful validation
- ML waits for successful load/feature build

Scheduling and retries:

- cron schedule (hourly/daily) plus manual triggers
- per-task retry policies (attempts, delay, backoff)
- timeout and SLA thresholds for long-running tasks

Orchestrator health monitoring:

- task state (`queued`, `running`, `success`, `failed`, `retry`)
- run duration, retry count, and failure reason
- freshness checks (time since last successful run)
- alerts via email/Slack/webhook for critical failures

### 9.7 Frontend Dashboard Layer (React or Next.js)

Data access patterns:

- REST APIs for historical and aggregate queries
- realtime updates from Supabase via Webhooks or Realtime subscriptions

Realtime architecture options:

- Supabase Realtime channel subscription on `INSERT/UPDATE`
- database webhook -> backend endpoint -> websocket/SSE push to frontend

Frontend payload patterns:

- KPI JSON (`records`, `rules`, `avg_confidence`, `last_updated`)
- recommendation arrays (`top_bundles`, `frequently_bought_together`, `promo_recommendations`)
- trend arrays for iteration history and model quality over time

Frontend error handling:

- fallback UI when realtime disconnects
- polling fallback if websocket/subscription fails
- stale-data indicator if freshness threshold exceeded

### 9.8 End-to-End Error Handling Matrix

- Source -> Extract: file/API read errors logged with `run_id`, retried, then failed
- Extract -> Transform: malformed rows quarantined, good rows continue when policy allows
- Transform -> Validation: expectation/model failures produce structured quality report
- Validation -> Supabase: only pass/warn data loaded, fail blocks downstream ML
- Supabase -> ML: feature schema mismatch blocks model execution
- ML -> Frontend: write-back failure prevents stale predictions from being shown as fresh

### 9.9 Recommended Operational Metrics

- ingestion row count per run
- validation pass rate and failed expectation counts
- Supabase load latency and error rate
- model quality metrics (for example AUC/F1/RMSE depending on task)
- prediction serving freshness (`now - latest_scored_at`)
- dashboard update latency (DB write to UI render)
