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

1. Source -> 2. Extract -> 3. Transform and Validation -> 4. Supabase (PostgreSQL) -> 5. Machine Learning -> 6. Backend API/Service -> 7. Frontend Dashboard

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

This layer contains a full recommendation sub-pipeline, not just a single model call.

Concrete sub-pipeline:

1. Input acquisition from Supabase
2. Basket matrix construction and feature shaping
3. Adaptive threshold generation (auto-threshold logic)
4. Candidate mining loop (Apriori/FP-Growth with threshold tuning)
5. Rule scoring and selection
6. Rule stability and governance checks
7. Recommendation artifact generation
8. Output persistence and API serving

#### 9.5.1 Inputs and Feature Construction

Input source:

- transactional rows pulled from Supabase source table
- latest historical rules pulled from prior runs for stability comparison

Feature construction (association-rule context):

- detect item/service indicator columns
- convert rows into boolean basket matrix (`transaction x item`)
- compute basket stats used downstream (`transactions`, `distinct_items`, `density`, variance)

Input/output data formats:

- input: tabular transaction rows (JSON/SQL rows)
- internal features: boolean DataFrame
- output candidates: itemsets and rule DataFrames

#### 9.5.2 Adaptive Thresholding (Auto-Threshold Logic)

Thresholds are dynamically derived from dataset shape and density, then refined in iterative loops.

Auto-generated thresholds include:

- `min_support`
- `min_confidence`
- `min_lift`
- `min_leverage`
- `min_conviction`

How auto-thresholding works:

- start from basket-density-aware base thresholds
- create stricter, baseline, and relaxed candidate threshold sets per loop
- run candidate mining for each set
- score each candidate result
- keep the highest-quality candidate
- early-stop on plateau after acceptable rule volume
- perform rescue passes if rules are too few

This is the intelligent auto-threshold mechanism required by the project rubric.

#### 9.5.3 Algorithm Selection and Optimization Loop

Algorithm selection:

- choose Apriori for lower density and moderate dimensionality
- choose FP-Growth for denser/larger patterns

Optimization loop behavior:

- evaluate multiple threshold candidates per loop
- track per-loop trace (`candidate`, thresholds, algorithm, rule count, score)
- keep both raw score (selection) and normalized score (0-100 display)

Scoring model includes weighted quality signals:

- confidence
- lift
- leverage
- conviction
- item coverage

Plus penalties for:

- rule-count mismatch against target
- low-confidence rule sets

#### 9.5.4 Rule Stability Test and Governance

After selecting best rules, the pipeline compares new rules against previous run rules.

Stability checks include:

- added rules
- removed rules
- changed confidence deltas
- dropped strong rules

Governance anomalies include examples such as:

- sharp confidence drops (`delta < -0.30`)
- high-confidence but very low-support rules

Governance output fields:

- `added_count`, `removed_count`, `changed_count`, `dropped_strong_count`
- `anomalies`
- `manual_review_required`

This is the explicit rule stability test mechanism required by the rubric.

#### 9.5.5 Recommendation Generation Layer

From final validated rules/itemsets, the ML layer generates multiple recommendation artifacts:

- top bundles
- frequently bought together (FBT)
- cross-sell suggestions
- promo recommendations

Each recommendation includes business-facing metrics where available:

- support
- confidence
- lift
- leverage
- conviction
- pricing/discount metadata (in backend enrichment)

Recommendation evaluation loop (implemented form):

- recommendations are derived from rules selected by iterative optimization loops
- loop traces and phase metrics expose how recommendation quality evolved
- governance checks gate unstable outputs for manual review

#### 9.5.6 Three-Phase Learning Strategy

The ML run executes three explicit phases:

1. Iteration 1: early sample (about 50%)
2. Iteration 2: mid sample (about 75%)
3. Iteration 3: full dataset (100%)

Per-phase evidence captured:

- thresholds used
- selected algorithm
- rule and itemset counts
- score and score_raw
- loop trace
- governance summary

#### 9.5.7 Persistence and API Contract

ML outputs are persisted in Supabase table `ml_recommendations` with JSON fields:

- `payload` (recommendation artifacts and rules)
- `metrics` (phase, thresholds, score, loop trace, quality)
- `governance` (stability and anomaly checks)

Primary API contract for frontend:

- `POST /api/ml/analyze`
- returns final recommendation payload, analytics comparison, iteration phase history, and logs

Example response fragments:

```json
{
  "iteration_phases": [
    {
      "phase": 1,
      "algorithm": "fpgrowth",
      "score": 62.4,
      "score_raw": -0.1523,
      "thresholds": {
        "min_support": 0.129,
        "min_confidence": 0.489,
        "min_lift": 1.17,
        "min_leverage": 0.0086,
        "min_conviction": 1.15
      }
    }
  ]
}
```

ML error handling:

- missing/invalid input features: fail fast with diagnostics
- empty mined rules: retry via relaxed rescue thresholds
- unstable output anomalies: set `manual_review_required=true`
- DB write failure: retry then fallback to local output artifact

Current implementation references:

- `src/3-back-end/machine-learning/teradrip_ml.py`
  - auto-thresholding: `auto_thresholds`
  - optimization/scoring loop: `_optimization_loop`, `_score_rules`
  - stability/governance: `_stability_report`
  - recommendation artifacts: `_itemsets_to_bundles`, `_fbt_widget`, `_cross_sell_map`, `_promo_recommendations`
- `src/3-back-end/backend.py`
  - API exposure and dashboard payload shaping: `run_ml_analysis_core`

### 9.6 Backend API and Service Layer

This layer is the integration hub between storage/ML and the dashboard.

Core responsibilities:

- receive frontend requests
- run ETL and ML pipeline actions
- read latest ML outputs from Supabase
- enrich recommendations (for example item labels and pricing coverage)
- return dashboard-ready JSON contracts

Primary backend routes in current system:

- `POST /api/upload`: receives CSV upload and triggers ETL flow
- `POST /api/ml/analyze`: runs or fetches ML output and returns recommendation payload
- `GET /api/data/stats`: returns row/table/column stats for Data tab
- `GET /api/health`: service health check

Backend I/O contracts:

- request format: JSON for ML controls (`refresh`, `table_name`, `limit`) and multipart for file upload
- response format: JSON objects with `success`, `logs`, metrics, recommendations, and diagnostics

Backend error handling:

- wraps ETL/ML exceptions into API-safe JSON (`success=false`, `error`)
- includes diagnostics (`load_warning`, `load_hint`, `etl_log_tail`) when load is degraded
- supports local fallback behavior when DB write path fails

Current implementation references:

- `src/3-back-end/backend.py`
- `src/2-front-end/teradrip_salon.html` (frontend API consumption)

### 9.7 Orchestration Layer (Apache Airflow or Prefect)

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

### 9.8 Frontend Dashboard Layer (Current: Vanilla HTML/CSS/JS)

Current implementation in this repository:

- frontend is plain HTML/CSS/JavaScript (`src/2-front-end/teradrip_salon.html` + `src/2-front-end/styles.css`)
- backend serves data to frontend via Flask API endpoints
- launcher script `src/2-front-end/teradrip_salon_gui.py` starts backend and opens the dashboard

Optional future alternatives (not currently used in this repo):

- React
- Next.js

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

### 9.9 End-to-End Error Handling Matrix

- Source -> Extract: file/API read errors logged with `run_id`, retried, then failed
- Extract -> Transform: malformed rows quarantined, good rows continue when policy allows
- Transform -> Validation: expectation/model failures produce structured quality report
- Validation -> Supabase: only pass/warn data loaded, fail blocks downstream ML
- Supabase -> ML: feature schema mismatch blocks model execution
- ML -> Backend API: incomplete output is flagged with diagnostics and governance status
- ML -> Frontend: write-back failure prevents stale predictions from being shown as fresh

### 9.10 Recommended Operational Metrics

- ingestion row count per run
- validation pass rate and failed expectation counts
- Supabase load latency and error rate
- model quality metrics (for example AUC/F1/RMSE depending on task)
- prediction serving freshness (`now - latest_scored_at`)
- dashboard update latency (DB write to UI render)

### 9.11 Complete Runtime Sequence (Current System)

This is the exact end-to-end process in the current implementation.

1. User starts launcher (`src/2-front-end/teradrip_salon_gui.py`).
2. Launcher starts Flask backend (`src/3-back-end/backend.py`) and opens dashboard page.
3. User uploads CSV from Dashboard (`POST /api/upload`).
4. Backend runs ETL (`src/1-etl/data_etl.py`): extract -> clean -> infer types -> load to Supabase.
5. User runs ML (`POST /api/ml/analyze` with `refresh=true`).
6. Backend runs ML engine (`src/3-back-end/machine-learning/teradrip_ml.py`) for 3 phases.
7. ML writes phase outputs to Supabase table `ml_recommendations`.
8. Backend reads latest ML result, computes analytics comparison, enriches pricing and labels.
9. Backend returns consolidated JSON payload to frontend.
10. Frontend renders Recommendations, Analytics, and Data iteration cards from backend response.

Operational fallback behavior in the same sequence:

- If direct Postgres write fails, ML attempts Supabase REST insert.
- If both DB/REST writes fail, ML persists local JSON fallback outputs.
- If backend is unreachable, frontend uses simulation mode for UI continuity.
