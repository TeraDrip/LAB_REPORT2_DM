"""
TeraDrip Salon Backend API
Integrates the ETL pipeline with the front-end interface.
"""
import os
import sys
import re
import json
import math
import subprocess
import threading
import warnings
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import datetime
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import pandas as pd
import psycopg
from werkzeug.utils import secure_filename

# Add paths for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ETL_DIR = PROJECT_ROOT / "src" / "1-etl"
ETL_CSV_DIR = ETL_DIR / "csv"
ML_DIR = Path(__file__).resolve().parent / "machine-learning"

sys.path.insert(0, str(ETL_DIR))
sys.path.insert(0, str(ML_DIR))

from dotenv import load_dotenv
from teradrip_ml import TeraDripMBAEngine
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

# Initialize Flask app
app = Flask(__name__, static_folder=str(PROJECT_ROOT / "src" / "2-front-end"))
CORS(app)

# Global state for pipeline status
pipeline_status = {
    "step": 0,
    "message": "",
    "rows_processed": 0,
    "columns": [],
    "error": None,
    "start_time": None,
    "end_time": None
}

# Tracks the most recent dataset table produced by /api/upload.
latest_loaded_table: str | None = None
latest_upload_time: str | None = None
pending_job: Dict[str, Any] | None = None

RUNTIME_STATE_PATH = PROJECT_ROOT / "src" / "3-back-end" / "runtime_state.json"
_state_lock = threading.Lock()
_resume_thread: threading.Thread | None = None


def persist_runtime_state() -> None:
    """Persist runtime state so interrupted work can resume after restart."""
    payload = {
        "pipeline_status": pipeline_status,
        "latest_loaded_table": latest_loaded_table,
        "latest_upload_time": latest_upload_time,
        "pending_job": pending_job,
        "saved_at": datetime.now().isoformat(),
    }
    try:
        RUNTIME_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = RUNTIME_STATE_PATH.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(RUNTIME_STATE_PATH)
    except Exception as exc:
        print(f"[WARN] Could not persist runtime state: {exc}")


def load_runtime_state() -> None:
    """Load persisted runtime state."""
    global pipeline_status, latest_loaded_table, latest_upload_time, pending_job
    if not RUNTIME_STATE_PATH.exists():
        return
    try:
        raw = json.loads(RUNTIME_STATE_PATH.read_text(encoding="utf-8"))
        if isinstance(raw.get("pipeline_status"), dict):
            pipeline_status.update(raw["pipeline_status"])
        latest_loaded_table = raw.get("latest_loaded_table")
        latest_upload_time = raw.get("latest_upload_time")
        pending = raw.get("pending_job")
        pending_job = pending if isinstance(pending, dict) else None
    except Exception as exc:
        print(f"[WARN] Could not load runtime state: {exc}")


def set_pending_job(job: Dict[str, Any] | None) -> None:
    """Update pending job and persist runtime state."""
    global pending_job
    with _state_lock:
        pending_job = job
        persist_runtime_state()


# ML Results History Management
ML_HISTORY_PATH = PROJECT_ROOT / "src" / "3-back-end" / "ml_results_history.json"
ML_HISTORY_MAX_ENTRIES = 50  # Keep last 50 runs


def load_ml_history() -> List[Dict[str, Any]]:
    """Load ML results history from file."""
    if not ML_HISTORY_PATH.exists():
        return []
    try:
        history = json.loads(ML_HISTORY_PATH.read_text(encoding="utf-8"))
        # Ensure history is a list
        if not isinstance(history, list):
            return []
        return history
    except Exception as exc:
        print(f"[WARN] Could not load ML history: {exc}")
        return []



def save_ml_result_to_history(result: Dict[str, Any], table_name: str) -> None:
    """Save an ML result to history with metadata."""
    try:
        history = load_ml_history()
        print(f"[DEBUG] Current history has {len(history)} entries before saving new run")
        
        # Create history entry
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        entry = {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "table_name": table_name,
            "result": result,
            "csv_filename": table_name.replace("table_", "").replace("_", " ").title() if table_name else "Unknown"
        }
        
        # Check if this run_id already exists (avoid duplicates)
        existing_run_ids = {e.get("run_id") for e in history}
        if run_id in existing_run_ids:
            print(f"[INFO] Run {run_id} already exists in history, skipping duplicate")
            return
        
        # Prepend new entry
        history.insert(0, entry)
        print(f"[DEBUG] Added new entry, history now has {len(history)} entries")
        
        # Keep only the most recent entries
        history = history[:ML_HISTORY_MAX_ENTRIES]
        
        # Save to file
        ML_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
        ML_HISTORY_PATH.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[INFO] ✓ Saved ML run {run_id} to history file (total: {len(history)} runs)")
        print(f"[INFO] History file location: {ML_HISTORY_PATH}")
    except Exception as exc:
        print(f"[ERROR] Could not save ML result to history: {exc}")
        import traceback
        traceback.print_exc()


def sanitize_identifier(raw_name: str, fallback: str = "column") -> str:
    """Sanitize column names for database compatibility."""
    value = re.sub(r"[^a-zA-Z0-9_]+", "_", str(raw_name).strip().lower())
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        value = fallback
    if value[0].isdigit():
        value = f"{fallback}_{value}"
    return value


def is_likely_datetime_series(series: pd.Series) -> bool:
    """Check if a series contains datetime-like values."""
    non_null = series.dropna()
    if non_null.empty:
        return False
    text = non_null.astype(str).str.strip().str.lower()
    has_date_tokens = text.str.contains(
        r"[-/:]|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|am|pm",
        regex=True,
        na=False,
    )
    return has_date_tokens.mean() >= 0.6


def is_identifier_column(column_name: str) -> bool:
    """Check if column is an identifier."""
    normalized = sanitize_identifier(column_name, "column")
    return re.search(r"(^|_)id($|_)", normalized) is not None


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize a dataframe."""
    if df.empty:
        return df

    cleaned = df.copy()
    cleaned.columns = [sanitize_identifier(col, "column") for col in cleaned.columns]
    cleaned = cleaned.loc[:, ~cleaned.columns.duplicated()]

    for col in cleaned.columns:
        if cleaned[col].dtype == object:
            cleaned[col] = cleaned[col].astype(str).str.strip()
            cleaned[col] = cleaned[col].replace({"": None, "nan": None, "none": None, "null": None})

    cleaned = cleaned.dropna(axis=1, how="all")
    cleaned = cleaned.dropna(axis=0, how="all")
    cleaned = cleaned.drop_duplicates().reset_index(drop=True)
    return cleaned


def infer_sql_type_and_transform(column_name: str, series: pd.Series) -> Tuple[str, pd.Series]:
    """Infer SQL type and transform series accordingly."""
    non_null = series.dropna()
    if non_null.empty:
        return "text", series.astype("string")

    if is_identifier_column(column_name):
        return "varchar", series.astype("string")

    as_str = non_null.astype(str).str.strip().str.lower()
    bool_values = {"true", "false", "1", "0", "yes", "no", "y", "n", "t", "f"}
    if set(as_str.unique()).issubset(bool_values):
        bool_map = {
            "true": True, "1": True, "yes": True, "y": True, "t": True,
            "false": False, "0": False, "no": False, "n": False, "f": False,
        }
        transformed = series.astype(str).str.strip().str.lower().map(bool_map)
        transformed = transformed.where(~series.isna(), None)
        return "boolean", transformed

    numeric = pd.to_numeric(series, errors="coerce")
    numeric_match = numeric.notna().sum() / len(non_null)
    if numeric_match == 1.0:
        int_like = ((numeric.dropna() % 1) == 0).all()
        if int_like:
            return "bigint", numeric.astype("Int64")
        return "double precision", numeric.astype("float64")

    if is_likely_datetime_series(series):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Could not infer format", category=UserWarning)
            datetime_series = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
        dt_match = datetime_series.notna().sum() / len(non_null)
        if dt_match >= 0.9:
            non_null_dt = datetime_series.dropna()
            is_date_only = (
                (non_null_dt.dt.hour == 0) & (non_null_dt.dt.minute == 0) & (non_null_dt.dt.second == 0)
            ).all()
            if is_date_only:
                return "date", datetime_series.dt.date
            return "timestamptz", datetime_series

    return "text", series.astype("string")


def resolve_db_url() -> str | None:
    """Resolve PostgreSQL database URL for direct connection."""
    def sanitize_postgres_dsn(raw_dsn: str) -> str:
        parts = urlsplit(raw_dsn)
        if not parts.scheme.startswith("postgres"):
            return raw_dsn
        cleaned = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k.lower() != "pgbouncer"]
        query = urlencode(cleaned)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))

    # Check for explicit DB URL
    explicit_db_url = (
        os.environ.get("SUPABASE_DB_URL")
        or os.environ.get("DATABASE_URL")
        or os.environ.get("POSTGRES_URL")
    )
    if explicit_db_url:
        if explicit_db_url.startswith("postgresql://") or explicit_db_url.startswith("postgres://"):
            return sanitize_postgres_dsn(explicit_db_url)
    
    # Build from password and project ref
    db_password = os.environ.get("SUPABASE_DB_PASSWORD")
    supabase_url = os.environ.get("SUPABASE_URL", "")
    if not db_password or not supabase_url:
        return None
    
    try:
        host = supabase_url.replace("https://", "").replace("http://", "")
        project_ref = host.split(".")[0]
        if project_ref:
            return f"postgresql://postgres:{db_password}@db.{project_ref}.supabase.co:5432/postgres"
    except Exception:
        pass
    return None


def create_table_sql(table_name: str, column_types: Dict[str, str]) -> str:
    """Generate CREATE TABLE SQL statement."""
    column_defs = ", ".join([f'"{col}" {sql_type}' for col, sql_type in column_types.items()])
    return f'CREATE TABLE IF NOT EXISTS public."{table_name}" ({column_defs});'


def create_table(table_name: str, column_types: Dict[str, str]) -> bool:
    """Create table in Supabase using direct PostgreSQL connection."""
    db_url = resolve_db_url()
    if not db_url:
        print(f"[WARN] No DB URL available - table {table_name} won't be auto-created")
        print("[HINT] Set SUPABASE_DB_PASSWORD in .env or create the table manually in Supabase")
        return False
    
    try:
        sql = create_table_sql(table_name, column_types)
        print(f"[DEBUG] Creating table: {table_name}")
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        print(f"[SUCCESS] Table {table_name} created/verified")
        return True
    except Exception as e:
        print(f"[WARN] CREATE TABLE failed: {e}")
        return False


def get_supabase():
    """Get Supabase client."""
    from supabase import create_client
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    return create_client(url, key)


def clear_table_rows(table_name: str) -> bool:
    """Delete existing rows from a target table so each upload can drive fresh ML outputs."""
    db_url = resolve_db_url()
    if not db_url:
        return False
    try:
        with psycopg.connect(db_url, autocommit=True, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DELETE FROM public."{table_name}";')
        print(f"[INFO] Cleared existing rows in public.{table_name}")
        return True
    except Exception as exc:
        print(f"[WARN] Could not clear table public.{table_name}: {exc}")
        return False


def resolve_item_label(item_id: str, name_map: Dict[str, str]) -> str:
    item = str(item_id or "").strip()
    if not item:
        return "Unknown Item"
    return name_map.get(item.upper(), item.replace("_", " ").title())


def map_item_list(items: List[str], name_map: Dict[str, str]) -> List[str]:
    return [resolve_item_label(item, name_map) for item in (items or [])]


def map_ids_in_text(text: str, name_map: Dict[str, str]) -> str:
    """Replace item-like IDs in free text with human-readable names."""
    raw = str(text or "")
    if not raw:
        return ""

    # Supports IDs like i00013, s00013, sku00123, etc.
    pattern = re.compile(r"\b[a-z]{1,4}\d{3,}\b", flags=re.IGNORECASE)
    return pattern.sub(lambda m: resolve_item_label(m.group(0), name_map), raw)


def parse_price_value(raw_value: Any) -> float | None:
    """Parse price-like values (numbers or currency strings) into float."""
    if raw_value is None:
        return None
    if isinstance(raw_value, bool):
        return None
    if isinstance(raw_value, (int, float)):
        if pd.isna(raw_value):
            return None
        value = float(raw_value)
        return value if value >= 0 else None

    text = str(raw_value).strip()
    if not text:
        return None
    cleaned = text.replace(",", "")
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
    if cleaned in {"", ".", "-", "-."}:
        return None
    try:
        value = float(cleaned)
        return value if value >= 0 else None
    except Exception:
        return None


def normalize_price_lookup_key(raw_value: Any) -> str:
    """Normalize IDs/names to a stable lookup key for price matching."""
    value = str(raw_value or "").strip().upper()
    if not value:
        return ""
    # Canonical form: treat separators/punctuation equally so tokens like
    # "disposable_cape", "Disposable Cape", and "Disposable/Cape" all match.
    value = re.sub(r"[^A-Z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def summarize_item_prices(item_ids: List[str], price_map: Dict[str, float]) -> Tuple[float, int, int]:
    """Return subtotal and availability stats for a list of item IDs."""
    subtotal = 0.0
    priced_count = 0
    missing_count = 0
    for item in item_ids or []:
        key = normalize_price_lookup_key(item)
        price = price_map.get(key)
        if isinstance(price, (int, float)) and price >= 0:
            subtotal += float(price)
            priced_count += 1
        else:
            missing_count += 1
    return round(subtotal, 2), priced_count, missing_count


def suggest_discount_pct(
    confidence: float = 0.0,
    lift: float = 1.0,
    leverage: float = 0.0,
    conviction: float = 1.0,
    item_count: int = 2,
) -> float:
    """Heuristic discount suggestion bounded to practical promo values."""
    conf = max(0.0, min(1.0, float(confidence or 0.0)))
    lift_score = max(0.0, min(1.0, (float(lift or 1.0) - 1.0) / 2.0))
    leverage_score = max(0.0, min(1.0, float(leverage or 0.0) / 0.08))

    raw_conviction = float(conviction or 1.0)
    if math.isfinite(raw_conviction):
        conviction_score = max(0.0, min(1.0, (raw_conviction - 1.0) / 2.0))
    else:
        conviction_score = 1.0

    size_bonus = max(0.0, min(0.06, (max(2, int(item_count or 2)) - 2) * 0.015))
    discount = (
        0.05
        + (conf * 0.07)
        + (lift_score * 0.05)
        + (leverage_score * 0.03)
        + (conviction_score * 0.02)
        + size_bonus
    )
    return round(max(0.05, min(0.22, discount)), 4)


def parse_json_field(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return value if isinstance(value, dict) else {}


def build_run_snapshot(record: Dict) -> Dict:
    payload = parse_json_field(record.get("payload"))
    metrics = parse_json_field(record.get("metrics"))
    fbt = payload.get("frequently_bought_together", []) or []
    confidences = [float(row.get("confidence", 0.0) or 0.0) for row in fbt if isinstance(row, dict)]
    leverages = [float(row.get("leverage", 0.0) or 0.0) for row in fbt if isinstance(row, dict)]
    convictions = []
    for row in fbt:
        if not isinstance(row, dict):
            continue
        conv = row.get("conviction")
        if conv is None:
            continue
        try:
            conv_value = float(conv)
        except Exception:
            continue
        if math.isfinite(conv_value):
            convictions.append(conv_value)

    avg_confidence = (sum(confidences) / len(confidences)) if confidences else 0.0
    avg_leverage = (sum(leverages) / len(leverages)) if leverages else 0.0
    avg_conviction = (sum(convictions) / len(convictions)) if convictions else 0.0
    basket_stats = metrics.get("basket_stats", {}) or {}

    return {
        "run_id": record.get("run_id"),
        "source_table": record.get("source_table"),
        "generated_at": record.get("generated_at"),
        "transactions": int(basket_stats.get("transactions", 0) or 0),
        "rule_count": int(metrics.get("rule_count", 0) or 0),
        "bundle_count": len(payload.get("top_bundles", []) or []),
        "fbt_count": len(fbt),
        "promo_count": len(payload.get("promo_recommendations", []) or []),
        "avg_confidence": round(avg_confidence, 4),
        "avg_leverage": round(avg_leverage, 4),
        "avg_conviction": round(avg_conviction, 4),
    }


def pct_delta(latest_value: float, baseline_value: float) -> float | None:
    if baseline_value == 0:
        return None
    return round(((latest_value - baseline_value) / baseline_value) * 100.0, 2)


def build_analytics_comparison(latest_record: Dict, historical_rows: List[Dict]) -> Dict:
    snapshots = [build_run_snapshot(row) for row in historical_rows]
    latest_snapshot = build_run_snapshot(latest_record)
    latest_run_id = latest_snapshot.get("run_id")

    prior = [s for s in snapshots if s.get("run_id") and s.get("run_id") != latest_run_id]
    if not prior:
        prior = [s for s in snapshots if s.get("run_id") != latest_run_id]

    def avg(metric: str) -> float:
        if not prior:
            return 0.0
        return sum(float(s.get(metric, 0) or 0) for s in prior) / len(prior)

    baseline = {
        "transactions": round(avg("transactions"), 2),
        "rule_count": round(avg("rule_count"), 2),
        "bundle_count": round(avg("bundle_count"), 2),
        "promo_count": round(avg("promo_count"), 2),
        "avg_confidence": round(avg("avg_confidence"), 4),
        "avg_leverage": round(avg("avg_leverage"), 4),
        "avg_conviction": round(avg("avg_conviction"), 4),
    }

    deltas = {
        "transactions": {
            "absolute": latest_snapshot["transactions"] - baseline["transactions"],
            "percent": pct_delta(float(latest_snapshot["transactions"]), float(baseline["transactions"])),
        },
        "rule_count": {
            "absolute": latest_snapshot["rule_count"] - baseline["rule_count"],
            "percent": pct_delta(float(latest_snapshot["rule_count"]), float(baseline["rule_count"])),
        },
        "bundle_count": {
            "absolute": latest_snapshot["bundle_count"] - baseline["bundle_count"],
            "percent": pct_delta(float(latest_snapshot["bundle_count"]), float(baseline["bundle_count"])),
        },
        "promo_count": {
            "absolute": latest_snapshot["promo_count"] - baseline["promo_count"],
            "percent": pct_delta(float(latest_snapshot["promo_count"]), float(baseline["promo_count"])),
        },
        "avg_confidence": {
            "absolute": round(latest_snapshot["avg_confidence"] - baseline["avg_confidence"], 4),
            "percent": pct_delta(float(latest_snapshot["avg_confidence"]), float(baseline["avg_confidence"])),
        },
        "avg_leverage": {
            "absolute": round(latest_snapshot["avg_leverage"] - baseline["avg_leverage"], 4),
            "percent": pct_delta(float(latest_snapshot["avg_leverage"]), float(baseline["avg_leverage"])),
        },
        "avg_conviction": {
            "absolute": round(latest_snapshot["avg_conviction"] - baseline["avg_conviction"], 4),
            "percent": pct_delta(float(latest_snapshot["avg_conviction"]), float(baseline["avg_conviction"])),
        },
    }

    trend = sorted(snapshots, key=lambda row: str(row.get("generated_at") or ""))[-8:]
    return {
        "latest": latest_snapshot,
        "historical_run_count": len(prior),
        "historical_baseline": baseline,
        "deltas": deltas,
        "recent_trend": trend,
    }


def fetch_masterlist_details(supabase, masterlist_table: str | None = None) -> Tuple[Dict[str, str], Dict[str, float]]:
    """Fetch ID -> name and ID -> price maps from a Supabase masterlist table."""
    configured = masterlist_table or os.environ.get("MASTERLIST_TABLE")
    table_candidates = [
        configured,
        "masterlist",
        "price_masterlist",
        "price_list",
        "item_masterlist",
        "service_masterlist",
        "products",
        "services",
    ]
    table_candidates = [t for t in table_candidates if t]

    id_candidates = [
        "item_id",
        "service_id",
        "item_code",
        "service_code",
        "code",
        "sku",
        "id",
    ]
    name_candidates = [
        "item_name",
        "service_name",
        "product_name",
        "label",
        "name",
        "title",
        "display_name",
        "description",
    ]
    price_candidates = [
        "price",
        "item_price",
        "service_price",
        "unit_price",
        "selling_price",
        "srp",
        "amount",
        "rate",
        "cost",
    ]

    aggregated_name_map: Dict[str, str] = {}
    aggregated_price_map: Dict[str, float] = {}

    for table_name in table_candidates:
        try:
            response = supabase.table(table_name).select("*").limit(5000).execute()
            rows = response.data or []
            if not rows:
                continue

            sample = rows[0]
            key_lookup = {str(k).lower(): k for k in sample.keys()}
            id_col = next((key_lookup[c] for c in id_candidates if c in key_lookup), None)
            name_col = next((key_lookup[c] for c in name_candidates if c in key_lookup), None)

            # Heuristic fallback for non-standard schemas.
            if not id_col:
                id_col = next(
                    (
                        orig
                        for lower, orig in key_lookup.items()
                        if lower == "id"
                        or lower.endswith("_id")
                        or lower.endswith("_code")
                        or "sku" in lower
                    ),
                    None,
                )
            if not name_col:
                name_col = next(
                    (
                        orig
                        for lower, orig in key_lookup.items()
                        if "name" in lower or "label" in lower or "title" in lower
                    ),
                    None,
                )
            if not id_col or not name_col:
                continue

            price_col = next((key_lookup[c] for c in price_candidates if c in key_lookup), None)
            if not price_col:
                price_col = next(
                    (
                        orig
                        for lower, orig in key_lookup.items()
                        if "price" in lower
                        or "amount" in lower
                        or "cost" in lower
                        or lower.endswith("_rate")
                    ),
                    None,
                )

            name_map: Dict[str, str] = {}
            price_map: Dict[str, float] = {}
            for row in rows:
                raw_id = row.get(id_col)
                raw_name = row.get(name_col)
                if raw_id is None or raw_name is None:
                    continue
                key = str(raw_id).strip().upper()
                value = str(raw_name).strip()
                if key and value:
                    name_map[key] = value

                if key and price_col:
                    parsed_price = parse_price_value(row.get(price_col))
                    if parsed_price is not None:
                        price_map[key] = round(parsed_price, 2)

            if name_map:
                print(
                    f"[INFO] Loaded masterlist details from {table_name} "
                    f"({len(name_map)} names, {len(price_map)} prices)"
                )

                # Merge names first-seen to preserve primary labeling source.
                for key, value in name_map.items():
                    aggregated_name_map.setdefault(key, value)

                # Merge prices with latest non-null value taking precedence.
                for key, value in price_map.items():
                    aggregated_price_map[key] = value
        except Exception as exc:
            print(f"[WARN] Masterlist lookup failed for table {table_name}: {exc}")

    if aggregated_name_map:
        print(
            "[INFO] Masterlist aggregate coverage: "
            f"{len(aggregated_name_map)} names, {len(aggregated_price_map)} prices"
        )

    return aggregated_name_map, aggregated_price_map


def fetch_masterlist_map(supabase, masterlist_table: str | None = None) -> Dict[str, str]:
    """Backward-compatible wrapper returning only the name map."""
    name_map, _price_map = fetch_masterlist_details(supabase, masterlist_table=masterlist_table)
    return name_map


def count_public_tables() -> int | None:
    """Count public base tables via direct PostgreSQL connection when available."""
    db_url = resolve_db_url()
    if not db_url:
        return None
    try:
        with psycopg.connect(db_url, autocommit=True, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
                    """
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0
    except Exception as exc:
        print(f"[WARN] Could not count public tables: {exc}")
        return None


def get_table_column_count(table_name: str) -> int | None:
    """Get column count for a specific public table using direct PostgreSQL connection."""
    db_url = resolve_db_url()
    if not db_url:
        return None
    try:
        with psycopg.connect(db_url, autocommit=True, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s;
                    """,
                    (table_name,),
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0
    except Exception as exc:
        print(f"[WARN] Could not count columns for {table_name}: {exc}")
        return None


def get_public_table_columns_map() -> Dict[str, List[str]]:
    """Return public table -> ordered column names map using direct PostgreSQL metadata."""
    db_url = resolve_db_url()
    if not db_url:
        return {}

    try:
        with psycopg.connect(db_url, autocommit=True, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position;
                    """
                )
                rows = cur.fetchall()

        table_map: Dict[str, List[str]] = {}
        for table_name, column_name in rows:
            key = str(table_name)
            table_map.setdefault(key, []).append(str(column_name))
        return table_map
    except Exception as exc:
        print(f"[WARN] Could not read public table columns: {exc}")
        return {}


def get_table_row_count(table_name: str) -> int | None:
    """Return row count for a public table when direct DB access is available."""
    db_url = resolve_db_url()
    if not db_url:
        return None

    try:
        with psycopg.connect(db_url, autocommit=True, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM public."{table_name}";')
                row = cur.fetchone()
                return int(row[0]) if row else 0
    except Exception:
        return None


def resolve_existing_table_for_columns(cleaned_columns: List[str], preferred_table: str | None = None) -> str | None:
    """Find an existing table with exactly the same column set for append-style uploads."""
    target_cols = [sanitize_identifier(col, "column") for col in cleaned_columns]
    target_set = set(target_cols)
    if not target_set:
        return None

    table_map = get_public_table_columns_map()
    if not table_map:
        return None

    excluded_tables = {
        "ml_recommendations",
        sanitize_identifier(os.environ.get("MASTERLIST_TABLE") or "", "table"),
        "masterlist",
        "price_masterlist",
        "price_list",
        "item_masterlist",
        "service_masterlist",
        "products",
        "services",
    }

    exact_matches = []
    for table_name, columns in table_map.items():
        if table_name in excluded_tables:
            continue
        if set(columns) == target_set:
            exact_matches.append(table_name)

    if not exact_matches:
        return None

    if preferred_table and preferred_table in exact_matches:
        return preferred_table

    # Prefer the table with the most rows so equivalent uploads converge into one canonical table.
    ranked = []
    for table_name in exact_matches:
        row_count = get_table_row_count(table_name)
        ranked.append((table_name, -1 if row_count is None else row_count))

    ranked.sort(key=lambda pair: (pair[1], pair[0]), reverse=True)
    return ranked[0][0]


def resolve_stats_table_name(requested_table: str | None = None) -> List[str]:
    """Build ordered table candidates for stats lookup."""
    candidates = [
        sanitize_identifier(requested_table, "table") if requested_table else None,
        latest_loaded_table,
        os.environ.get("DEFAULT_DATA_TABLE"),
        "teradrip_datasets_hairstylist_datasets",
        "teradrip_lab_report_2_datasets_hairstylist_dataset",
    ]
    result: List[str] = []
    seen = set()
    for raw in candidates:
        if not raw:
            continue
        name = str(raw).strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(name)
    return result


def records_from_dataframe(df: pd.DataFrame) -> List[Dict]:
    """Convert dataframe to list of records for Supabase."""
    records = []
    for row in df.to_dict(orient="records"):
        normalized = {}
        for key, value in row.items():
            if pd.isna(value):
                normalized[key] = None
            elif hasattr(value, "isoformat"):
                normalized[key] = value.isoformat()
            else:
                normalized[key] = value
        records.append(normalized)
    return records


def run_etl_script(csv_path: Path, table_name: str) -> tuple[int, str]:
    """Run the ETL script as a subprocess for a specific CSV file."""
    etl_script = ETL_DIR / "data_etl.py"
    result = subprocess.run(
        [
            sys.executable,
            str(etl_script),
            "--input-path",
            str(csv_path),
            "--table-name",
            table_name,
        ],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    combined_output = (result.stdout or "")
    if result.stderr:
        combined_output = f"{combined_output}\n{result.stderr}".strip()
    return result.returncode, combined_output


def diagnose_load_issue(etl_output: str) -> str:
    """Convert common ETL load failures into actionable diagnostics."""
    lower = (etl_output or "").lower()

    if "getaddrinfo failed" in lower and "db." in lower:
        return (
            "Direct DB host DNS failed. This network cannot resolve Supabase DB host for your project. "
            "Use a valid Supabase connection-pooler DSN from Dashboard or run on a network that can reach DB DNS."
        )

    if "network is unreachable" in lower:
        return (
            "Supabase DB is only reachable via IPv6 from this host, but your network cannot route IPv6. "
            "Use Supabase pooler DSN (IPv4) in SUPABASE_DB_URL from Dashboard."
        )

    if "pgrst205" in lower or "could not find the table" in lower:
        return (
            "REST insert target table is missing from API schema. "
            "Because direct DB create failed, table auto-creation could not run. "
            "Set a working SUPABASE_DB_URL (pooler/DB DSN) or create the table manually in Supabase."
        )

    if "permission denied" in lower or "not authorized" in lower:
        return (
            "Supabase key lacks write permission. Use SUPABASE_SERVICE_ROLE_KEY for ETL loads "
            "or adjust RLS policies for the target table."
        )

    if "tenant or user not found" in lower:
        return (
            "Connection pooler credentials/host are incorrect for this project. "
            "Copy the exact pooler connection string from Supabase Dashboard > Connect."
        )

    return "ETL load failed. Inspect etl_log_tail for details."


def process_saved_upload(saved_file: Path, safe_name: str) -> Dict[str, Any]:
    """Process an already-saved CSV file through ETL steps."""
    global pipeline_status, latest_loaded_table, latest_upload_time

    load_warning = None
    load_hint = None
    etl_log_tail: List[str] = []

    # Step 1: EXTRACT
    pipeline_status["step"] = 1
    pipeline_status["message"] = f"Extracting data from CSV: {saved_file.name}"
    pipeline_status["error"] = None
    persist_runtime_state()

    raw_df = pd.read_csv(saved_file, low_memory=False)
    pipeline_status["rows_processed"] = len(raw_df)
    pipeline_status["columns"] = list(raw_df.columns)
    persist_runtime_state()

    # Step 2: TRANSFORM
    pipeline_status["step"] = 2
    pipeline_status["message"] = "Transforming and cleaning data..."
    persist_runtime_state()

    cleaned_df = clean_dataframe(raw_df)
    if cleaned_df.empty:
        raise ValueError("No data after cleaning")

    filename_table_name = sanitize_identifier(Path(safe_name).stem, "table")
    matched_table_name = resolve_existing_table_for_columns(
        list(cleaned_df.columns),
        preferred_table=latest_loaded_table,
    )
    table_name = matched_table_name or filename_table_name
    append_mode = matched_table_name is not None

    # Step 3: LOAD
    pipeline_status["step"] = 3
    pipeline_status["message"] = "Running ETL script..."
    persist_runtime_state()

    rc, etl_output = run_etl_script(saved_file, table_name)
    if etl_output:
        etl_log_tail = [line for line in etl_output.splitlines() if line.strip()][-15:]

    has_etl_error = (rc != 0) or ("[ERROR]" in etl_output)
    if has_etl_error:
        load_warning = "ETL script finished with warnings/errors during load step."
        load_hint = diagnose_load_issue(etl_output)
        pipeline_status["message"] = "CSV saved and transformed, but warehouse load may have failed."
        pipeline_status["error"] = (etl_log_tail[-1] if etl_log_tail else f"ETL exited with code {rc}")
        print(f"[WARN] {load_warning}")
        print(f"[HINT] {load_hint}")
    else:
        if append_mode:
            pipeline_status["message"] = f"Appended {len(cleaned_df)} records to {table_name}"
            print(f"[SUCCESS] Appended {len(cleaned_df)} records to existing table {table_name}")
        else:
            pipeline_status["message"] = f"Loaded {len(cleaned_df)} records to {table_name}"
            print(f"[SUCCESS] Loaded {len(cleaned_df)} records to new table {table_name}")

    # Step 4: WAREHOUSE
    pipeline_status["step"] = 4
    pipeline_status["message"] = "Data stored in warehouse successfully!"
    pipeline_status["end_time"] = datetime.now().isoformat()
    latest_loaded_table = table_name
    latest_upload_time = pipeline_status["end_time"]
    persist_runtime_state()

    return {
        "success": True,
        "rows": len(cleaned_df),
        "columns": list(cleaned_df.columns),
        "table_name": table_name,
        "table_resolution": {
            "mode": "append_existing" if append_mode else "new_table",
            "filename_table": filename_table_name,
            "resolved_table": table_name,
            "matched_by_schema": bool(append_mode),
        },
        "latest_loaded_table": latest_loaded_table,
        "saved_file": str(saved_file.relative_to(PROJECT_ROOT)),
        "load_warning": load_warning,
        "load_hint": load_hint,
        "etl_log_tail": etl_log_tail,
        "status": pipeline_status,
    }


def run_ml_analysis_core(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run the ML analysis core logic and return API response payload."""
    global pipeline_status, latest_loaded_table

    requested_table = payload.get("table_name")
    source_table = latest_loaded_table or requested_table or "teradrip_datasets_hairstylist_datasets"
    output_table = payload.get("output_table", "ml_recommendations")
    limit = payload.get("limit")
    refresh = bool(payload.get("refresh", True))

    supabase = get_supabase()
    final_record = None
    phase_records: List[Dict[str, Any]] = []

    if refresh:
        engine = TeraDripMBAEngine(
            source_table=source_table,
            output_table=output_table,
            max_loops=5,
        )
        run_result = engine.run(limit=limit, persist=True)
        final_record = run_result.get("final") or {}
        phase_records = run_result.get("phases") or []
    else:
        latest = (
            supabase.table(output_table)
            .select("*")
            .eq("source_table", source_table)
            .order("generated_at", desc=True)
            .limit(1)
            .execute()
        )
        if latest.data:
            final_record = latest.data[0]
            run_id = final_record.get("run_id")
            if run_id:
                try:
                    phase_query = (
                        supabase.table(output_table)
                        .select("phase, generated_at, algorithm_used, metrics")
                        .eq("run_id", run_id)
                        .order("phase")
                        .limit(10)
                        .execute()
                    )
                    phase_records = phase_query.data or []
                except Exception:
                    phase_records = []

    if not final_record:
        raise ValueError("No ML result found. Run with refresh=true first.")

    ml_payload = parse_json_field(final_record.get("payload"))
    metrics = parse_json_field(final_record.get("metrics"))
    governance = parse_json_field(final_record.get("governance"))

    historical_query = supabase.table(output_table).select("*").order("generated_at", desc=True).limit(60)
    historical_response = historical_query.execute()
    historical_rows = historical_response.data or []
    analytics_comparison = build_analytics_comparison(final_record, historical_rows)

    iteration_phases = []
    for phase_row in phase_records:
        metrics_row = parse_json_field(phase_row.get("metrics"))
        thresholds_row = metrics_row.get("thresholds", {}) or {}
        basket_stats_row = metrics_row.get("basket_stats", {}) or {}
        iteration_phases.append(
            {
                "phase": int(phase_row.get("phase", metrics_row.get("phase", 0)) or 0),
                "generated_at": phase_row.get("generated_at") or final_record.get("generated_at"),
                "algorithm": phase_row.get("algorithm_used") or metrics_row.get("algorithm"),
                "score": float(metrics_row.get("score", 0.0) or 0.0),
                "score_raw": float(metrics_row.get("score_raw", 0.0) or 0.0),
                "rule_count": int(metrics_row.get("rule_count", 0) or 0),
                "itemset_count": int(metrics_row.get("itemset_count", 0) or 0),
                "target_rule_count": int(metrics_row.get("target_rule_count", 0) or 0),
                "thresholds": thresholds_row,
                "basket_stats": basket_stats_row,
                "loop_trace": metrics_row.get("loop_trace", []) or [],
            }
        )

    iteration_phases = sorted(
        [row for row in iteration_phases if int(row.get("phase", 0) or 0) > 0],
        key=lambda row: int(row.get("phase", 0) or 0),
    )

    name_map, price_map = fetch_masterlist_details(
        supabase,
        masterlist_table=payload.get("masterlist_table") or os.environ.get("MASTERLIST_TABLE"),
    )
    price_lookup = {normalize_price_lookup_key(k): float(v) for k, v in (price_map or {}).items()}
    for item_id, item_name in (name_map or {}).items():
        key = normalize_price_lookup_key(item_id)
        label_key = normalize_price_lookup_key(item_name)
        if key in price_lookup and label_key:
            price_lookup[label_key] = price_lookup[key]

    price_coverage = {
        "named_items": len(name_map),
        "priced_items": len(price_map),
    }

    top_bundles = []
    for bundle in (ml_payload.get("top_bundles", []) or []):
        mapped_bundle = dict(bundle)
        raw_items = bundle.get("items", []) or []
        mapped_bundle["items"] = map_item_list(raw_items, name_map)
        if "explanation" in mapped_bundle:
            mapped_bundle["explanation"] = map_ids_in_text(mapped_bundle.get("explanation", ""), name_map)

        subtotal, priced_count, missing_count = summarize_item_prices(raw_items, price_lookup)
        discount_pct = suggest_discount_pct(
            confidence=0.0,
            lift=1.0 + float(bundle.get("support", 0.0) or 0.0),
            leverage=float(bundle.get("leverage", 0.0) or 0.0),
            conviction=float(bundle.get("conviction", 1.0) or 1.0),
            item_count=len(raw_items),
        )
        savings = round(subtotal * discount_pct, 2) if subtotal > 0 else 0.0
        discounted = round(max(0.0, subtotal - savings), 2) if subtotal > 0 else 0.0
        mapped_bundle["pricing"] = {
            "subtotal": subtotal,
            "discount_pct": discount_pct,
            "discount_amount": savings,
            "discounted_total": discounted,
            "priced_items": priced_count,
            "missing_prices": missing_count,
        }
        top_bundles.append(mapped_bundle)

    mapped_fbt_rows = []
    for row in (ml_payload.get("frequently_bought_together", []) or []):
        mapped_row = dict(row)
        raw_if_items = row.get("if_items", []) or []
        raw_then_items = row.get("then_items", []) or []
        mapped_row["if_items"] = map_item_list(raw_if_items, name_map)
        mapped_row["then_items"] = map_item_list(raw_then_items, name_map)

        if_subtotal, if_priced_count, if_missing_count = summarize_item_prices(raw_if_items, price_lookup)
        then_subtotal, then_priced_count, then_missing_count = summarize_item_prices(raw_then_items, price_lookup)
        combined_subtotal = round(if_subtotal + then_subtotal, 2)
        discount_pct = suggest_discount_pct(
            confidence=float(row.get("confidence", 0.0) or 0.0),
            lift=float(row.get("lift", 1.0) or 1.0),
            leverage=float(row.get("leverage", 0.0) or 0.0),
            conviction=float(row.get("conviction", 1.0) or 1.0),
            item_count=len(raw_if_items) + len(raw_then_items),
        )
        discount_amount = round(combined_subtotal * discount_pct, 2) if combined_subtotal > 0 else 0.0
        discounted_total = round(max(0.0, combined_subtotal - discount_amount), 2) if combined_subtotal > 0 else 0.0
        mapped_row["pricing"] = {
            "if_subtotal": if_subtotal,
            "then_subtotal": then_subtotal,
            "combined_subtotal": combined_subtotal,
            "discount_pct": discount_pct,
            "discount_amount": discount_amount,
            "discounted_total": discounted_total,
            "priced_items": if_priced_count + then_priced_count,
            "missing_prices": if_missing_count + then_missing_count,
        }
        mapped_fbt_rows.append(mapped_row)

    mapped_promos = []
    for promo in (ml_payload.get("promo_recommendations", []) or []):
        mapped_promo = dict(promo)
        raw_trigger_items = promo.get("trigger_items", []) or []
        raw_target_items = promo.get("target_items", []) or []
        mapped_promo["trigger_items"] = map_item_list(raw_trigger_items, name_map)
        mapped_promo["target_items"] = map_item_list(raw_target_items, name_map)
        if "message" in mapped_promo:
            mapped_promo["message"] = map_ids_in_text(mapped_promo.get("message", ""), name_map)

        combo_items = [*raw_trigger_items, *raw_target_items]
        subtotal, priced_count, missing_count = summarize_item_prices(combo_items, price_lookup)
        if str(mapped_promo.get("offer_type", "")).lower() == "buy_2_get_1" and len(combo_items) >= 3:
            discount_pct = round(1.0 / 3.0, 4)
        else:
            discount_pct = suggest_discount_pct(
                confidence=float(promo.get("confidence", 0.0) or 0.0),
                lift=float(promo.get("lift", 1.0) or 1.0),
                leverage=float(promo.get("leverage", 0.0) or 0.0),
                conviction=float(promo.get("conviction", 1.0) or 1.0),
                item_count=len(combo_items),
            )
        discount_amount = round(subtotal * discount_pct, 2) if subtotal > 0 else 0.0
        discounted_total = round(max(0.0, subtotal - discount_amount), 2) if subtotal > 0 else 0.0
        mapped_promo["pricing"] = {
            "subtotal": subtotal,
            "discount_pct": discount_pct,
            "discount_amount": discount_amount,
            "discounted_total": discounted_total,
            "priced_items": priced_count,
            "missing_prices": missing_count,
        }
        mapped_promos.append(mapped_promo)

    mapped_cross_sell = {}
    cross_sell_pricing = []
    for base_item, suggestions in (ml_payload.get("cross_sell_suggestions", {}) or {}).items():
        mapped_base = resolve_item_label(base_item, name_map)
        raw_suggestions = suggestions or []
        mapped_cross_sell[mapped_base] = map_item_list(raw_suggestions, name_map)

        base_price = price_lookup.get(normalize_price_lookup_key(base_item))
        suggestion_rows = []
        for raw_suggestion in raw_suggestions:
            suggestion_price = price_lookup.get(normalize_price_lookup_key(raw_suggestion))
            if suggestion_price is None:
                discount_pct = 0.0
                discounted_price = None
                savings = None
            else:
                discount_pct = suggest_discount_pct(confidence=0.55, lift=1.2, leverage=0.01, conviction=1.1, item_count=2)
                savings = round(suggestion_price * discount_pct, 2)
                discounted_price = round(max(0.0, suggestion_price - savings), 2)

            suggestion_rows.append(
                {
                    "item": resolve_item_label(raw_suggestion, name_map),
                    "price": suggestion_price,
                    "discount_pct": discount_pct,
                    "discount_amount": savings,
                    "discounted_price": discounted_price,
                }
            )

        cross_sell_pricing.append(
            {
                "base_item": mapped_base,
                "base_price": base_price,
                "suggestions": suggestion_rows,
            }
        )

    fbt_rows = mapped_fbt_rows
    top_recommendations = []
    for row in fbt_rows[:10]:
        if_items = row.get("if_items", [])
        then_items = row.get("then_items", [])
        if_item = if_items[0] if if_items else "Item A"
        then_item = then_items[0] if then_items else "Item B"
        conf = float(row.get("confidence", 0.0)) * 100
        top_recommendations.append(
            {
                "if_item": if_item,
                "then_item": then_item,
                "confidence": round(conf, 1),
                "support": row.get("support", 0),
            }
        )

    business_insights = [
        map_ids_in_text(line, name_map)
        for line in (ml_payload.get("business_insights", []) or [])
    ]
    insight_lines = [
        {"text": f"> Insight: {line}", "type": "info"}
        for line in business_insights[:3]
    ]

    pipeline_status["message"] = "ML analysis complete!"
    persist_runtime_state()

    final_thresholds = metrics.get("thresholds", {}) or {}
    threshold_preview = {
        "minsup": final_thresholds.get("min_support"),
        "mincof": final_thresholds.get("min_confidence"),
        "min_lift": final_thresholds.get("min_lift"),
        "min_leverage": final_thresholds.get("min_leverage"),
        "min_conviction": final_thresholds.get("min_conviction"),
        "min_support": final_thresholds.get("min_support"),
        "min_confidence": final_thresholds.get("min_confidence"),
        "source": "ml_final",
    }

    return {
        "success": True,
        "run_id": final_record.get("run_id"),
        "source_table": source_table,
        "requested_table": requested_table,
        "latest_loaded_table": latest_loaded_table,
        "algorithm": metrics.get("algorithm"),
        "total_records": int(metrics.get("basket_stats", {}).get("transactions", 0)),
        "feature_columns": [],
        "item_frequencies": {},
        "thresholds": threshold_preview,
        "recommendations": top_recommendations,
        "top_bundles": top_bundles,
        "frequently_bought_together": fbt_rows,
        "cross_sell_suggestions": mapped_cross_sell,
        "cross_sell_pricing": cross_sell_pricing,
        "promo_recommendations": mapped_promos,
        "business_insights": business_insights,
        "price_coverage": price_coverage,
        "governance": governance,
        "analytics_comparison": analytics_comparison,
        "iteration_phases": iteration_phases,
        "loop_trace": metrics.get("loop_trace", []),
        "logs": [
            {"text": "> Connecting to Warehouse...", "type": "info"},
            {"text": f"> Running Hybrid MBA on {source_table}...", "type": "info"},
            {"text": f"> Algorithm: {metrics.get('algorithm', 'n/a')}", "type": "info"},
            {
                "text": (
                    f"> Records: {int(metrics.get('basket_stats', {}).get('transactions', 0)):,}"
                ),
                "type": "info",
            },
            {"text": f"> Rules generated: {int(metrics.get('rule_count', 0)):,}", "type": "info"},
            {
                "text": (
                    f"> Historical baselines: {int(analytics_comparison.get('historical_run_count', 0))} prior run(s)"
                ),
                "type": "info",
            },
            {
                "text": (
                    f"> Price coverage: {price_coverage['priced_items']}/{price_coverage['named_items']} masterlist items"
                ),
                "type": "info",
            },
            {"text": f"> Manual review flags: {len(governance.get('anomalies', []))}", "type": "warning"},
            *insight_lines,
            {"text": "> Analysis Complete ✅", "type": "success"},
        ],
    }


def resume_pending_job_if_needed() -> None:
    """Resume unfinished upload/ML work in a background thread after restart."""
    global _resume_thread
    if not isinstance(pending_job, dict):
        return
    if _resume_thread and _resume_thread.is_alive():
        return

    def _runner():
        global pipeline_status
        job = dict(pending_job or {})
        kind = str(job.get("kind", "")).lower()
        try:
            if kind == "upload":
                rel = job.get("saved_file")
                safe_name = job.get("safe_name")
                if not rel or not safe_name:
                    return
                csv_path = PROJECT_ROOT / str(rel)
                if not csv_path.exists():
                    pipeline_status["error"] = f"Cannot resume upload; file not found: {rel}"
                    persist_runtime_state()
                    return
                print(f"[INFO] Resuming pending upload job for {csv_path.name}")
                process_saved_upload(csv_path, str(safe_name))
                set_pending_job(None)
            elif kind == "ml":
                payload = job.get("payload") or {}
                print("[INFO] Resuming pending ML analysis job")
                run_ml_analysis_core(dict(payload))
                set_pending_job(None)
        except Exception as exc:
            pipeline_status["error"] = f"Resume failed: {exc}"
            persist_runtime_state()

    _resume_thread = threading.Thread(target=_runner, daemon=True, name="resume-pending-job")
    _resume_thread.start()


# ============ API ROUTES ============

@app.route("/")
def serve_frontend():
    """Serve the main HTML page."""
    return send_from_directory(app.static_folder, "teradrip_salon.html")


@app.route("/api/status")
def get_status():
    """Get current pipeline status."""
    return jsonify(pipeline_status)


@app.route("/api/upload", methods=["POST"])
def upload_csv():
    """Upload and process a CSV file through the ETL pipeline."""
    global pipeline_status
    
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files["file"]
    if not file.filename.endswith(".csv"):
        return jsonify({"error": "File must be a CSV"}), 400
    
    pipeline_status = {
        "step": 0,
        "message": "Starting pipeline...",
        "rows_processed": 0,
        "columns": [],
        "error": None,
        "start_time": datetime.now().isoformat(),
        "end_time": None
    }
    persist_runtime_state()
    try:
        ETL_CSV_DIR.mkdir(parents=True, exist_ok=True)

        safe_name = secure_filename(file.filename or "uploaded.csv")
        if not safe_name.lower().endswith(".csv"):
            safe_name = f"{Path(safe_name).stem}.csv"

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved_file = ETL_CSV_DIR / f"{timestamp}_{safe_name}"
        file.save(saved_file)
        set_pending_job(
            {
                "kind": "upload",
                "saved_file": str(saved_file.relative_to(PROJECT_ROOT)),
                "safe_name": safe_name,
                "created_at": datetime.now().isoformat(),
            }
        )
        result = process_saved_upload(saved_file, safe_name)
        set_pending_job(None)
        return jsonify(result)
        
    except Exception as e:
        pipeline_status["error"] = str(e)
        persist_runtime_state()
        return jsonify({"error": str(e)}), 500


@app.route("/api/ml/analyze", methods=["POST"])
def run_ml_analysis():
    """Run hybrid MBA analysis and return frontend-ready recommendations."""
    global pipeline_status, latest_loaded_table

    pipeline_status["step"] = 5
    pipeline_status["message"] = "Running ML analysis..."
    pipeline_status["error"] = None
    persist_runtime_state()

    try:
        payload = request.get_json(silent=True) or {}
        set_pending_job(
            {
                "kind": "ml",
                "payload": payload,
                "created_at": datetime.now().isoformat(),
            }
        )
        result = run_ml_analysis_core(payload)
        
        # Save result to history
        table_used = latest_loaded_table or payload.get("table_name") or "unknown_table"
        save_ml_result_to_history(result, table_used)
        
        set_pending_job(None)
        return jsonify(result)
    except ValueError as e:
        pipeline_status["error"] = str(e)
        persist_runtime_state()
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        pipeline_status["error"] = str(e)
        persist_runtime_state()
        return jsonify({"error": str(e)}), 500


@app.route("/api/ml/history", methods=["GET"])
def get_ml_history():
    """Get list of historical ML runs."""
    try:
        history = load_ml_history()
        
        # Return summary info (without full results to keep payload small)
        summary = []
        for entry in history:
            summary.append({
                "run_id": entry.get("run_id"),
                "timestamp": entry.get("timestamp"),
                "table_name": entry.get("table_name"),
                "csv_filename": entry.get("csv_filename", "Unknown")
            })
        
        return jsonify({"history": summary, "count": len(summary)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ml/history/<run_id>", methods=["GET"])
def get_ml_run_by_id(run_id: str):
    """Get a specific ML run result by run_id."""
    try:
        history = load_ml_history()
        
        for entry in history:
            if entry.get("run_id") == run_id:
                return jsonify(entry.get("result", {}))
        
        return jsonify({"error": f"Run {run_id} not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/data/stats")
def get_data_stats():
    """Get data statistics from the warehouse."""
    try:
        supabase = get_supabase()
        requested_table = request.args.get("table_name")
        target_table = None
        probe_response = None
        table_candidates = resolve_stats_table_name(requested_table)

        for table_name in table_candidates:
            try:
                response = (
                    supabase.table(table_name)
                    .select("*", count="exact")
                    .limit(1)
                    .execute()
                )
                target_table = table_name
                probe_response = response
                break
            except Exception:
                continue

        if not target_table or probe_response is None:
            return jsonify(
                {
                    "records": 0,
                    "transforms": 0,
                    "tables": count_public_tables() or 0,
                    "table_name": None,
                    "table_found": False,
                    "source": "database",
                    "message": "No accessible dataset table was found for stats lookup.",
                }
            )

        record_count = int(getattr(probe_response, "count", 0) or 0)
        sample_rows = probe_response.data or []
        column_count = len(sample_rows[0].keys()) if sample_rows else 0
        if column_count == 0:
            column_count = int(get_table_column_count(target_table) or 0)

        tables_count = count_public_tables()
        return jsonify(
            {
                "records": record_count,
                "transforms": column_count,
                "tables": int(tables_count if tables_count is not None else 1),
                "table_name": target_table,
                "table_found": True,
                "source": "database",
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/health")
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "supabase_configured": bool(os.environ.get("SUPABASE_URL"))
    })


if __name__ == "__main__":
    load_runtime_state()
    print("\n" + "=" * 50)
    print("TeraDrip Salon Backend Starting...")
    print("=" * 50)
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Frontend: {app.static_folder}")
    print(f"Supabase API: {'Configured' if os.environ.get('SUPABASE_URL') else 'Not configured'}")
    db_url = resolve_db_url()
    print(f"DB Direct: {'Configured' if db_url else 'Not configured (table auto-creation disabled)'}")
    if pending_job:
        print(f"[RESUME] Pending job detected: {pending_job.get('kind', 'unknown')}")
    print("=" * 50)
    print("Open http://localhost:5000 in your browser")
    print("=" * 50 + "\n")

    resume_pending_job_if_needed()
    app.run(debug=True, port=5000, use_reloader=False)
