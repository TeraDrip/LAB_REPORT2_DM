import argparse
import json
import os
import re
import time
import warnings
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd
import psycopg
from psycopg.errors import ConnectionTimeout


def sanitize_identifier(raw_name: str, fallback: str = "column") -> str:
    value = re.sub(r"[^a-zA-Z0-9_]+", "_", str(raw_name).strip().lower())
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        value = fallback
    if value[0].isdigit():
        value = f"{fallback}_{value}"
    return value


def load_env_file(env_path: Path, override: bool = False) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue

        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]

        if override or key not in os.environ:
            os.environ[key] = value


class CSVToSupabaseETL:
    def __init__(self, input_path: Path):
        self.project_root = Path(__file__).resolve().parent.parent.parent
        self.input_path = input_path.resolve()
        self.env_path = self.project_root / ".env"
        load_env_file(self.env_path, override=False)

        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")

        self.db_connect_timeout_s = int(os.environ.get("SUPABASE_DB_CONNECT_TIMEOUT", "12"))
        self.http_timeout_s = int(os.environ.get("SUPABASE_HTTP_TIMEOUT", "30"))
        self.db_port = int(os.environ.get("SUPABASE_DB_PORT", "5432"))
        self.db_pooler_port = int(os.environ.get("SUPABASE_DB_POOLER_PORT", "6543"))

        self.db_url = self.resolve_db_url()
        self.use_postgres = bool(self.db_url)

        if not self.use_postgres and (not self.supabase_url or not self.supabase_key):
            raise ValueError(
                "Missing credentials. Set SUPABASE_DB_URL (recommended) or set SUPABASE_URL plus "
                "SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) in .env."
            )

        if not self.use_postgres:
            print(
                "[WARN] No DB URL found. Table auto-creation is disabled. "
                "Set SUPABASE_DB_URL or SUPABASE_DB_PASSWORD (plus SUPABASE_URL) in .env to enable CREATE TABLE."
            )

    def resolve_db_url(self) -> str | None:
        explicit_db_url = (
            os.environ.get("SUPABASE_DB_URL")
            or os.environ.get("DATABASE_URL")
            or os.environ.get("POSTGRES_URL")
        )
        if explicit_db_url:
            # Reject common mistake: HTTP project URL is not a Postgres DSN.
            if explicit_db_url.startswith("postgresql://") or explicit_db_url.startswith("postgres://"):
                return explicit_db_url
            print(
                "[WARN] Ignoring invalid DB URL format in .env. "
                "SUPABASE_DB_URL must start with postgres:// or postgresql://"
            )

        db_password = os.environ.get("SUPABASE_DB_PASSWORD")
        if not db_password:
            return None

        if not self.supabase_url:
            print("[WARN] SUPABASE_DB_PASSWORD is set but SUPABASE_URL is missing; cannot build a DB URL.")
            return None

        # Build default Supabase Postgres URL from project ref in SUPABASE_URL.
        # Example host: https://<project_ref>.supabase.co
        project_ref = ""
        try:
            host = self.supabase_url.replace("https://", "").replace("http://", "")
            project_ref = host.split(".")[0]
        except Exception:
            project_ref = ""

        if not project_ref:
            return None

        encoded_password = quote(db_password, safe="")
        return (
            f"postgresql://postgres:{encoded_password}"
            f"@db.{project_ref}.supabase.co:{self.db_port}/postgres?sslmode=require"
        )

    def with_port(self, postgres_url: str, port: int) -> str:
        if not (postgres_url.startswith("postgresql://") or postgres_url.startswith("postgres://")):
            return postgres_url
        from urllib.parse import urlsplit, urlunsplit

        parts = urlsplit(postgres_url)
        hostname = parts.hostname or ""
        if not hostname:
            return postgres_url

        userinfo = ""
        if parts.username:
            userinfo = parts.username
            if parts.password is not None:
                userinfo += f":{parts.password}"
            userinfo += "@"

        netloc = f"{userinfo}{hostname}:{int(port)}"
        if parts.port == int(port) and parts.netloc:
            return postgres_url
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))

    def connect_postgres(self):
        if not self.db_url:
            raise ValueError("DB URL is not configured")

        try:
            return psycopg.connect(
                self.db_url,
                autocommit=True,
                connect_timeout=self.db_connect_timeout_s,
            )
        except ConnectionTimeout:
            # Common on some networks: outbound 5432 is blocked but 6543 (pooler) works.
            alt_url = self.with_port(self.db_url, self.db_pooler_port)
            if alt_url == self.db_url:
                raise
            conn = psycopg.connect(
                alt_url,
                autocommit=True,
                connect_timeout=self.db_connect_timeout_s,
            )
            self.db_url = alt_url
            return conn

    def is_likely_datetime_series(self, series: pd.Series) -> bool:
        non_null = series.dropna()
        if non_null.empty:
            return False

        text = non_null.astype(str).str.strip().str.lower()
        # Avoid parsing IDs/codes as dates.
        has_date_tokens = text.str.contains(
            r"[-/:]|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|am|pm",
            regex=True,
            na=False,
        )
        token_ratio = has_date_tokens.mean()
        return token_ratio >= 0.6

    def is_identifier_column(self, column_name: str) -> bool:
        # Only treat "id" as an identifier token, not a substring.
        # Example: "wide_tooth_comb" contains letters "id" but is NOT an ID column.
        normalized = sanitize_identifier(column_name, "column")
        return re.search(r"(^|_)id($|_)", normalized) is not None

    def discover_csv_files(self) -> List[Path]:
        if self.input_path.is_file() and self.input_path.suffix.lower() == ".csv":
            return [self.input_path]

        if self.input_path.is_dir():
            csv_files: List[Path] = []
            for path in self.input_path.rglob("*.csv"):
                if ".venv" in path.parts:
                    continue
                csv_files.append(path)
            return sorted(csv_files)

        return []

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def infer_sql_type_and_transform(self, column_name: str, series: pd.Series) -> Tuple[str, pd.Series]:
        non_null = series.dropna()
        if non_null.empty:
            return "text", series.astype("string")

        # Force ID-like columns to string type for key compatibility.
        if self.is_identifier_column(column_name):
            return "varchar", series.astype("string")

        as_str = non_null.astype(str).str.strip().str.lower()
        bool_values = {"true", "false", "1", "0", "yes", "no", "y", "n", "t", "f"}
        if set(as_str.unique()).issubset(bool_values):
            bool_map = {
                "true": True,
                "1": True,
                "yes": True,
                "y": True,
                "t": True,
                "false": False,
                "0": False,
                "no": False,
                "n": False,
                "f": False,
            }
            transformed = series.astype(str).str.strip().str.lower().map(bool_map)
            transformed = transformed.where(~series.isna(), None)
            return "boolean", transformed

        numeric = pd.to_numeric(series, errors="coerce")
        numeric_match = numeric.notna().sum() / len(non_null)
        if numeric_match == 1.0:
            # Treat 0/1 numeric columns as boolean (common after CSV reads with NaNs -> floats).
            unique_vals = set(numeric.dropna().unique().tolist())
            if unique_vals.issubset({0, 1, 0.0, 1.0}):
                transformed = numeric.map(lambda v: None if pd.isna(v) else bool(int(v)))
                return "boolean", transformed

            int_like = ((numeric.dropna() % 1) == 0).all()
            if int_like:
                return "bigint", numeric.astype("Int64")
            return "double precision", numeric.astype("float64")

        if self.is_likely_datetime_series(series):
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="Could not infer format, so each element will be parsed individually",
                    category=UserWarning,
                )
                datetime_series = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")

            dt_match = datetime_series.notna().sum() / len(non_null)
            if dt_match >= 0.9:
                non_null_dt = datetime_series.dropna()
                is_date_only = (
                    (non_null_dt.dt.hour == 0)
                    & (non_null_dt.dt.minute == 0)
                    & (non_null_dt.dt.second == 0)
                ).all()
                if is_date_only:
                    return "date", datetime_series.dt.date
                return "timestamptz", datetime_series

        return "text", series.astype("string")

    def create_table_sql(self, table_name: str, column_types: Dict[str, str]) -> str:
        column_defs = ", ".join([f"\"{col}\" {sql_type}" for col, sql_type in column_types.items()])
        return f"create table if not exists public.\"{table_name}\" ({column_defs});"

    def fetch_existing_columns(self, table_name: str) -> Dict[str, str]:
        if not self.db_url:
            return {}
        sql = """
            select column_name, data_type
            from information_schema.columns
            where table_schema = 'public'
              and table_name = %s
        """
        with self.connect_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (table_name,))
                rows = cur.fetchall()
        return {name: dtype for name, dtype in rows}

    def ensure_table_schema(self, table_name: str, column_types: Dict[str, str]) -> None:
        if not self.db_url:
            return

        existing = self.fetch_existing_columns(table_name)
        missing = [col for col in column_types.keys() if col not in existing]
        for col in missing:
            sql_type = column_types[col]
            alter = f'alter table public."{table_name}" add column if not exists "{col}" {sql_type};'
            with self.connect_postgres() as conn:
                with conn.cursor() as cur:
                    cur.execute(alter)

        self.migrate_boolean_columns(table_name, column_types=column_types)

    def migrate_boolean_columns(self, table_name: str, column_types: Dict[str, str]) -> None:
        existing = self.fetch_existing_columns(table_name)
        bool_cols = [col for col, typ in column_types.items() if typ == "boolean"]
        if not bool_cols:
            return

        truthy = ("1", "1.0", "true", "t", "yes", "y")
        falsy = ("0", "0.0", "false", "f", "no", "n")

        for col in bool_cols:
            current_type = existing.get(col)
            if not current_type or current_type == "boolean":
                continue

            if current_type not in ("character varying", "text", "bigint", "integer", "numeric", "double precision"):
                continue

            validate = f"""
                select count(*)
                from public."{table_name}"
                where "{col}" is not null
                  and trim("{col}"::text) <> ''
                  and lower(trim("{col}"::text)) not in %s
            """
            allowed = truthy + falsy
            with self.connect_postgres() as conn:
                with conn.cursor() as cur:
                    cur.execute(validate, (allowed,))
                    bad_count = int(cur.fetchone()[0])

            if bad_count > 0:
                print(
                    f"[WARN] Column public.{table_name}.{col} is {current_type} but has {bad_count} "
                    "non-boolean values; skipping auto-migration."
                )
                continue

            using_expr = (
                f"case "
                f"when \"{col}\" is null then null "
                f"when trim(\"{col}\"::text) = '' then null "
                f"when lower(trim(\"{col}\"::text)) in {truthy!r} then true "
                f"when lower(trim(\"{col}\"::text)) in {falsy!r} then false "
                f"else null end"
            )
            alter = f'alter table public."{table_name}" alter column "{col}" type boolean using ({using_expr});'
            with self.connect_postgres() as conn:
                with conn.cursor() as cur:
                    cur.execute(alter)

            print(f"[OK] Migrated public.{table_name}.{col} to boolean")

    def create_table(self, table_name: str, column_types: Dict[str, str]) -> None:
        if not self.db_url:
            return
        sql = self.create_table_sql(table_name, column_types)
        with self.connect_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

    def records_from_dataframe(self, df: pd.DataFrame) -> List[Dict]:
        records: List[Dict] = []
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

    def normalize_db_value(self, value):
        if pd.isna(value):
            return None

        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()

        # Convert numpy scalars to Python primitives when possible.
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass

        return value

    def load_to_postgres(self, table_name: str, df: pd.DataFrame, batch_size: int = 500) -> None:
        if not self.db_url:
            raise ValueError("DB URL is not configured")

        if df.empty:
            return

        columns = list(df.columns)
        if not columns:
            return

        column_list = ", ".join([f"\"{col}\"" for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"insert into public.\"{table_name}\" ({column_list}) values ({placeholders})"

        with self.connect_postgres() as conn:
            with conn.cursor() as cur:
                for start in range(0, len(df), batch_size):
                    chunk = df.iloc[start : start + batch_size]
                    rows = []
                    for _, row in chunk.iterrows():
                        rows.append(tuple(self.normalize_db_value(row[col]) for col in columns))
                    if rows:
                        cur.executemany(sql, rows)

    def http_post_json(self, url: str, headers: Dict[str, str], payload) -> Tuple[int, str]:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(url, data=data, headers=headers, method="POST")
        try:
            with urlopen(request, timeout=self.http_timeout_s) as resp:
                return resp.status, resp.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            return exc.code, exc.read().decode("utf-8", errors="replace")
        except URLError as exc:
            raise RuntimeError(f"HTTP request failed: {exc}") from exc

    def load_to_supabase_rest(self, table_name: str, df: pd.DataFrame, batch_size: int = 500) -> None:
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) must be set in .env")

        records = self.records_from_dataframe(df)
        if not records:
            return

        url = f"{self.supabase_url.rstrip('/')}/rest/v1/{table_name}"
        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

        for start in range(0, len(records), batch_size):
            chunk = records[start : start + batch_size]
            status, body = self.http_post_json(url=url, headers=headers, payload=chunk)
            if status in (200, 201, 204):
                continue
            raise RuntimeError(body or f"Supabase REST insert failed with HTTP {status}")

    def process_file(self, csv_path: Path) -> None:
        table_name = sanitize_identifier(csv_path.stem, "table")
        raw_df = pd.read_csv(csv_path, low_memory=False)
        cleaned_df = self.clean_dataframe(raw_df)

        if cleaned_df.empty:
            print(f"[SKIP] {csv_path.name}: no rows after cleaning")
            return
        if len(cleaned_df.columns) == 0:
            print(f"[SKIP] {csv_path.name}: no usable columns after cleaning")
            return

        transformed = {}
        column_types: Dict[str, str] = {}
        for column in cleaned_df.columns:
            sql_type, transformed_series = self.infer_sql_type_and_transform(column, cleaned_df[column])
            transformed[column] = transformed_series
            column_types[column] = sql_type
        transformed_df = pd.DataFrame(transformed)

        if self.use_postgres:
            try:
                self.create_table(table_name, column_types)
                self.ensure_table_schema(table_name, column_types)
            except Exception as exc:
                print(f"[WARN] CREATE TABLE failed for {table_name}: {exc}")
                print(
                    "[HINT] Verify SUPABASE_DB_URL (recommended) or SUPABASE_DB_PASSWORD. "
                    "Some networks block outbound port 5432."
                )
                raise

            self.load_to_postgres(table_name, transformed_df)
            print(f"[OK] {csv_path.name} -> public.{table_name} ({len(transformed_df)} rows)")
            return

        print(f"[WARN] Skipping CREATE TABLE for {table_name} (DB URL not configured).")

        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            try:
                self.load_to_supabase_rest(table_name, transformed_df)
                print(f"[OK] {csv_path.name} -> public.{table_name} ({len(transformed_df)} rows)")
                break
            except Exception as exc:
                is_schema_cache_issue = "PGRST205" in str(exc)
                if is_schema_cache_issue and attempt < max_attempts:
                    print(
                        f"[WARN] Supabase schema cache not ready for {table_name}. "
                        f"Retrying ({attempt}/{max_attempts})..."
                    )
                    time.sleep(2)
                    continue
                print(f"[ERROR] Load failed for {csv_path.name}: {exc}")
                print(
                    "[HINT] Table may not exist yet. Set SUPABASE_DB_URL (recommended) to enable automatic table "
                    "creation, or create the table manually in Supabase before running REST mode."
                )
                break

    def run(self) -> None:
        csv_files = self.discover_csv_files()
        if not csv_files:
            print(f"No CSV files found in: {self.input_path}")
            return

        for csv_file in csv_files:
            self.process_file(csv_file)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto ETL CSV files to Supabase")
    parser.add_argument(
        "--input-path",
        default=str(Path(__file__).resolve().parent / "csv-files"),
        help="Path to a CSV file or a directory to recursively scan for CSV files",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    etl = CSVToSupabaseETL(input_path=Path(args.input_path))
    etl.run()