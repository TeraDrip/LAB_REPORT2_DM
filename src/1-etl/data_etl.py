import argparse
import os
import re
import time
import warnings
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import psycopg
from dotenv import load_dotenv
from supabase import Client, create_client


def sanitize_identifier(raw_name: str, fallback: str = "column") -> str:
    value = re.sub(r"[^a-zA-Z0-9_]+", "_", str(raw_name).strip().lower())
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        value = fallback
    if value[0].isdigit():
        value = f"{fallback}_{value}"
    return value


class CSVToSupabaseETL:
    def __init__(self, input_path: Path):
        self.project_root = Path(__file__).resolve().parent.parent.parent
        self.input_path = input_path.resolve()
        self.env_path = self.project_root / ".env"
        load_dotenv(dotenv_path=self.env_path)

        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) must be set in .env")
        self.supabase: Client = create_client(supabase_url, supabase_key)

        self.db_url = self.resolve_db_url(supabase_url)
        if not self.db_url:
            print(
                "[WARN] No DB URL found. Table auto-creation is disabled. "
                "Set SUPABASE_DB_URL or SUPABASE_DB_PASSWORD in .env to enable CREATE TABLE."
            )

    def resolve_db_url(self, supabase_url: str) -> str | None:
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

        # Build default Supabase Postgres URL from project ref in SUPABASE_URL.
        # Example host: https://<project_ref>.supabase.co
        project_ref = ""
        try:
            host = supabase_url.replace("https://", "").replace("http://", "")
            project_ref = host.split(".")[0]
        except Exception:
            project_ref = ""

        if not project_ref:
            return None

        return f"postgresql://postgres:{db_password}@db.{project_ref}.supabase.co:5432/postgres"

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
        normalized = column_name.strip().lower()
        if "id" not in normalized:
            return False
        return True

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

    def create_table(self, table_name: str, column_types: Dict[str, str]) -> None:
        if not self.db_url:
            return
        sql = self.create_table_sql(table_name, column_types)
        with psycopg.connect(self.db_url, autocommit=True) as conn:
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

    def load_to_supabase(self, table_name: str, df: pd.DataFrame, batch_size: int = 500) -> None:
        records = self.records_from_dataframe(df)
        if not records:
            return

        for start in range(0, len(records), batch_size):
            chunk = records[start : start + batch_size]
            self.supabase.table(table_name).insert(chunk).execute()

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

        if self.db_url:
            try:
                self.create_table(table_name, column_types)
            except Exception as exc:
                print(f"[WARN] CREATE TABLE failed for {table_name}: {exc}")
                print("[WARN] Continuing without table creation for this run.")
                self.db_url = None
        if not self.db_url:
            print(f"[WARN] Skipping CREATE TABLE for {table_name} (DB URL not configured).")

        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            try:
                self.load_to_supabase(table_name, transformed_df)
                print(f"[OK] {csv_path.name} -> public.{table_name} ({len(transformed_df)} rows)")
                break
            except Exception as exc:
                is_schema_cache_issue = "PGRST205" in str(exc)
                if is_schema_cache_issue and attempt < max_attempts:
                    print(f"[WARN] Supabase schema cache not ready for {table_name}. Retrying ({attempt}/{max_attempts})...")
                    time.sleep(2)
                    continue
                print(f"[ERROR] Load failed for {csv_path.name}: {exc}")
                if not self.db_url:
                    print(
                        "[HINT] Table may not exist yet. Add SUPABASE_DB_URL or SUPABASE_DB_PASSWORD in .env "
                        "to enable automatic table creation."
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
