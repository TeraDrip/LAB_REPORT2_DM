import sys
from pathlib import Path

import pandas as pd

# 1. Path Fix: Ensure database.py can be found in the folder structure
BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

try:
    from database import select_table

    print("✓ MODULE IMPORT: Successfully imported database.py")
except ImportError:
    print("✗ ERROR: Could not find database.py. Check your folder structure.")


class SalonETL:
    def __init__(self, test_table=None):
        self.check_supabase_connection(test_table)

    def check_supabase_connection(self, test_table=None):
        """Verifies Supabase connectivity."""
        try:
            if test_table:
                select_table(test_table, limit=1)
                print(f"✓ SUPABASE CONNECTED: Successfully connected to table: {test_table}")
            else:
                print("✓ SUPABASE REST READY: Config present (No table test performed).")
            return True
        except Exception as e:
            print(f"✗ SUPABASE CONNECTION ERROR: {e}")
            return False

    def extract_from_cloud(self, table_name):
        """Pulls the latest transaction data for the pipeline."""
        try:
            data = select_table(table_name)
            print(f"✓ EXTRACT SUCCESS: Retrieved {len(data)} records from {table_name}.")
            return pd.DataFrame(data)
        except Exception as e:
            print(f"✗ EXTRACT ERROR: {e}")
            return pd.DataFrame()

    def transform_data(self, df):
        """Cleans and prepares data in Python before it reaches the UI."""
        if df.empty:
            print("! TRANSFORM INFO: Skipping, DataFrame is empty.")
            return df

        # Cleaning: Dropping metadata columns
        metadata = ["id", "created_at", "customer_transaction_id", "transaction_date", "services_availed"]
        features = df.drop(columns=[col for col in metadata if col in df.columns], errors="ignore")

        # Encoding: Convert to Boolean for the ML engine
        transformed = features.astype(bool)
        print(f"✓ TRANSFORM SUCCESS: Cleaned data into shape {transformed.shape}.")
        return transformed


# --- 2. THE OUTPUT TEST (This makes it 'work' in the terminal) ---
if __name__ == "__main__":
    print("\n--- STARTING SALON PIPELINE TEST ---")

    # Replace 'customer_dataset' with your actual table name to see a live test
    etl = SalonETL(test_table="customer_dataset")

    # Optional: Test a full Extract -> Transform loop
    # raw_data = etl.extract_from_cloud("customer_transactions")
    # clean_data = etl.transform_data(raw_data)

    print("--- TEST COMPLETE ---\n")
