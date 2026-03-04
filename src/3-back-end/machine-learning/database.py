import os
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv

# --- FIX: Point to the .env in the root folder ---
# This finds the directory 2 levels up from 'src/3-back-end/'
env_path = Path(__file__).resolve().parent.parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

def get_supabase() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        # This is the error you were seeing
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env")

    return create_client(url, key)