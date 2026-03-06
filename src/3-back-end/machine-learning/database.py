import os
from pathlib import Path
import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

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


env_path = Path(__file__).resolve().parent.parent.parent.parent / ".env"
load_env_file(env_path, override=False)

def get_supabase_rest_config() -> tuple[str, str]:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) must be set in .env")

    return url.rstrip("/"), key


def select_table(table_name: str, limit: int | None = None) -> list[dict]:
    base_url, key = get_supabase_rest_config()
    url = f"{base_url}/rest/v1/{table_name}?select=*"
    if limit is not None:
        url += f"&limit={int(limit)}"

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
    }

    request = Request(url, headers=headers, method="GET")
    try:
        with urlopen(request, timeout=30) as resp:
            payload = resp.read().decode("utf-8", errors="replace")
            return json.loads(payload) if payload else []
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(body or f"Supabase REST select failed with HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"HTTP request failed: {exc}") from exc