"""
TeraDrip Salon GUI Launcher
Starts the backend server and opens the dashboard in the browser.
"""
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BACKEND_PATH = PROJECT_ROOT / "src" / "3-back-end" / "backend.py"
FRONTEND_URL = "http://localhost:5000"


def check_dependencies() -> bool:
    """Check if required packages are installed."""
    required_imports = {
        "flask": "flask",
        "flask-cors": "flask_cors",
        "pandas": "pandas",
        "supabase": "supabase",
        "python-dotenv": "dotenv",
        "psycopg[binary]": "psycopg",
    }
    missing = []

    for package, import_name in required_imports.items():
        try:
            __import__(import_name)
        except ImportError:
            missing.append(package)

    if missing:
        print(f"Missing packages: {', '.join(missing)}")
        print(f"Run: pip install -r {PROJECT_ROOT / 'requirements.txt'}")
        return False

    return True


def start_backend() -> None:
    """Start the Flask backend server."""
    print("\n" + "=" * 50)
    print("TeraDrip Salon - Starting Application")
    print("=" * 50)

    # Run backend as a script so __file__ and relative paths work as expected.
    subprocess.run(
        [sys.executable, str(BACKEND_PATH)],
        cwd=str(PROJECT_ROOT),
        check=True,
    )


def open_browser_after_delay(url: str, delay: int = 2) -> None:
    """Open browser after a short delay to let server start."""
    time.sleep(delay)
    print(f"Opening browser at {url}")
    webbrowser.open(url)


def main() -> None:
    """Main entry point."""
    print("\nTeraDrip Salon Dashboard Launcher")
    print("-" * 40)

    if not check_dependencies():
        print("\nPlease install missing dependencies first.")
        input("Press Enter to exit...")
        return

    browser_thread = threading.Thread(
        target=open_browser_after_delay,
        args=(FRONTEND_URL, 2),
        daemon=True,
    )
    browser_thread.start()

    try:
        start_backend()
    except KeyboardInterrupt:
        print("\n\nServer stopped.")
    except subprocess.CalledProcessError as e:
        print(f"\nError: backend exited with code {e.returncode}")
    except Exception as e:
        print(f"\nError: {e}")
        input("Press Enter to exit...")


if __name__ == "__main__":
    main()
