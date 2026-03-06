"""
TeraDrip Salon GUI Launcher
Starts the backend server and opens the dashboard in the browser.
"""
import os
import sys
import time
import webbrowser
import subprocess
import threading
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BACKEND_PATH = PROJECT_ROOT / "src" / "3-back-end" / "backend.py"
FRONTEND_URL = "http://localhost:5000"


def check_dependencies():
    """Check if required packages are installed."""
    required = ["flask", "flask_cors", "pandas", "supabase", "dotenv"]
    missing = []
    
    for package in required:
        try:
            __import__(package.replace("-", "_"))
        except ImportError:
            missing.append(package)
    
    if missing:
        print(f"⚠️  Missing packages: {', '.join(missing)}")
        print(f"   Run: pip install -r {PROJECT_ROOT / 'lib.txt'}")
        return False
    return True


def start_backend():
    """Start the Flask backend server."""
    print("\n" + "=" * 50)
    print("🚀 TeraDrip Salon - Starting Application")
    print("=" * 50)
    
    # Change to backend directory
    os.chdir(BACKEND_PATH.parent)
    
    # Import and run Flask app
    sys.path.insert(0, str(BACKEND_PATH.parent))
    
    # Use exec to run the backend
    exec(open(BACKEND_PATH).read())


def open_browser_after_delay(url, delay=2):
    """Open browser after a short delay to let server start."""
    time.sleep(delay)
    print(f"🌐 Opening browser at {url}")
    webbrowser.open(url)


def main():
    """Main entry point."""
    print("\n💧 TeraDrip Salon Dashboard Launcher")
    print("-" * 40)
    
    # Check dependencies
    if not check_dependencies():
        print("\n❌ Please install missing dependencies first.")
        input("Press Enter to exit...")
        return
    
    # Open browser in background thread
    browser_thread = threading.Thread(
        target=open_browser_after_delay, 
        args=(FRONTEND_URL, 2),
        daemon=True
    )
    browser_thread.start()
    
    # Start backend (this blocks)
    try:
        start_backend()
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped. Goodbye!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        input("Press Enter to exit...")


if __name__ == "__main__":
    main()
