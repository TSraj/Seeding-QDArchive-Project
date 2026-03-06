import argparse
import sys
import logging
from pathlib import Path
from .db import init_db
from .config import TARGET_EXTENSIONS, MAX_PAGES, PROJECT_ROOT, SCRAPERS_CONFIG

# Import scrapers
from . import zenodo_scraper
from . import dataverse_scraper
from . import dataverse_no_scraper
from . import borealis_scraper
from . import aussda_scraper
from . import heidata_scraper
from . import figshare_scraper
from . import osf_scraper
from . import qdr_scraper
from . import dans_scraper
from . import ada_scraper

LOG_FILE = PROJECT_ROOT / "scraper.log"

def setup_logging():
    """
    Set up logging to both console and a log file.
    The log file is RESET (overwritten) on every run so you always
    see the current session from the beginning.
    """
    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(message)s")

    # File handler — mode='w' truncates the file on each run
    file_handler = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Console handler — so you still see output in the terminal
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Redirect print() calls to the log as well
    class LogWriter:
        def __init__(self, logger, level):
            self.logger = logger
            self.level = level
            self._buffer = ""

        def write(self, message):
            if message and message.strip():
                self.logger.log(self.level, message.rstrip())

        def flush(self):
            pass

    sys.stdout = LogWriter(root_logger, logging.INFO)

def main():
    parser = argparse.ArgumentParser(description="Qualitative Data Scraper Pipeline")
    parser.add_argument("--extensions", type=str, help="Comma-separated list of extensions to search (default: all from config)")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES, help="Max pages per query to paginate")
    parser.add_argument("--dry-run", action="store_true", help="Search and report matches without actually downloading anything")
    parser.add_argument("--max-runtime-minutes", type=float, default=None, help="Maximum number of minutes for the scraper to run (e.g. 10)")
    parser.add_argument("--max-runtime-hours", type=float, default=None, help="Maximum number of hours for the scraper to run (e.g. 2.5)")
    
    args = parser.parse_args()
    
    # Set up logging — resets the log file on every run
    setup_logging()
    
    # Ensure database is set up
    init_db()
    
    # Resolve runtime: prefer minutes if both given, convert to hours for internal use
    max_runtime_hours = args.max_runtime_hours
    if args.max_runtime_minutes is not None:
        max_runtime_hours = args.max_runtime_minutes / 60.0
    
    # Determine the extensions to search
    extensions = TARGET_EXTENSIONS
    if args.extensions:
        extensions = [ext.strip().lower() for ext in args.extensions.split(",") if ext.strip()]
    
    # Display runtime in a human-friendly way
    if max_runtime_hours is not None:
        total_minutes = max_runtime_hours * 60
        if total_minutes >= 60:
            runtime_display = f"{max_runtime_hours:.2f} hours ({total_minutes:.0f} min)"
        else:
            runtime_display = f"{total_minutes:.0f} minutes"
    else:
        runtime_display = "unlimited"
        
    print(f"Starting Scraper Pipeline...")
    print(f"Log file: {LOG_FILE} (reset on each run)")
    print(f"Dry run: {args.dry_run}")
    print(f"Max pages/query: {args.max_pages}")
    print(f"Max runtime: {runtime_display}")
    print(f"Target extensions mapped: {len(extensions)}")
    
    # Evaluate configured scrapers and run them
    scrapers_run = 0
    if SCRAPERS_CONFIG.get("zenodo", False):
        print("\n=================================")
        print("  Starting Zenodo Scraper")
        print("=================================")
        zenodo_scraper.scrape(
            extensions=extensions,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            max_runtime_hours=max_runtime_hours
        )
        scrapers_run += 1
        
    if SCRAPERS_CONFIG.get("dataverse", False):
        print("\n=================================")
        print("  Starting Dataverse Scraper")
        print("=================================")
        dataverse_scraper.scrape(
            extensions=extensions,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            max_runtime_hours=max_runtime_hours
        )
        scrapers_run += 1
        
    if SCRAPERS_CONFIG.get("dataverse_no", False):
        print("\n=================================")
        print("  Starting Dataverse NO Scraper")
        print("=================================")
        dataverse_no_scraper.scrape(
            extensions=extensions,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            max_runtime_hours=max_runtime_hours
        )
        scrapers_run += 1
        
    if SCRAPERS_CONFIG.get("borealis", False):
        print("\n=================================")
        print("  Starting Borealis Scraper")
        print("=================================")
        borealis_scraper.scrape(
            extensions=extensions,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            max_runtime_hours=max_runtime_hours
        )
        scrapers_run += 1
        
    if SCRAPERS_CONFIG.get("aussda", False):
        print("\n=================================")
        print("  Starting AUSSDA Scraper")
        print("=================================")
        aussda_scraper.scrape(
            extensions=extensions,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            max_runtime_hours=max_runtime_hours
        )
        scrapers_run += 1
        
    if SCRAPERS_CONFIG.get("heidata", False):
        print("\n=================================")
        print("  Starting heiDATA Scraper")
        print("=================================")
        heidata_scraper.scrape(
            extensions=extensions,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            max_runtime_hours=max_runtime_hours
        )
        scrapers_run += 1

    if SCRAPERS_CONFIG.get("figshare", False):
        print("\n=================================")
        print("  Starting Figshare Scraper")
        print("=================================")
        figshare_scraper.scrape(
            extensions=extensions,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            max_runtime_hours=max_runtime_hours
        )
        scrapers_run += 1

    if SCRAPERS_CONFIG.get("osf", False):
        print("\n=================================")
        print("  Starting OSF Scraper")
        print("=================================")
        osf_scraper.scrape(
            extensions=extensions,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            max_runtime_hours=max_runtime_hours
        )
        scrapers_run += 1
        
    if SCRAPERS_CONFIG.get("qdr", False):
        print("\n=================================")
        print("  Starting QDR Scraper")
        print("=================================")
        qdr_scraper.scrape(
            extensions=extensions,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            max_runtime_hours=max_runtime_hours
        )
        scrapers_run += 1
        
    if SCRAPERS_CONFIG.get("dans", False):
        print("\n=================================")
        print("  Starting DANS Scraper")
        print("=================================")
        dans_scraper.scrape(
            extensions=extensions,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            max_runtime_hours=max_runtime_hours
        )
        scrapers_run += 1

    if SCRAPERS_CONFIG.get("ada", False):
        print("\n=================================")
        print("  Starting ADA Scraper")
        print("=================================")
        ada_scraper.scrape(
            extensions=extensions,
            max_pages=args.max_pages,
            dry_run=args.dry_run,
            max_runtime_hours=max_runtime_hours
        )
        scrapers_run += 1
        
    if scrapers_run == 0:
        print("\n[WARNING] No scrapers are enabled in config.yaml. Exiting.")
    else:
        print("\nScraping complete.")

if __name__ == "__main__":
    main()
