import os
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
METADATA_DIR = DATA_DIR / "metadata"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)

# Database path
DB_PATH = METADATA_DIR / "qdarchive.db"

# Zenodo API configuration
ZENODO_API_BASE = "https://zenodo.org/api"
RATE_LIMIT_DELAY = 2.0  # seconds between API requests
MAX_PAGES = 40          # default max pages per search query (40*25=1000)
RESULTS_PER_PAGE = 25

# Download parallelism
MAX_DOWNLOAD_WORKERS = 4  # number of threads for parallel file downloads

# Target extensions for Qualitative Data (lowercase without leading dot)
TARGET_EXTENSIONS = [
    "qdpx", "atlproj", "nvp", "nvpx", "mqda", "mx22", "mx24",
    "ppj", "qdp", "qrk", "mqda", "mqbac", "mqtc", "mqex", "mqmtr",
    "mx24bac", "mc24", "mex24", "mx20", "mx18", "mx12", "mx11",
    "mx5", "mx4", "mx3", "mx2", "m2k", "loa", "sea", "mtr",
    "mod", "mex22", "hpr7", "pprj", "qlt", "f4p", "qpd"
]
