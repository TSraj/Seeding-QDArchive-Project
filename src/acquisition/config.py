import os
import yaml
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Load configuration from config.yaml if it exists
CONFIG_FILE = PROJECT_ROOT / "config.yaml"
user_config = {}
if CONFIG_FILE.exists():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        user_config = yaml.safe_load(f) or {}

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
METADATA_DIR = DATA_DIR / "metadata"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)

# Database path
DB_PATH = METADATA_DIR / "qdarchive.db"

# Zenodo and Dataverse API configuration
_zenodo_settings = user_config.get("settings", {})
ZENODO_API_BASE = _zenodo_settings.get("zenodo_api_base", "https://zenodo.org/api")
DATAVERSE_API_BASE = _zenodo_settings.get("dataverse_api_base", "https://dataverse.harvard.edu/api")
DATAVERSE_NO_API_BASE = _zenodo_settings.get("dataverse_no_api_base", "https://dataverse.no/api")
BOREALIS_API_BASE = _zenodo_settings.get("borealis_api_base", "https://borealisdata.ca/api")
AUSSDA_API_BASE = _zenodo_settings.get("aussda_api_base", "https://data.aussda.at/api")
HEIDATA_API_BASE = _zenodo_settings.get("heidata_api_base", "https://heidata.uni-heidelberg.de/api")
FIGSHARE_API_BASE = "https://api.figshare.com/v2"
OSF_API_BASE = "https://api.osf.io/v2"

RATE_LIMIT_DELAY = 2.0  # seconds between API requests
MAX_PAGES = _zenodo_settings.get("max_pages", 40)          # default max pages per search query (40*25=1000)
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

# Expose active scrapers map
SCRAPERS_CONFIG = user_config.get("scrapers", {})
