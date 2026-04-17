import re
import sqlite3
from datetime import datetime
from typing import List, Optional, Tuple
from .config import DB_PATH

# Repository ID Mapping (Hardcoded map linking text repo names to integer IDs)
REPO_ID_MAP = {
    "Zenodo": 1,
    "Dataverse": 2,
    "DataverseNO": 3,
    "Borealis": 4,
    "AUSSDA": 5,
    "Heidata": 6,
    "Figshare": 7,
    "OSF": 8,
    "QDR": 9,
    "DANS": 10,
    "ADA": 11
}

def get_connection():
    return sqlite3.connect(DB_PATH)

def init_db():
    """Initialize the SQLite database schema with foreign keys."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    # 1. Project Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY,
            query_string TEXT,
            repository_id INTEGER,
            repository_url TEXT,
            project_url TEXT,
            version TEXT,
            title TEXT,
            description TEXT,
            language TEXT,
            doi TEXT,
            upload_date DATE,
            download_date TIMESTAMP,
            download_repository_folder TEXT,
            download_project_folder TEXT,
            download_version_folder TEXT,
            download_method TEXT
        )
    ''')

    # 2. Files Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER,
            file_name TEXT,
            file_type TEXT,
            status TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
        )
    ''')

    # 3. Keywords Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER,
            keyword TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
        )
    ''')

    # 4. Person_Role Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PERSON_ROLE (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER,
            name TEXT,
            role TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
        )
    ''')

    # 5. Licenses Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS licenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER,
            license TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
        )
    ''')
    
    conn.commit()
    conn.close()

def is_downloaded(project_id: int) -> bool:
    """Check if a project ID is already mapped in DB."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM projects WHERE id = ?', (project_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def insert_project(
    project_id: int,
    query_string: str,
    repository_name: str,
    repository_url: str,
    project_url: str,
    version: str,
    title: str,
    description: str,
    language: str,
    doi: str,
    upload_date: str,
    download_repository_folder: str,
    download_project_folder: str,
    download_version_folder: str,
    download_method: str = "API-CALL"
):
    """Insert a new project record."""
    conn = get_connection()
    cursor = conn.cursor()
    
    repository_id = REPO_ID_MAP.get(repository_name, 0)
    now_str = datetime.now().isoformat()
    
    cursor.execute('''
        INSERT OR REPLACE INTO projects 
        (id, query_string, repository_id, repository_url, project_url, version, title, 
         description, language, doi, upload_date, download_date, download_repository_folder, 
         download_project_folder, download_version_folder, download_method)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        project_id,
        query_string or "",
        repository_id,
        repository_url or "",
        project_url or "",
        version or "",
        title or "",
        description or "",
        language or "",
        doi or "",
        upload_date or "",
        now_str,
        download_repository_folder or "",
        download_project_folder or "",
        download_version_folder or "",
        download_method
    ))
    
    conn.commit()
    conn.close()
    return project_id

# Allowed values for files.status
_STATUS_MAP = {
    "SUCCESS":   "SUCCEEDED",
    "SUCCEEDED": "SUCCEEDED",
    "FAILED_SERVER_UNRESPONSIVE": "FAILED_SERVER_UNRESPONSIVE",
    "FAILED_LOGIN_REQUIRED":      "FAILED_LOGIN_REQUIRED",
    "FAILED_TOO_LARGE":           "FAILED_TOO_LARGE",
}

def _normalize_status(status: str) -> str:
    """Map raw status strings to the required canonical values."""
    return _STATUS_MAP.get(status, "SUCCEEDED")

def insert_file(project_id: int, file_name: str, file_type: str, status: str = "SUCCEEDED"):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO files (project_id, file_name, file_type, status)
        VALUES (?, ?, ?, ?)
    ''', (project_id, file_name or "", file_type or "", _normalize_status(status)))
    conn.commit()
    conn.close()

def insert_keyword(project_id: int, keyword: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO keywords (project_id, keyword)
        VALUES (?, ?)
    ''', (project_id, keyword or ""))
    conn.commit()
    conn.close()

# Allowed values for PERSON_ROLE.role
_ROLE_MAP = {
    # UPLOADER
    "uploader":  "UPLOADER",
    "UPLOADER":  "UPLOADER",
    # AUTHOR
    "author":    "AUTHOR",
    "Author":    "AUTHOR",
    "creator":   "AUTHOR",
    "Creator":   "AUTHOR",
    # OWNER
    "owner":     "OWNER",
    "Owner":     "OWNER",
    # OTHER (all known scraped roles that don't fit above)
    "contact":             "OTHER",
    "ContactPerson":       "OTHER",
    "contributor":         "OTHER",
    "Researcher":          "OTHER",
    "Data Collector":      "OTHER",
    "DataCollector":       "OTHER",
    "Supervisor":          "OTHER",
    "Project Member":      "OTHER",
    "ProjectMember":       "OTHER",
    "Data Curator":        "OTHER",
    "DataCurator":         "OTHER",
    "Project Leader":      "OTHER",
    "ProjectLeader":       "OTHER",
    "Data Manager":        "OTHER",
    "Project Manager":     "OTHER",
    "ProjectManager":      "OTHER",
    "Editor":              "OTHER",
    "Funder":              "OTHER",
    "Hosting Institution": "OTHER",
    "HostingInstitution":  "OTHER",
    "Related Person":      "OTHER",
    "Research Group":      "OTHER",
    "ResearchGroup":       "OTHER",
    "Sponsor":             "OTHER",
    "Other":               "OTHER",
    "other":               "OTHER",
}

def _normalize_role(role: str) -> str:
    """Map raw scraped role strings to the required canonical values."""
    return _ROLE_MAP.get(role, "UNKNOWN")

def insert_person_role(project_id: int, name: str, role: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO PERSON_ROLE (project_id, name, role)
        VALUES (?, ?, ?)
    ''', (project_id, name or "", _normalize_role(role or "")))
    conn.commit()
    conn.close()

# Common CC/SPDX normalisation map (case-insensitive pattern → canonical ID)
_SPDX_MAP = [
    (r'^cc0[\s\-_]?1\.0$',                  'CC0-1.0'),
    (r'^cc[\s\-]?zero$',                     'CC0-1.0'),
    (r'^cc0$',                               'CC0-1.0'),
    (r'^cc[\s\-]?by[\s\-]?nc[\s\-]?nd[\s\-]?4\.0.*$', 'CC-BY-NC-ND-4.0'),
    (r'^cc[\s\-]?by[\s\-]?nc[\s\-]?sa[\s\-]?4\.0.*$', 'CC-BY-NC-SA-4.0'),
    (r'^cc[\s\-]?by[\s\-]?nc[\s\-]?4\.0.*$','CC-BY-NC-4.0'),
    (r'^cc[\s\-]?by[\s\-]?sa[\s\-]?4\.0.*$','CC-BY-SA-4.0'),
    (r'^cc[\s\-]?by[\s\-]?4\.0.*$',         'CC-BY-4.0'),
    (r'^cc[\s\-]?by$',                       'CC-BY-4.0'),
    (r'^mit[\s\-]?licen[sc]e$',              'MIT'),
    (r'^odc[\s\-]?by$',                      'ODC-By'),
]

def _clean_license(text: str) -> str:
    """Strip HTML tags and normalise common SPDX identifiers."""
    # Strip HTML tags
    text = re.sub(r'<[^>]*>', ' ', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;',  '<', text)
    text = re.sub(r'&gt;',  '>', text)
    text = re.sub(r'\s+',   ' ', text).strip()
    # Normalise to SPDX where possible
    for pattern, spdx in _SPDX_MAP:
        if re.match(pattern, text, re.IGNORECASE):
            return spdx
    return text

def insert_license(project_id: int, license_text: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO licenses (project_id, license)
        VALUES (?, ?)
    ''', (project_id, _clean_license(license_text or "")))
    conn.commit()
    conn.close()
