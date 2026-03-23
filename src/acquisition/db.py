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
        CREATE TABLE IF NOT EXISTS person_roles (
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

def insert_file(project_id: int, file_name: str, file_type: str, status: str = "SUCCESS"):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO files (project_id, file_name, file_type, status)
        VALUES (?, ?, ?, ?)
    ''', (project_id, file_name or "", file_type or "", status))
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

def insert_person_role(project_id: int, name: str, role: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO person_roles (project_id, name, role)
        VALUES (?, ?, ?)
    ''', (project_id, name or "", role or ""))
    conn.commit()
    conn.close()

def insert_license(project_id: int, license_text: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO licenses (project_id, license)
        VALUES (?, ?)
    ''', (project_id, license_text or ""))
    conn.commit()
    conn.close()
