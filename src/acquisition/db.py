import sqlite3
from datetime import datetime
from typing import List
from .config import DB_PATH

def get_connection():
    return sqlite3.connect(DB_PATH)

def init_db():
    """Initialize the SQLite database schema."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            doi TEXT,
            folder_name TEXT NOT NULL,
            matched_extensions TEXT,
            download_date TEXT NOT NULL,
            total_files INTEGER,
            total_size_bytes INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_url TEXT,
            download_timestamp TEXT,
            local_dir_name TEXT NOT NULL,
            local_file_name TEXT NOT NULL,
            context_repository TEXT,
            license TEXT,
            uploader_name TEXT,
            uploader_email TEXT,
            doi TEXT,
            file_type TEXT,
            year TEXT,
            author TEXT
        )
    ''')
    conn.commit()
    conn.close()

def is_downloaded(record_id: int) -> bool:
    """Check if a Zenodo record ID is already mapped in DB."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM records WHERE id = ?', (record_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def mark_downloaded(record_id: int, title: str, doi: str, folder_name: str, matched_extensions: List[str], total_files: int, total_size_bytes: int):
    """Mark a record as fully downloaded in config db."""
    conn = get_connection()
    cursor = conn.cursor()
    
    ext_str = ",".join(matched_extensions)
    now_str = datetime.now().isoformat()
    
    cursor.execute('''
        INSERT OR REPLACE INTO records 
        (id, title, doi, folder_name, matched_extensions, download_date, total_files, total_size_bytes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (record_id, title, doi, folder_name, ext_str, now_str, total_files, total_size_bytes))
    
    conn.commit()
    conn.close()

def insert_file_metadata(
    file_url: str,
    local_dir_name: str,
    local_file_name: str,
    context_repository: str,
    license: str,
    uploader_name: str,
    uploader_email: str,
    doi: str,
    file_type: str,
    year: str,
    author: str
):
    """Insert a new file metadata record."""
    conn = get_connection()
    cursor = conn.cursor()
    now_str = datetime.now().isoformat()
    
    cursor.execute('''
        INSERT INTO file_metadata 
        (file_url, download_timestamp, local_dir_name, local_file_name, context_repository, 
         license, uploader_name, uploader_email, doi, file_type, year, author)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        file_url or "", 
        now_str, 
        local_dir_name or "", 
        local_file_name or "", 
        context_repository or "", 
        license or "", 
        uploader_name or "", 
        uploader_email or "", 
        doi or "", 
        file_type or "", 
        year or "", 
        author or ""
    ))
    
    conn.commit()
    conn.close()
