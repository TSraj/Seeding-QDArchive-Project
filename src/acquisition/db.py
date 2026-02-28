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
