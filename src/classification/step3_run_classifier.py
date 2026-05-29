"""
Step 3: Run Classifier

Executes the classification pipeline on QDA_PROJECT and QD_PROJECT.
1. Creates necessary tables
2. Gathers metadata (Tier 1) and extracts text (Tier 2)
3. Runs the ISIC classifier
4. Saves results
"""

import sqlite3
import os
import re
import html
import hashlib
from tqdm import tqdm
from .classifier import classify, generate_tags
from .text_extractor import extract_text

def clean_html(text: str) -> str:
    if not text:
        return ""
    # Strip HTML tags
    clean = re.sub(r'<[^>]+>', ' ', text)
    # Unescape HTML entities (e.g. &nbsp;, &rsquo;)
    clean = html.unescape(clean)
    # Clean multiple spaces/newlines
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

def sanitize_folder_name(title: str) -> str:
    """Remove special characters and truncate to avoid OS filename length limits."""
    sanitized = re.sub(r'[^\w\s-]', '', title).strip()
    if len(sanitized) > 200:
        title_hash = hashlib.md5(title.encode()).hexdigest()[:8]
        sanitized = sanitized[:200].rstrip() + f"_{title_hash}"
    return sanitized


def create_tables(cursor):
    # project_classifications
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS project_classifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER,
        isic_section TEXT,
        isic_section_name TEXT,
        isic_division TEXT,
        isic_division_name TEXT,
        confidence_score REAL,
        input_text_summary TEXT,
        FOREIGN KEY (project_id) REFERENCES projects(id)
    )''')

    # file_classifications
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS file_classifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id INTEGER,
        project_id INTEGER,
        isic_section TEXT,
        isic_section_name TEXT,
        isic_division TEXT,
        isic_division_name TEXT,
        confidence_score REAL,
        input_text_summary TEXT,
        FOREIGN KEY (file_id) REFERENCES files(id),
        FOREIGN KEY (project_id) REFERENCES projects(id)
    )''')

    # project_tags
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS project_tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER,
        tag TEXT,
        source TEXT,
        FOREIGN KEY (project_id) REFERENCES projects(id)
    )''')

def get_project_metadata(cursor, project_id):
    # Get title and description
    cursor.execute("SELECT title, description FROM projects WHERE id = ?", (project_id,))
    row = cursor.fetchone()
    title = row[0] if row and row[0] else ""
    desc = row[1] if row and row[1] else ""
    
    # Get keywords
    cursor.execute("SELECT keyword FROM keywords WHERE project_id = ?", (project_id,))
    keywords = [r[0] for r in cursor.fetchall() if r[0]]
    
    return title, desc, keywords

def run(db_path: str, data_dir: str):
    from .isic_taxonomy import SECTIONS, DIVISIONS
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("Creating tables...")
    create_tables(cursor)
    
    # Clear existing classifications for a fresh run
    cursor.execute("DELETE FROM project_classifications")
    cursor.execute("DELETE FROM file_classifications")
    cursor.execute("DELETE FROM project_tags")
    conn.commit()

    # Get target projects
    cursor.execute("SELECT id FROM projects WHERE type IN ('QDA_PROJECT', 'QD_PROJECT')")
    project_ids = [r[0] for r in cursor.fetchall()]
    
    print(f"Starting classification for {len(project_ids)} qualitative projects...")
    
    for pid in tqdm(project_ids):
        title, desc, keywords_list = get_project_metadata(cursor, pid)
        cleaned_desc = clean_html(desc)
        keywords_str = ", ".join(keywords_list)
        
        # Tier 1 Metadata text
        tier1_text = f"Title: {title}\nDescription: {cleaned_desc}\nKeywords: {keywords_str}"
        
        # Look for files associated with the project to extract Tier 2 text
        cursor.execute("SELECT id, file_name, file_type FROM files WHERE project_id = ?", (pid,))
        files = cursor.fetchall()
        
        project_tier2_texts = []
        
        for fid, fname, ftype in files:
            file_text = ""
            
            # Since QDA Folders are named after the sanitized project title, we resolve it
            folder_name = sanitize_folder_name(title)
            possible_paths = [
                os.path.join(data_dir, "QDA Folders", folder_name, str(fname)),
                os.path.join(data_dir, "QDA Folders", str(fname)),
            ]
            
            extracted = ""
            for p in possible_paths:
                if os.path.exists(p):
                    extracted = extract_text(p)
                    if extracted:
                        break
            
            # Combine Tier 1 metadata + filename + any extracted text for the file
            file_input_text = f"{tier1_text}\\nFile: {fname}\\nContent: {extracted}"
            
            # Run file-level classification
            f_sec, f_div, f_score = classify(file_input_text)
            
            if f_sec and f_div:
                cursor.execute('''
                    INSERT INTO file_classifications 
                    (file_id, project_id, isic_section, isic_section_name, isic_division, isic_division_name, confidence_score, input_text_summary)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    fid, pid, 
                    f_sec, SECTIONS.get(f_sec, ''), 
                    f_div, DIVISIONS.get(f_div, {}).get('title', ''), 
                    f_score, file_input_text[:200]
                ))
            
            if extracted:
                project_tier2_texts.append(extracted)
                
        # Run project-level classification
        project_input_text = tier1_text
        if project_tier2_texts:
            # Add up to 5000 chars of extracted text to the project classification context
            combined_extracted = "\\n---\\n".join(project_tier2_texts)[:5000]
            project_input_text += f"\\nExtracted Data: {combined_extracted}"
            
        p_sec, p_div, p_score = classify(project_input_text)
        
        if p_sec and p_div:
            cursor.execute('''
                INSERT INTO project_classifications 
                (project_id, isic_section, isic_section_name, isic_division, isic_division_name, confidence_score, input_text_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                pid, 
                p_sec, SECTIONS.get(p_sec, ''), 
                p_div, DIVISIONS.get(p_div, {}).get('title', ''), 
                p_score, project_input_text[:200]
            ))
            
        # Generate tags
        tags = generate_tags(project_input_text, keywords_str)
        for tag in tags:
            cursor.execute('''
                INSERT INTO project_tags (project_id, tag, source)
                VALUES (?, ?, ?)
            ''', (pid, tag, 'database_keywords'))
            
        conn.commit()

    conn.close()
    print("\\n✅ Step 3: Text Extraction & Classification Run complete.")

if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    db_path = os.path.join(project_root, '23359384-sq26-classification.db')
    data_dir = os.path.join(project_root, 'data')
    
    run(db_path, data_dir)
