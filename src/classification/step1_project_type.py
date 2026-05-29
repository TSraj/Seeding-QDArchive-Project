"""
Step 1: PROJECT_TYPE Classification

Classifies all projects in the database into one of:
  - QDA_PROJECT: Contains at least one QDA file (NVivo, ATLAS.ti, MAXQDA, QDPX, etc.)
  - QD_PROJECT: Not QDA, but contains primary qualitative data files
  - OTHER_PROJECT: Not QD, but contains valid structured/quantitative data files
  - NOT_A_PROJECT: No recognizable data files

Uses a cascade logic based on file extensions from the 'files' table.
Writes results to the 'type' column in the 'projects' table.

Usage:
    python -m src.classification.step1_project_type
"""

import sqlite3
import os

# ---------------------------------------------------------------------------
# File extension definitions (all lowercase for case-insensitive matching)
# ---------------------------------------------------------------------------

# QDA project files — qualitative data analysis software formats
QDA_EXTENSIONS = {
    'qdpx',                          # Universal QDA exchange format
    'nvp', 'nvpx',                   # NVivo
    'atlproj', 'atlproj9', 'atlproj23', 'hpr7',  # ATLAS.ti
    'mx3', 'mx4', 'mx12', 'mx18', 'mx20', 'mx22', 'mx24',  # MAXQDA
    'mex', 'mex24',                  # MAXQDA exchange
}

# Primary data files — qualitative source material
# NOTE: Audio/video/image are included here for PROJECT_TYPE classification only.
#       For ISIC classification (Steps 2-3), only text-based formats are extracted.
PRIMARY_DATA_EXTENSIONS = {
    # Text-based (Tier 2 — extractable for ISIC classification)
    'txt', 'rtf', 'docx', 'doc', 'pdf', 'odt',
    # Audio
    'mp3', 'wav', 'm4a', 'wma',
    # Video
    'mp4', 'mov', 'avi',
    # Image
    'jpg', 'jpeg', 'png', 'tif', 'tiff', 'heic',
    # Subtitle/transcript
    'vtt',
}

# Valid data files — structured/quantitative data
VALID_DATA_EXTENSIONS = {
    # Spreadsheets
    'csv', 'tsv', 'xlsx', 'xls', 'xlsb', 'xlsm',
    # Statistical software
    'sav', 'zsav', 'dta', 'sps', 'do', 'sas',
    # R data
    'rdata', 'rda', 'rds', 'r',
    # Programming/data
    'py', 'json', 'jsonl', 'xml',
    # Database/GIS
    'dbf',
    # Scientific
    'mat', 'nc', 'h5',
}


def classify_project(file_types: set) -> str:
    """
    Classify a project based on its file extensions using cascade logic.

    Args:
        file_types: Set of lowercase file extensions for the project's files

    Returns:
        One of: 'QDA_PROJECT', 'QD_PROJECT', 'OTHER_PROJECT', 'NOT_A_PROJECT'
    """
    if file_types & QDA_EXTENSIONS:
        return 'QDA_PROJECT'
    elif file_types & PRIMARY_DATA_EXTENSIONS:
        return 'QD_PROJECT'
    elif file_types & VALID_DATA_EXTENSIONS:
        return 'OTHER_PROJECT'
    else:
        return 'NOT_A_PROJECT'


def run(db_path: str):
    """
    Run PROJECT_TYPE classification on all projects in the database.

    Args:
        db_path: Path to the SQLite database file
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # --- Add 'type' column if it doesn't exist ---
    cursor.execute("PRAGMA table_info(projects)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'type' not in columns:
        cursor.execute("ALTER TABLE projects ADD COLUMN type TEXT")
        conn.commit()
        print("Added 'type' column to projects table.")
    else:
        print("'type' column already exists.")

    # --- Get all projects ---
    cursor.execute("SELECT id FROM projects")
    project_ids = [row[0] for row in cursor.fetchall()]
    print(f"\nTotal projects to classify: {len(project_ids)}")

    # --- Classify each project ---
    counts = {'QDA_PROJECT': 0, 'QD_PROJECT': 0, 'OTHER_PROJECT': 0, 'NOT_A_PROJECT': 0}

    for project_id in project_ids:
        # Get all file extensions for this project (case-insensitive)
        cursor.execute(
            "SELECT file_type FROM files WHERE project_id = ?",
            (project_id,)
        )
        file_types = {
            row[0].lower().strip() if row[0] else ''
            for row in cursor.fetchall()
        }
        # Remove empty strings
        file_types.discard('')

        project_type = classify_project(file_types)
        counts[project_type] += 1

        cursor.execute(
            "UPDATE projects SET type = ? WHERE id = ?",
            (project_type, project_id)
        )

    conn.commit()

    # --- Print summary ---
    print("\n" + "=" * 60)
    print("PROJECT_TYPE Classification Summary")
    print("=" * 60)
    total = sum(counts.values())
    for ptype, count in counts.items():
        pct = (count / total * 100) if total > 0 else 0
        print(f"  {ptype:<20s} {count:>5d}  ({pct:5.1f}%)")
    print(f"  {'TOTAL':<20s} {total:>5d}")

    # --- Breakdown by repository ---
    print("\n" + "-" * 60)
    print("Breakdown by Repository")
    print("-" * 60)
    cursor.execute("""
        SELECT p.repository_id, p.type, COUNT(*) as cnt
        FROM projects p
        GROUP BY p.repository_id, p.type
        ORDER BY p.repository_id, p.type
    """)
    rows = cursor.fetchall()

    # Get repository URLs for display
    cursor.execute("SELECT DISTINCT repository_id, repository_url FROM projects")
    repo_map = {row[0]: row[1] for row in cursor.fetchall()}

    current_repo = None
    for repo_id, ptype, cnt in rows:
        if repo_id != current_repo:
            current_repo = repo_id
            repo_url = repo_map.get(repo_id, 'Unknown')
            print(f"\n  Repository {repo_id} ({repo_url}):")
        print(f"    {ptype:<20s} {cnt:>5d}")

    conn.close()
    print("\n✅ PROJECT_TYPE classification complete.")


if __name__ == '__main__':
    # Determine the database path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    db_path = os.path.join(project_root, '23359384-sq26-classification.db')
    db_path = os.path.normpath(db_path)

    print(f"Database: {db_path}")
    if not os.path.exists(db_path):
        print(f"ERROR: Database not found at {db_path}")
        exit(1)

    run(db_path)
