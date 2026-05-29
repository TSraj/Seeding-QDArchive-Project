"""
Step 4: Reporting

Generates the final statistical distributions for the classified data.
Produces:
1. PROJECT_TYPE summary
2. 18 ISIC distributions (9 repositories x 2 project types)
3. CSV exports for Google Sheets
"""

import sqlite3
import os
import csv
import random

def generate_project_type_summary(cursor, output_dir):
    print("\\n=== PROJECT_TYPE SUMMARY ===")
    
    # Get all repository IDs and URLs
    cursor.execute("SELECT DISTINCT repository_id, repository_url FROM projects")
    repos = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Get counts
    cursor.execute('''
        SELECT repository_id, type, COUNT(*) as cnt
        FROM projects
        GROUP BY repository_id, type
        ORDER BY repository_id, type
    ''')
    
    results = cursor.fetchall()
    
    csv_path = os.path.join(output_dir, 'project_type_summary.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Repository ID', 'Repository URL', 'Project Type', 'Count'])
        
        current_repo = None
        for repo_id, ptype, cnt in results:
            if repo_id != current_repo:
                current_repo = repo_id
                print(f"\\nRepository {repo_id} ({repos.get(repo_id)}):")
                
            print(f"  {ptype:<20s}: {cnt}")
            writer.writerow([repo_id, repos.get(repo_id), ptype, cnt])
            
    print(f"\\nSaved PROJECT_TYPE summary to {csv_path}")

def generate_isic_distributions(cursor, output_dir):
    print("\\n=== ISIC DISTRIBUTIONS (9 Repos x 2 Types = 18 Distributions) ===")
    
    # Target project types
    project_types = ['QDA_PROJECT', 'QD_PROJECT']
    
    # Get repositories
    cursor.execute("SELECT DISTINCT repository_id, repository_url FROM projects WHERE type IN ('QDA_PROJECT', 'QD_PROJECT')")
    repos = cursor.fetchall()
    
    total_distributions = 0
    csv_path = os.path.join(output_dir, 'isic_distributions.csv')
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Repository ID', 'Repository URL', 'Project Type', 'ISIC Section', 'ISIC Section Name', 'ISIC Division', 'ISIC Division Name', 'Count'])
        
        for repo_id, repo_url in repos:
            for ptype in project_types:
                cursor.execute('''
                    SELECT pc.isic_section, pc.isic_section_name, pc.isic_division, pc.isic_division_name, COUNT(*) as cnt
                    FROM project_classifications pc
                    JOIN projects p ON pc.project_id = p.id
                    WHERE p.repository_id = ? AND p.type = ?
                    GROUP BY pc.isic_section, pc.isic_division
                    ORDER BY cnt DESC
                ''', (repo_id, ptype))
                
                dist = cursor.fetchall()
                if not dist:
                    continue
                    
                total_distributions += 1
                print(f"\\n[{total_distributions}/18] Distribution for Repo {repo_id} ({repo_url}) | Type: {ptype}")
                
                for row in dist:
                    sec, sec_name, div, div_name, cnt = row
                    print(f"  {sec}{div} - {div_name[:40]:<40} : {cnt}")
                    writer.writerow([repo_id, repo_url, ptype, sec, sec_name, div, div_name, cnt])
                    
    print(f"\\nGenerated {total_distributions} distributions.")
    print(f"Saved ISIC distributions to {csv_path}")

def spot_check(cursor):
    print("\\n=== SPOT CHECK (10 Random Classifications) ===")
    cursor.execute('''
        SELECT p.title, p.type, pc.isic_section, pc.isic_division, pc.isic_division_name, pc.confidence_score
        FROM project_classifications pc
        JOIN projects p ON pc.project_id = p.id
    ''')
    all_classifications = cursor.fetchall()
    
    if not all_classifications:
        print("No classifications found!")
        return
        
    sample = random.sample(all_classifications, min(10, len(all_classifications)))
    
    for title, ptype, sec, div, div_name, score in sample:
        safe_title = (title[:60] + '...') if title and len(title) > 60 else title
        print(f"\\nTitle: {safe_title}")
        print(f"Type: {ptype}")
        print(f"ISIC: Section {sec}, Division {div} ({div_name})")
        print(f"Confidence: {score:.3f}")

def run(db_path: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    generate_project_type_summary(cursor, output_dir)
    generate_isic_distributions(cursor, output_dir)
    spot_check(cursor)
    
    conn.close()
    print("\\n✅ Step 4: Reporting complete.")

if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    db_path = os.path.join(project_root, '23359384-sq26-classification.db')
    output_dir = os.path.join(project_root, 'reports')
    
    run(db_path, output_dir)
