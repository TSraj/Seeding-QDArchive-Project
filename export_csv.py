import sqlite3
import csv
import os

DB_PATH = 'data/metadata/qdarchive.db'
EXPORT_DIR = 'data/metadata/'

def export_table_to_csv(table_name):
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        if not rows:
            print(f"Table '{table_name}' is empty.")
            return

        # Get column names
        column_names = [description[0] for description in cursor.description]
        
        csv_file_path = os.path.join(EXPORT_DIR, f"{table_name}_export.csv")
        
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(column_names)
            writer.writerows(rows)
            
        print(f"Successfully exported '{table_name}' to {csv_file_path}")
    except sqlite3.OperationalError as e:
        print(f"Error reading table '{table_name}': {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    print("Exporting database tables to CSV...")
    export_table_to_csv('records')
    export_table_to_csv('file_metadata')
