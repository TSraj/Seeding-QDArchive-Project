import csv
import json

def generate_taxonomy():
    sections = {}
    divisions = {}
    
    current_section = None
    
    with open('isic_rev_5.csv', 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row['ISIC Rev 5 Code'].strip()
            title = row['ISIC Rev 5 Title'].strip()
            
            if len(code) == 1 and code.isalpha():
                sections[code] = title
                current_section = code
            elif len(code) == 2 and code.isdigit():
                # Store the division and its parent section
                divisions[code] = {
                    'title': title,
                    'section': current_section,
                    'full_description': f"{sections[current_section]} - {title}"
                }

    output = '"""\nISIC Rev. 5 Taxonomy\nContains sections and divisions for the classification pipeline.\n"""\n\n'
    output += 'SECTIONS = ' + json.dumps(sections, indent=4) + '\n\n'
    output += 'DIVISIONS = ' + json.dumps(divisions, indent=4) + '\n'
    
    with open('src/classification/isic_taxonomy.py', 'w', encoding='utf-8') as f:
        f.write(output)
        
    print(f"Generated taxonomy with {len(sections)} sections and {len(divisions)} divisions.")

if __name__ == '__main__':
    generate_taxonomy()
