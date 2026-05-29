import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def generate_histograms(csv_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    # Read the data
    df = pd.read_csv(csv_path)
    
    # Aggregate data by ISIC Section to get a high-level histogram
    section_counts = df.groupby(['ISIC Section', 'ISIC Section Name'])['Count'].sum().reset_index()
    section_counts = section_counts.sort_values('Count', ascending=False)
    
    # Create the Section Histogram
    plt.figure(figsize=(12, 8))
    sns.barplot(data=section_counts, x='Count', y='ISIC Section Name', hue='ISIC Section Name', palette='viridis', legend=False)
    plt.title('Distribution of Qualitative Projects by ISIC Section', fontsize=16)
    plt.xlabel('Number of Projects', fontsize=12)
    plt.ylabel('ISIC Section', fontsize=12)
    plt.tight_layout()
    
    # Save as PNG
    section_png = os.path.join(output_dir, 'histogram_isic_sections.png')
    plt.savefig(section_png, format='png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Aggregate data by ISIC Division (Top 20 for readability)
    division_counts = df.groupby(['ISIC Division', 'ISIC Division Name'])['Count'].sum().reset_index()
    division_counts['Division Label'] = division_counts['ISIC Division'].astype(str).str.zfill(2) + ' - ' + division_counts['ISIC Division Name'].astype(str).str.slice(0, 40)
    division_counts = division_counts.sort_values('Count', ascending=False).head(20)
    
    # Create the Division Histogram
    plt.figure(figsize=(14, 10))
    sns.barplot(data=division_counts, x='Count', y='Division Label', hue='Division Label', palette='mako', legend=False)
    plt.title('Top 20 ISIC Divisions for Qualitative Projects', fontsize=16)
    plt.xlabel('Number of Projects', fontsize=12)
    plt.ylabel('ISIC Division', fontsize=12)
    plt.tight_layout()
    
    # Save as PNG
    division_png = os.path.join(output_dir, 'histogram_isic_divisions.png')
    plt.savefig(division_png, format='png', dpi=300, bbox_inches='tight')
    plt.close()

    print(f"✅ Histograms successfully generated and saved to: {output_dir}")
    print(f"Files created: \\n- {section_png}\\n- {division_png}")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    csv_path = os.path.join(project_root, 'reports', 'isic_distributions.csv')
    output_dir = os.path.join(project_root, 'reports')
    
    if os.path.exists(csv_path):
        generate_histograms(csv_path, output_dir)
    else:
        print(f"Error: Could not find {csv_path}")
