import pandas as pd
import matplotlib.pyplot as plt
import os
import re


def sanitize_filename(url):
    domain = re.sub(r'https?://(www\.)?', '', url).rstrip('/')
    return domain.replace('.', '_').replace('/', '_')


def generate_per_repo_histograms(csv_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(csv_path)
    repos = df['Repository URL'].unique()

    for repo_url in repos:
        repo_df = df[df['Repository URL'] == repo_url]

        # Aggregate counts across all project types per division
        division_counts = (
            repo_df.groupby(['ISIC Division', 'ISIC Division Name'])['Count']
            .sum()
            .reset_index()
            .sort_values('Count', ascending=False)
            .head(20)
        )

        # Full label: zero-padded code + full name (no truncation)
        division_counts['Label'] = (
            division_counts['ISIC Division'].astype(str).str.zfill(2)
            + ' - '
            + division_counts['ISIC Division Name']
        )

        fig, ax = plt.subplots(figsize=(18, max(8, len(division_counts) * 0.55)))

        bars = ax.barh(division_counts['Label'], division_counts['Count'], color='steelblue')
        ax.invert_yaxis()

        # Count label at end of each bar
        x_max = division_counts['Count'].max()
        for bar, count in zip(bars, division_counts['Count']):
            ax.text(
                bar.get_width() + x_max * 0.01,
                bar.get_y() + bar.get_height() / 2,
                str(int(count)),
                va='center', ha='left', fontsize=9
            )

        ax.set_xlim(0, x_max * 1.12)
        ax.set_xlabel('Number of Projects', fontsize=12)
        ax.set_title(f'Top ISIC Divisions — {repo_url}', fontsize=13, pad=12)
        ax.tick_params(axis='y', labelsize=8)
        plt.tight_layout()

        repo_name = sanitize_filename(repo_url)
        svg_path = os.path.join(output_dir, f'histogram_{repo_name}.svg')
        plt.savefig(svg_path, format='svg', bbox_inches='tight')
        plt.close()

        print(f"Saved: {svg_path}")

    print(f"\nDone — {len(repos)} histograms saved to: {output_dir}")


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    csv_path = os.path.join(project_root, 'reports', 'isic_distributions.csv')
    output_dir = os.path.join(project_root, 'reports', 'per_repo_histograms')

    if os.path.exists(csv_path):
        generate_per_repo_histograms(csv_path, output_dir)
    else:
        print(f"Error: Could not find {csv_path}")
