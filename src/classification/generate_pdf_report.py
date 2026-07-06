import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.backends.backend_pdf import PdfPages
import os
import re
import textwrap


def get_domain(url):
    return re.sub(r'https?://(www\.)?', '', url).rstrip('/')


def wrap_text(text, width=55):
    return '\n'.join(textwrap.wrap(str(text), width))


def add_cover_page(pdf):
    fig, ax = plt.subplots(figsize=(8.27, 11.69))
    ax.axis('off')
    ax.text(0.5, 0.65, 'ISIC Classification Report',
            ha='center', va='center', fontsize=26, fontweight='bold',
            transform=ax.transAxes)
    ax.text(0.5, 0.55, 'Qualitative Data Archive — Project Classification',
            ha='center', va='center', fontsize=14, color='#444444',
            transform=ax.transAxes)
    ax.text(0.5, 0.45, 'Student ID: 23359384',
            ha='center', va='center', fontsize=12, color='#666666',
            transform=ax.transAxes)
    ax.plot([0.1, 0.9], [0.62, 0.62], color='#2c5f8a', linewidth=1.5,
            transform=ax.transAxes)
    pdf.savefig(fig, bbox_inches='tight')
    plt.close()


def add_histogram_page(pdf, division_counts, repo_url):
    domain = get_domain(repo_url)
    fig, ax = plt.subplots(figsize=(11.69, 8.27))  # A4 landscape for wide charts

    bars = ax.barh(division_counts['Label'], division_counts['Count'], color='#2c5f8a')
    ax.invert_yaxis()

    x_max = division_counts['Count'].max()
    for bar, count in zip(bars, division_counts['Count']):
        ax.text(
            bar.get_width() + x_max * 0.012,
            bar.get_y() + bar.get_height() / 2,
            str(int(count)),
            va='center', ha='left', fontsize=8.5, fontweight='bold'
        )

    ax.set_xlim(0, x_max * 1.18)
    ax.set_xlabel('Number of Projects', fontsize=11)
    ax.set_title(f'Primary ISIC Classes — {domain}', fontsize=13, pad=14, fontweight='bold')
    ax.tick_params(axis='y', labelsize=7.5)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()

    pdf.savefig(fig, bbox_inches='tight')
    plt.close()


def add_table_page(pdf, division_counts, repo_url):
    domain = get_domain(repo_url)
    fig, ax = plt.subplots(figsize=(8.27, 11.69))
    ax.axis('off')

    ax.text(0.02, 0.97, f'Top 20 ISIC Divisions — {domain}',
            ha='left', va='top', fontsize=13, fontweight='bold',
            transform=ax.transAxes)
    ax.plot([0.02, 0.98], [0.955, 0.955], color='#2c5f8a', linewidth=1,
            transform=ax.transAxes)

    table_data = []
    for rank, (_, row) in enumerate(division_counts.iterrows(), 1):
        table_data.append([
            str(rank),
            str(int(row['ISIC Division'])).zfill(2),
            wrap_text(row['ISIC Division Name'], width=60),
            str(int(row['Count']))
        ])

    col_labels = ['Rank', 'Code', 'ISIC Division Name', 'Count']
    col_widths = [0.07, 0.07, 0.72, 0.1]

    table = ax.table(
        cellText=table_data,
        colLabels=col_labels,
        colWidths=col_widths,
        loc='upper center',
        bbox=[0.01, 0.02, 0.98, 0.91],
        cellLoc='left'
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)

    # Header styling
    for j in range(len(col_labels)):
        cell = table[(0, j)]
        cell.set_facecolor('#2c5f8a')
        cell.set_text_props(color='white', fontweight='bold')
        cell.set_height(0.045)

    # Row styling
    for i in range(1, len(table_data) + 1):
        bg = '#f0f4f8' if i % 2 == 0 else 'white'
        for j in range(len(col_labels)):
            cell = table[(i, j)]
            cell.set_facecolor(bg)
            cell.set_height(0.042)

    pdf.savefig(fig, bbox_inches='tight')
    plt.close()


def add_comments_page(pdf, repo_url, repo_summary_df):
    domain = get_domain(repo_url)
    fig, ax = plt.subplots(figsize=(8.27, 11.69))
    ax.axis('off')

    ax.text(0.02, 0.97, f'Comments — {domain}',
            ha='left', va='top', fontsize=13, fontweight='bold',
            transform=ax.transAxes)
    ax.plot([0.02, 0.98], [0.955, 0.955], color='#2c5f8a', linewidth=1,
            transform=ax.transAxes)

    # Project type summary table
    ax.text(0.02, 0.92, 'Project Type Distribution:',
            ha='left', va='top', fontsize=10, fontweight='bold',
            transform=ax.transAxes)

    y = 0.88
    for _, row in repo_summary_df.iterrows():
        ax.text(0.04, y, f"  {row['Project Type']}: {int(row['Count'])} projects",
                ha='left', va='top', fontsize=9, transform=ax.transAxes)
        y -= 0.035

    ax.text(0.02, y - 0.02, 'Findings:',
            ha='left', va='top', fontsize=10, fontweight='bold',
            transform=ax.transAxes)
    ax.text(0.04, y - 0.06, '[Add your observations about the dominant classes and patterns here]',
            ha='left', va='top', fontsize=9, color='#888888', style='italic',
            transform=ax.transAxes)

    pdf.savefig(fig, bbox_inches='tight')
    plt.close()


def add_technical_challenges_page(pdf):
    fig, ax = plt.subplots(figsize=(8.27, 11.69))
    ax.axis('off')

    ax.text(0.02, 0.97, 'Technical Challenges',
            ha='left', va='top', fontsize=16, fontweight='bold',
            transform=ax.transAxes)
    ax.plot([0.02, 0.98], [0.955, 0.955], color='#2c5f8a', linewidth=1,
            transform=ax.transAxes)
    ax.text(0.04, 0.91, '[Add your observations on technical challenges with the data (not programming) here]',
            ha='left', va='top', fontsize=10, color='#888888', style='italic',
            transform=ax.transAxes)

    pdf.savefig(fig, bbox_inches='tight')
    plt.close()


def build_report(csv_path, summary_csv_path, output_pdf):
    df = pd.read_csv(csv_path)
    summary_df = pd.read_csv(summary_csv_path)

    repos = sorted(df['Repository URL'].unique())

    with PdfPages(output_pdf) as pdf:
        add_cover_page(pdf)

        for repo_url in repos:
            repo_df = df[df['Repository URL'] == repo_url]
            repo_summary = summary_df[summary_df['Repository URL'] == repo_url]

            division_counts = (
                repo_df.groupby(['ISIC Division', 'ISIC Division Name'])['Count']
                .sum()
                .reset_index()
                .sort_values('Count', ascending=False)
                .head(20)
            )

            division_counts['Label'] = (
                division_counts['ISIC Division'].astype(str).str.zfill(2)
                + ' - '
                + division_counts['ISIC Division Name']
            )

            add_histogram_page(pdf, division_counts, repo_url)
            add_table_page(pdf, division_counts, repo_url)
            add_comments_page(pdf, repo_url, repo_summary)

            print(f"Done: {get_domain(repo_url)}")

        add_technical_challenges_page(pdf)

    print(f"\nPDF saved to: {output_pdf}")


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    csv_path = os.path.join(project_root, 'reports', 'isic_distributions.csv')
    summary_path = os.path.join(project_root, 'reports', 'project_type_summary.csv')
    output_pdf = os.path.join(project_root, 'reports', 'classification_report.pdf')

    build_report(csv_path, summary_path, output_pdf)
