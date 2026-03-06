# Seeding QDArchive: An Automated Pipeline for Qualitative Data Acquisition

## Project Overview

**Seeding QDArchive** is a systematic, automated pipeline designed to discover, acquire, and curate open qualitative research data (QDA) from prominent digital repositories. As the movement toward Open Science expands, the sharing of qualitative data such as interview transcripts, coding trees, and multimedia files remains fragmented across various platforms and formats. This project addresses the challenge of discovering and consolidating these disparate resources into a unified, accessible, and analytically viable archive.

The primary objectives of the **Seeding QDArchive** pipeline are to:
- **Acquire** open qualitative research data (QDA files) across extensive online repositories.
- **Extract and Validate** highly granular file-level metadata (e.g., timestamps, persistent identifiers, robust author and affiliation tracking).
- **Verify** open licenses to ensure ethical and legal reuse of the data.
- **Classify and Organize** qualitative datasets to facilitate long-term preservation and reproducibility.
- **Enable Analytical Exploration** to understand the landscape of open qualitative research and standard sharing practices.

## Supported Repositories

The pipeline seamlessly interfaces with the REST APIs of several major multidisciplinary repositories and institutional dataverses. It currently supports automated data retrieval and metadata extraction from:

- **Zenodo** (CERN)
- **OSF** (Open Science Framework)
- **Figshare**
- **Harvard Dataverse**
- **Dataverse NO**
- **Borealis** (The Canadian Dataverse Repository)
- **AUSSDA** (The Austrian Social Science Data Archive)
- **heiDATA** (Heidelberg University Dataverse)
- **QDR** (Qualitative Data Repository - Syracuse University)
- **DANS** (Data Archiving and Networked Services - KNAW)
- **ADA** (Australian Data Archive)

## Targeted Data Formats

To ensure comprehensive capture of qualitative research outputs, the scraper specifically targets native application files, project bundles, and standard exchange formats from leading Computer-Assisted Qualitative Data Analysis Software (CAQDAS). 

**Primary Target:**
- `.qdpx` (REFI-QDA Standard Exchange Format - Highest Priority)

**Software-Specific Formats:**
- **ATLAS.ti:** `.atlproj`
- **NVivo:** `.nvp`, `.nvpx`
- **MAXQDA:** `.mqda`, `.mx24`, `.mx22`, `.mx20`, `.mx18`, `.mx12`, `.mx11`, `.mx5`, `.mx4`, `.mx3`, `.mx2`, `.m2k`, `.mqbac`, `.mqtc`, `.mqex`, `.mqmtr`, `.mx24bac`, `.mc24`, `.mex24`, `.mex22`
- **Other Formats:** `.ppj`, `.qdp`, `.qrk`, `.loa`, `.sea`, `.mtr`, `.mod`, `.hpr7`, `.pprj`, `.qlt`, `.f4p`, `.qpd`

## Architecture & Workflow

1. **Configuration (`config.yaml`):** An intuitive YAML interface allows researchers to toggle specific scrapers on or off and target particular API endpoints.
2. **Search & Discovery:** The pipeline queries the specific repository APIs using pagination limits and precise extension queries.
3. **Data Acquisition (`downloader.py`):** Highly parallelized downloading algorithms retrieve raw files while respecting repository rate limits. Empty or inaccessible payloads (e.g., closed access) are gracefully skipped.
4. **Granular Metadata Extraction (`db.py`):** The system parses hierarchical JSON responses provided by repository endpoints. Critical analytical data—including direct file URLs, local paths, DOIs, specific licensing terms, and author details—are inserted into a persistent SQLite database (`qdarchive.db`).
5. **Deduplication:** Hashed identifiers prevent redundant downloads when pipelines are executed iteratively.

## Usage / How to Run

To run the Seeding QDArchive pipeline locally, follow these steps:

1. **Configure Scrapers:**
   Open the `config.yaml` file in the project root. Enable the scrapers you wish to run by setting their values to `true` (and `false` for the ones you want to skip).
   ```yaml
   scrapers:
     zenodo: false
     dataverse: false
     figshare: true
     # ...
   ```

2. **Execute the Scraper:**
   Run the following `uv` command in your terminal. You can adjust the `--max-runtime-minutes` argument to limit how long the scraper runs.
   ```bash
   uv run python -m src.acquisition.main --max-runtime-minutes 30
   ```
   
The pipeline will automatically initialize the database, parse your configuration, and begin downloading targeted data formats.

## Database Structure

The pipeline stores acquired metadata in a local SQLite database (`data/metadata/qdarchive.db`). The database contains two primary tables:

### 1. `records`
This table stores **dataset-level** or project-level information. One row is created per dataset discovered.
*   **`id`**: Primary Key (integer).
*   **`title`**: The title of the dataset/project.
*   **`doi`**: The main DOI of the dataset.
*   **`folder_name`**: The local directory name where files are saved.
*   **`download_date`**: ISO timestamp of when the record was processed.
*   **`total_files` & `total_size_bytes`**: Aggregate statistics for the downloaded files.

### 2. `file_metadata`
This table stores highly granular **file-level** information. One row is created for every individual file downloaded or processed.
*   **`id`**: Primary Key (Auto-incremented).
*   **`file_url`**: The direct download URL of the specific file.
*   **`download_timestamp`**: ISO timestamp of the download.
*   **`local_dir_name` & **`local_file_name`**: The storage location on disk.
*   **`context_repository`**: Which repository the file came from (e.g., Zenodo, Figshare).
*   **`license`**, **`uploader_name`**, **`uploader_email`**, **`doi`**, **`file_type`**, **`year`**, **`author`**: Specific metadata fields extracted from the repository response. If metadata is missing during scraping, these fields remain empty.

> [!NOTE]
> You may also see a third table named `sqlite_sequence`. This is an internal SQLite table generated automatically to manage the auto-incrementing IDs for the `file_metadata` table.

## Exporting Data

To export the acquired metadata and records from the SQLite database into CSV files, you can use the provided export script. This is especially useful for sharing the data or analyzing it in external tools like Excel or R.

Run the following command in your terminal from the project's root directory:
```bash
python export_csv.py
```

This will read the `qdarchive.db` database and generate two CSV files in the `data/metadata/` directory:
- `records_export.csv`
- `file_metadata_export.csv`

## Purpose and Research Implication

The **Seeding QDArchive** acts as the foundational infrastructure for a larger meta-research initiative. By systematically compiling thousands of disparate QDA files and their associated metadata, this tool enables researchers to:
- Conduct large-scale meta-analyses on qualitative methodologies.
- Investigate compliance with Open Data and FAIR (Findable, Accessible, Interoperable, and Reusable) principles within qualitative paradigms.
- Assess the adoption rates of the REFI-QDA interoperability standard (`.qdpx`). 

By bridging the gap between raw data repositories and qualitative researchers, this project directly supports the advancement of computational qualitative research and open science transparency.
