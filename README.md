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
2. **Dual Search Strategy:**
   - **Extension-Based Search:** Queries repository APIs for specific QDA file extensions (e.g., `.qdpx`, `.nvpx`). Only matching files are downloaded.
   - **Smart Query Discovery:** Uses methodology-focused keywords (e.g., "grounded theory", "semi-structured interviews", "thematic analysis") to discover projects that may not explicitly list QDA extensions in their metadata. In this mode, **all** files within a matched project are retrieved.
3. **Qualitative Filtering:** To maintain high data precision, the pipeline implements an `is_qualitative_dataset` filter. It cross-references project titles and descriptions against a `NON_QUALITATIVE_KEYWORDS` list (e.g., "genome", "sequencing", "satellite imagery") to automatically exclude false positives from smart query results.
4. **Data Acquisition (`downloader.py`):** Highly parallelized downloading algorithms retrieve raw files while respecting repository rate limits.
5. **Relational Metadata Storage (`db.py`):** Parses hierarchical JSON responses and stores granular metadata (descriptions, languages, contributor roles, keywords) in a 5-table relational SQLite database.
6. **Deduplication:** Hashed identifiers prevent redundant downloads/entries when pipelines are executed iteratively.

## Usage / How to Run

To run the Seeding QDArchive pipeline locally, follow these steps:

1. **Configure Scrapers:**
   Open the `config.yaml` file in the project root. Enable the scrapers you wish to run by setting their values to `true`.
   ```yaml
   scrapers:
     zenodo: true
     dataverse: false
     figshare: true
     # ...
   ```

2. **Execute the Scraper:**
   Run the following command in your terminal. Use `--max-runtime-minutes` to limit the session duration.
   ```bash
   uv run python -m src.acquisition.main --max-runtime-minutes 60
   ```
   
The pipeline will automatically initialize the database, parse configuration, and execute both search strategies for all enabled repositories.

## Database Structure

The pipeline stores acquired metadata in a local SQLite database (`data/metadata/qdarchive.db`) using a normalized relational schema:

### 1. `projects`
Stores high-level dataset information.
- `id`: Primary ID (provided by repository or generated).
- `title`, `description`, `language`, `version`, `doi`: Extracted metadata.
- `project_url`, `repository_url`: Web links for the project.
- `upload_date`, `download_date`: Timestamps.

### 2. `files`
Stores individual file metadata linked to projects.
- `project_id`: Foreign key to `projects`.
- `file_name`, `file_type`, `status`: File details.

### 3. `keywords`
Stores project keywords/tags.
- `project_id`: Foreign key to `projects`.
- `keyword`: Individual tag name.

### 4. `person_roles`
Stores authors, creators, and contacts.
- `project_id`: Foreign key to `projects`.
- `name`, `role`: Person's name and their specific role (e.g., Author, Data Collector).

### 5. `licenses`
Stores licensing information.
- `project_id`: Foreign key to `projects`.
- `license`: Textual license identifier (e.g., CC-BY-4.0).

## Exporting Data

To export all relational tables from the SQLite database into CSV files for external analysis (Excel, R, etc.):

```bash
python export_csv.py
```

This generates five CSV files in `data/metadata/`:
- `projects_export.csv`
- `files_export.csv`
- `keywords_export.csv`
- `person_roles_export.csv`
- `licenses_export.csv`

## Purpose and Research Implication

The **Seeding QDArchive** acts as the foundational infrastructure for a larger meta-research initiative. By systematically compiling thousands of disparate QDA files and their associated metadata, this tool enables researchers to:
- Conduct large-scale meta-analyses on qualitative methodologies.
- Investigate compliance with Open Data and FAIR (Findable, Accessible, Interoperable, and Reusable) principles within qualitative paradigms.
- Assess the adoption rates of the REFI-QDA interoperability standard (`.qdpx`). 

By bridging the gap between raw data repositories and qualitative researchers, this project directly supports the advancement of computational qualitative research and open science transparency.
