import time
import requests
from .config import ZENODO_API_BASE, RATE_LIMIT_DELAY, MAX_PAGES, RESULTS_PER_PAGE, TARGET_EXTENSIONS, RAW_DIR, SMART_QUERIES, NON_QUALITATIVE_KEYWORDS
from .downloader import download_record, sanitize_folder_name
from .db import is_downloaded, insert_project, insert_file, insert_keyword, insert_person_role, insert_license

def extract_zenodo_meta(record):
    meta = record.get("metadata", {})
    title = meta.get("title", "Unknown")
    description = meta.get("description", "")
    language = meta.get("language", "")
    version = meta.get("version", "")
    upload_date = meta.get("publication_date", "")
    
    keywords = meta.get("keywords", [])
    if isinstance(keywords, str):
        keywords = [k.strip() for k in keywords.split(",")]

    lic = meta.get("license", {}).get("id", "")
    if not isinstance(lic, str):
        lic = str(lic)
    
    persons = []
    for c in meta.get("creators", []):
        persons.append({"name": c.get("name", ""), "role": "creator"})
    for c in meta.get("contributors", []):
        persons.append({"name": c.get("name", ""), "role": c.get("type", "contributor")})
        
    return {
        "title": title,
        "description": description,
        "language": language,
        "version": version,
        "upload_date": upload_date,
        "keywords": keywords,
        "license": lic,
        "persons": persons
    }

def api_get(url, params=None):
    """Wrapper for requests to handle rate limit delay gracefully."""
    time.sleep(RATE_LIMIT_DELAY)
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def search_zenodo(extension: str, max_pages: int = MAX_PAGES):
    """Search Zenodo records for an extension query."""
    print(f"\n--- Searching Zenodo for query: '{extension}' ---")
    records_found = []
    
    for page in range(1, max_pages + 1):
        params = {
            "q": extension,
            "size": RESULTS_PER_PAGE,
            "page": page
        }
        
        try:
            data = api_get(f"{ZENODO_API_BASE}/records", params)
            hits = data.get("hits", {}).get("hits", [])
            
            if not hits:
                break
                
            records_found.extend(hits)
            
            # If we got fewer than requested, we're on the last page
            if len(hits) < RESULTS_PER_PAGE:
                break
                
        except Exception as e:
            print(f"Error fetching page {page} for '{extension}': {e}")
            break
            
    return records_found

def get_record_files(record_id: int):
    """Fetch the list of files for a specific record."""
    try:
        data = api_get(f"{ZENODO_API_BASE}/records/{record_id}/files")
        return data.get("entries", [])
    except Exception as e:
        print(f"Error fetching files for record {record_id}: {e}")
        return []

def has_target_extension(files: list, target_exts: list) -> list:
    """
    Checks if any file has a target extension.
    Returns the list of matching extensions found.
    """
    matched = set()
    for f in files:
        filename = f.get("key", "").lower()
        for ext in target_exts:
            if filename.endswith(f".{ext}"):
                matched.add(ext)
    return list(matched)

def scrape(extensions=TARGET_EXTENSIONS, max_pages=MAX_PAGES, dry_run=False, max_runtime_hours=None):
    """Orchestrates the scraping, filtering, and downloading process."""
    processed_record_ids = set()
    start_time = time.time()
    
    for ext in extensions:
        # Check runtime before starting a new extension
        if max_runtime_hours is not None:
            elapsed_hours = (time.time() - start_time) / 3600
            if elapsed_hours >= max_runtime_hours:
                runtime_display = f"{max_runtime_hours * 60:.0f} minutes" if max_runtime_hours < 1 else f"{max_runtime_hours:.2f} hours"
                print(f"\n[INFO] Maximum runtime of {runtime_display} reached. Stopping scraper.")
                break
                
        records = search_zenodo(ext, max_pages)
        print(f"Found {len(records)} records for query '{ext}'. Checking files...")
        
        for record in records:
            record_id = record.get("id")
            title = record.get("title", "Unknown")
            doi = record.get("doi", "")
            
            if not record_id:
                continue
                
            if record_id in processed_record_ids:
                # Already processed this record from a previous extension query
                continue 
            processed_record_ids.add(record_id)
            
            # Check if the record's folder already exists on disk
            folder_name_check = sanitize_folder_name(title) or f"Zenodo_Record_{record_id}"
            if (RAW_DIR / folder_name_check).exists():
                print(f"Skipping Record {record_id} ('{title}'): Folder already exists in raw/. Skipping.")
                continue

            if not dry_run and is_downloaded(record_id):
                print(f"Skipping Record {record_id}: Already completely downloaded and logged in DB.")
                continue
                
            # Check runtime before downloading a new record
            if max_runtime_hours is not None:
                elapsed_hours = (time.time() - start_time) / 3600
                if elapsed_hours >= max_runtime_hours:
                    runtime_display = f"{max_runtime_hours * 60:.0f} minutes" if max_runtime_hours < 1 else f"{max_runtime_hours:.2f} hours"
                    print(f"\n[INFO] Maximum runtime of {runtime_display} reached. Stopping scraper.")
                    return
                
            # Fetch details about the record's files
            files = get_record_files(record_id)
            if not files:
                continue
                
            matched_exts = has_target_extension(files, TARGET_EXTENSIONS)
            
            if not matched_exts:
                # None of the files actually had any of our target extensions.
                continue
                
            print(f"\n[MATCH] Record {record_id} ({title}) matches extensions: {matched_exts}")
            print(f"  Contains {len(files)} files.")
            
            if dry_run:
                continue
                
            # Perform Download (sending all files in the record)
            total_dl, total_bytes, folder_name, downloaded_files = download_record(record, files)
            
            # Log successful download to local DB
            if total_dl > 0:
                parsed_meta = extract_zenodo_meta(record)
                project_url = record.get("links", {}).get("html", "")
                
                insert_project(
                    project_id=record_id,
                    query_string=ext,
                    repository_name="Zenodo",
                    repository_url="https://zenodo.org",
                    project_url=project_url,
                    version=parsed_meta["version"],
                    title=parsed_meta["title"],
                    description=parsed_meta["description"],
                    language=parsed_meta["language"],
                    doi=doi,
                    upload_date=parsed_meta["upload_date"],
                    download_repository_folder="raw",
                    download_project_folder=folder_name,
                    download_version_folder="",
                    download_method="API-CALL"
                )
                
                if parsed_meta["license"]:
                    insert_license(record_id, parsed_meta["license"])
                    
                for kw in parsed_meta["keywords"]:
                    if kw:
                        insert_keyword(record_id, str(kw))
                        
                for p in parsed_meta["persons"]:
                    if p["name"]:
                        insert_person_role(record_id, p["name"], p["role"])
                        
                for dl_file in downloaded_files:
                    file_type = dl_file.split(".")[-1] if "." in dl_file else ""
                    insert_file(
                        project_id=record_id,
                        file_name=dl_file,
                        file_type=file_type,
                        status="SUCCESS"
                    )


def is_qualitative_dataset_zenodo(record):
    """Check if a Zenodo record is likely qualitative research."""
    title = record.get("metadata", {}).get("title", "")
    description = record.get("metadata", {}).get("description", "")
    combined_text = f"{title} {description}".lower()
    
    for keyword in NON_QUALITATIVE_KEYWORDS:
        if keyword.lower() in combined_text:
            print(f"  [SKIP] Non-qualitative record detected (matched '{keyword}'): {title[:80]}")
            return False
    return True


def search_smart_zenodo(query: str, max_pages: int = MAX_PAGES):
    """Search Zenodo using a smart query."""
    print(f"\n--- Smart Query on Zenodo: '{query}' ---")
    records_found = []
    
    for page in range(1, max_pages + 1):
        params = {
            "q": query,
            "size": RESULTS_PER_PAGE,
            "page": page
        }
        
        try:
            data = api_get(f"{ZENODO_API_BASE}/records", params)
            hits = data.get("hits", {}).get("hits", [])
            
            if not hits:
                break
                
            records_found.extend(hits)
            
            if len(hits) < RESULTS_PER_PAGE:
                break
                
        except Exception as e:
            print(f"Error fetching page {page} for smart query '{query}': {e}")
            break
            
    return records_found


def scrape_smart(queries=SMART_QUERIES, max_pages=MAX_PAGES, dry_run=False, max_runtime_hours=None):
    """Orchestrates smart-query-based scraping for Zenodo."""
    processed_record_ids = set()
    start_time = time.time()
    
    for query in queries:
        if max_runtime_hours is not None:
            elapsed_hours = (time.time() - start_time) / 3600
            if elapsed_hours >= max_runtime_hours:
                print("\n[INFO] Maximum runtime reached. Stopping Zenodo smart query scraper.")
                break
                
        records = search_smart_zenodo(query, max_pages)
        print(f"Found {len(records)} records for smart query '{query}'. Checking files...")
        
        for record in records:
            record_id = record.get("id")
            title = record.get("title", "Unknown")
            doi = record.get("doi", "")
            
            if not record_id:
                continue
                
            if record_id in processed_record_ids:
                continue 
            processed_record_ids.add(record_id)
            
            folder_name_check = sanitize_folder_name(title) or f"Zenodo_Record_{record_id}"
            if (RAW_DIR / folder_name_check).exists():
                print(f"Skipping Record {record_id} ('{title}'): Folder already exists.")
                continue

            if not dry_run and is_downloaded(record_id):
                print(f"Skipping Record {record_id}: Already downloaded.")
                continue
                
            if max_runtime_hours is not None:
                elapsed_hours = (time.time() - start_time) / 3600
                if elapsed_hours >= max_runtime_hours:
                    print("\n[INFO] Maximum runtime reached. Stopping Zenodo scraper.")
                    return
            
            if not is_qualitative_dataset_zenodo(record):
                continue

            # Fetch details about the record's files
            files = get_record_files(record_id)
            if not files:
                continue
                
            print(f"\n[SMART MATCH] Record {record_id} ({title}) matched smart query: {query}")
            print(f"  Contains {len(files)} files. Queuing ALL for download.")
            
            if dry_run:
                continue
                
            total_dl, total_bytes, folder_name, downloaded_files = download_record(record, files)
            
            if total_dl > 0:
                parsed_meta = extract_zenodo_meta(record)
                project_url = record.get("links", {}).get("html", "")
                
                insert_project(
                    project_id=record_id,
                    query_string=query,
                    repository_name="Zenodo",
                    repository_url="https://zenodo.org",
                    project_url=project_url,
                    version=parsed_meta["version"],
                    title=parsed_meta["title"],
                    description=parsed_meta["description"],
                    language=parsed_meta["language"],
                    doi=doi,
                    upload_date=parsed_meta["upload_date"],
                    download_repository_folder="raw",
                    download_project_folder=folder_name,
                    download_version_folder="",
                    download_method="API-CALL"
                )
                
                if parsed_meta["license"]:
                    insert_license(record_id, parsed_meta["license"])
                    
                for kw in parsed_meta["keywords"]:
                    if kw:
                        insert_keyword(record_id, str(kw))
                        
                for p in parsed_meta["persons"]:
                    if p["name"]:
                        insert_person_role(record_id, p["name"], p["role"])
                        
                for dl_file in downloaded_files:
                    file_type = dl_file.split(".")[-1] if "." in dl_file else ""
                    insert_file(
                        project_id=record_id,
                        file_name=dl_file,
                        file_type=file_type,
                        status="SUCCESS"
                    )
