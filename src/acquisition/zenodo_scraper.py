import time
import requests
from .config import ZENODO_API_BASE, RATE_LIMIT_DELAY, MAX_PAGES, RESULTS_PER_PAGE, TARGET_EXTENSIONS, RAW_DIR
from .downloader import download_record, sanitize_folder_name
from .db import is_downloaded, mark_downloaded, insert_file_metadata

def extract_zenodo_meta(record):
    meta = record.get("metadata", {})
    author = "; ".join([c.get("name", "") for c in meta.get("creators", [])])
    year = meta.get("publication_date", "")[:4] if meta.get("publication_date") else ""
    lic = meta.get("license", {}).get("id", "")
    return author, year, author, "", lic

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
                mark_downloaded(
                    record_id=record_id,
                    title=title,
                    doi=doi,
                    folder_name=folder_name,
                    matched_extensions=matched_exts,
                    total_files=total_dl,
                    total_size_bytes=total_bytes
                )
                
                author, year, up_name, up_email, lic = extract_zenodo_meta(record)
                for dl_file in downloaded_files:
                    file_info = next((f for f in files if f.get("key") == dl_file), None)
                    file_url = file_info.get("links", {}).get("content", "") if file_info else ""
                    file_type = dl_file.split(".")[-1] if "." in dl_file else ""
                    insert_file_metadata(
                        file_url=file_url,
                        local_dir_name=folder_name,
                        local_file_name=dl_file,
                        context_repository="Zenodo",
                        license=lic[:100],
                        uploader_name=up_name,
                        uploader_email=up_email,
                        doi=doi,
                        file_type=file_type,
                        year=year,
                        author=author
                    )
