import time
import requests
from .config import DATAVERSE_NO_API_BASE, RATE_LIMIT_DELAY, MAX_PAGES, RESULTS_PER_PAGE, TARGET_EXTENSIONS, RAW_DIR
from .downloader import download_record, sanitize_folder_name
from .db import is_downloaded, mark_downloaded, insert_file_metadata


def extract_dv_meta(dataset_record):
    latest = dataset_record.get("latestVersion", {})
    blocks = latest.get("metadataBlocks", {})
    citation = blocks.get("citation", {}).get("fields", [])
    author = ""; year = ""; up_name = ""; up_email = ""
    year = latest.get("publicationDate", "")[:4] if latest.get("publicationDate") else ""
    for f in citation:
        if f.get("typeName") == "author":
            author = "; ".join([a.get("authorName", {}).get("value", "") for a in f.get("value", [])])
        elif f.get("typeName") == "datasetContact":
            up_name = "; ".join([c.get("datasetContactName", {}).get("value", "") for c in f.get("value", [])])
            up_email = "; ".join([c.get("datasetContactEmail", {}).get("value", "") for c in f.get("value", [])])
    lic = latest.get("license", {}).get("name", "")
    if not lic: lic = latest.get("termsOfAccess", "")[:100]
    return author, year, up_name, up_email, lic

def api_get(url, params=None):
    """Wrapper for requests to handle rate limit delay gracefully."""
    time.sleep(RATE_LIMIT_DELAY)
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def search_dataverse_no(extension: str, max_pages: int = MAX_PAGES):
    """Search Dataverse NO records for an extension query."""
    print(f"\n--- Searching Dataverse NO for query: '{extension}' ---")
    records_found = []
    
    for page in range(1, max_pages + 1):
        # Dataverse uses start/per_page for pagination
        start = (page - 1) * RESULTS_PER_PAGE
        params = {
            "q": extension,
            "type": "file",  # Search specifically for files
            "start": start,
            "per_page": RESULTS_PER_PAGE,
            "show_entity_ids": True
        }
        
        try:
            data = api_get(f"{DATAVERSE_NO_API_BASE}/search", params)
            data_dict = data.get("data", {})
            hits = data_dict.get("items", [])
            
            if not hits:
                break
                
            records_found.extend(hits)
            
            # If we got fewer than requested, we're on the last page
            if len(hits) < RESULTS_PER_PAGE:
                break
                
        except Exception as e:
            print(f"Error fetching page {page} from Dataverse NO for '{extension}': {e}")
            break
            
    return records_found

def scrape(extensions=TARGET_EXTENSIONS, max_pages=MAX_PAGES, dry_run=False, max_runtime_hours=None):
    """Orchestrates the scraping, filtering, and downloading process for Dataverse NO."""
    processed_dataset_ids = set()
    start_time = time.time()
    
    for ext in extensions:
        # Check runtime before starting a new extension
        if max_runtime_hours is not None:
            elapsed_hours = (time.time() - start_time) / 3600
            if elapsed_hours >= max_runtime_hours:
                runtime_display = f"{max_runtime_hours * 60:.0f} minutes" if max_runtime_hours < 1 else f"{max_runtime_hours:.2f} hours"
                print(f"\n[INFO] Maximum runtime of {runtime_display} reached. Stopping Dataverse NO scraper.")
                break
                
        files = search_dataverse_no(ext, max_pages)
        print(f"Found {len(files)} files for query '{ext}' in Dataverse NO. Processing...")
        
        for file_item in files:
            # Dataverse search returns files. We group them by dataset using the DOI.
            dataset_id = file_item.get("dataset_citation", "Unknown_Dataset")
            
            dataset_title = file_item.get("dataset_citation", f"Dataverse_NO_Dataset_{dataset_id}").split(',')[0].strip() # Take the first part of the citation
            
            doi = file_item.get("dataset_persistent_id", "")
            if not doi:
                print(f"File '{file_item.get('name')}' has no dataset DOI. Skipping.")
                continue
                
            record_id = f"dno_{doi}" # Use DOI as the unique identifier for the processed dataset
            
            if record_id in processed_dataset_ids:
                # Already processed this dataset entirely
                continue 
            processed_dataset_ids.add(record_id)
            
            # Check if the dataset folder already exists on disk
            folder_name_check = sanitize_folder_name(dataset_title)
            
            if (RAW_DIR / folder_name_check).exists():
                print(f"Skipping Dataverse NO Dataset '{dataset_title}': Folder '{folder_name_check}' already exists in raw/. Skipping.")
                continue

            # Convert record_id to an integer offset (e.g. 2 billion base to avoid clashes)
            # Use hash of DOI to ensure determinism
            db_record_id = (hash(doi) % (10**8)) + 2000000000
                
            if not dry_run and is_downloaded(db_record_id):
                print(f"Skipping Dataverse NO Dataset {db_record_id}: Already completely downloaded and logged in DB.")
                continue
                
            # Check runtime before downloading a new record
            if max_runtime_hours is not None:
                elapsed_hours = (time.time() - start_time) / 3600
                if elapsed_hours >= max_runtime_hours:
                    runtime_display = f"{max_runtime_hours * 60:.0f} minutes" if max_runtime_hours < 1 else f"{max_runtime_hours:.2f} hours"
                    print(f"\n[INFO] Maximum runtime of {runtime_display} reached. Stopping Dataverse NO scraper.")
                    return
                
            print(f"\n[MATCH] Dataset '{dataset_title}' ({doi}) contains matching extension: {ext}")
            
            # Fetch ALL files for this Dataset
            print(f"  Fetching full file list for dataset {doi}...")
            try:
                dataset_url = f"{DATAVERSE_NO_API_BASE}/datasets/:persistentId/?persistentId={doi}"
                dataset_data = api_get(dataset_url)
                all_files = dataset_data.get("data", {}).get("latestVersion", {}).get("files", [])
            except Exception as e:
                print(f"  Failed to fetch dataset files for {doi}: {e}")
                continue
                
            if not all_files:
                print(f"  No files found in latest version of {doi}.")
                continue
                
            print(f"  Found {len(all_files)} total files in dataset.")
            
            if dry_run:
                continue
                
            # Construct standard file info list for downloader
            files_to_download = []
            actual_matched_extensions = set()
            has_target_extension = False
            
            for f in all_files:
                # Skip restricted files since we cannot authenticate to download them
                if f.get("restricted", False) or f.get("dataFile", {}).get("restricted", False):
                    continue
                    
                datafile = f.get("dataFile", {})
                filename = datafile.get("filename", "").lower()
                
                # Check if this specific file matches any of our target extensions
                for t_ext in TARGET_EXTENSIONS:
                    if filename.endswith(f".{t_ext.lower()}"):
                        has_target_extension = True
                        actual_matched_extensions.add(t_ext)
                        
                # Add ALL non-restricted files to the download queue
                files_to_download.append({
                    "key": datafile.get("filename", f"file_{datafile.get('id')}"),
                    "size": datafile.get("filesize", 0),
                    "checksum": f"md5:{datafile.get('md5')}" if datafile.get("md5") else "",
                    "links": {
                        "content": f"{DATAVERSE_NO_API_BASE}/access/datafile/{datafile.get('id')}"
                    }
                })
                        
            # FALSE POSITIVE CHECK
            if not has_target_extension:
                print(f"  False positive in search API or all target files are restricted. No downloadable target extensions actually found in dataset {doi}. Skipping entire dataset.")
                continue

            print(f"  Found {len(files_to_download)} total files in dataset containing target extensions. Queuing ALL for download.")
            
            # Prepare dataset record metadata for downloader
            dataset_record = dataset_data.get("data", {})
            dataset_record["title"] = dataset_title 
            
            # Perform Download
            total_dl, total_bytes, folder_name, downloaded_files = download_record(dataset_record, files_to_download)
            
            if total_dl > 0:
                # Log successful download to local DB
                mark_downloaded(
                    record_id=db_record_id,
                    title=dataset_title,
                    doi=doi,
                    folder_name=folder_name,
                    matched_extensions=list(actual_matched_extensions),
                    total_files=total_dl,
                    total_size_bytes=total_bytes
                )                
                # Insert individual file metadata
                author, year, up_name, up_email, lic = extract_dv_meta(dataset_record)
                for dl_file in downloaded_files:
                    file_info = next((f for f in files_to_download if f["key"] == dl_file), None)
                    file_url = file_info["links"]["content"] if file_info else ""
                    file_type = dl_file.split(".")[-1] if "." in dl_file else ""
                    insert_file_metadata(
                        file_url=file_url,
                        local_dir_name=folder_name,
                        local_file_name=dl_file,
                        context_repository="Dataverse",
                        license=lic,
                        uploader_name=up_name,
                        uploader_email=up_email,
                        doi=doi,
                        file_type=file_type,
                        year=year,
                        author=author
                    )
            else:
                print(f"  Failed to download any files for dataset {doi}. Not marking as completed in DB.")
