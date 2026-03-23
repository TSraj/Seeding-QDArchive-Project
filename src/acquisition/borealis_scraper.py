import time
import requests
from .config import BOREALIS_API_BASE, RATE_LIMIT_DELAY, MAX_PAGES, RESULTS_PER_PAGE, TARGET_EXTENSIONS, RAW_DIR, SMART_QUERIES, NON_QUALITATIVE_KEYWORDS
from .downloader import download_record, sanitize_folder_name
from .db import is_downloaded, insert_project, insert_file, insert_keyword, insert_person_role, insert_license


def extract_dv_meta(dataset_record):
    title = dataset_record.get("title", "Unknown")
    latest = dataset_record.get("latestVersion", {})
    version = f"{latest.get('versionNumber', 1)}.{latest.get('versionMinorNumber', 0)}"

    blocks = latest.get("metadataBlocks", {})
    citation = blocks.get("citation", {}).get("fields", [])
    
    description = ""
    language = ""
    keywords = []
    persons = []
    
    upload_date = latest.get("releaseTime", "")
    if not upload_date:
        upload_date = latest.get("createTime", "")
        
    for f in citation:
        if f.get("typeName") == "title" and title == "Unknown":
            title = f.get("value", "Unknown")
        elif f.get("typeName") == "author":
            for a in f.get("value", []):
                persons.append({"name": a.get("authorName", {}).get("value", ""), "role": "author"})
        elif f.get("typeName") == "datasetContact":
            for c in f.get("value", []):
                persons.append({"name": c.get("datasetContactName", {}).get("value", ""), "role": "contact"})
        elif f.get("typeName") == "contributor":
            for c in f.get("value", []):
                persons.append({"name": c.get("contributorName", {}).get("value", ""), "role": c.get("contributorType", {}).get("value", "contributor")})
        elif f.get("typeName") == "dsDescription":
            desc_list = [d.get("dsDescriptionValue", {}).get("value", "") for d in f.get("value", [])]
            description = " ".join(desc_list)
        elif f.get("typeName") == "keyword":
            for k in f.get("value", []):
                val = k.get("keywordValue", {}).get("value", "")
                if val: keywords.append(val)
        elif f.get("typeName") == "language":
            langs = f.get("value", [])
            language = ", ".join(langs) if isinstance(langs, list) else str(langs)

    lic = latest.get("license", {}).get("name", "")
    if not lic: 
        lic = str(latest.get("termsOfAccess", ""))[:255]
        
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

def search_borealis(extension: str, max_pages: int = MAX_PAGES):
    """Search Borealis records for an extension query."""
    print(f"\n--- Searching Borealis for query: '{extension}' ---")
    records_found = []
    
    for page in range(1, max_pages + 1):
        # Borealis uses start/per_page for pagination
        start = (page - 1) * RESULTS_PER_PAGE
        params = {
            "q": extension,
            "type": "file",  # Search specifically for files
            "start": start,
            "per_page": RESULTS_PER_PAGE,
            "show_entity_ids": True
        }
        
        try:
            data = api_get(f"{BOREALIS_API_BASE}/search", params)
            data_dict = data.get("data", {})
            hits = data_dict.get("items", [])
            
            if not hits:
                break
                
            records_found.extend(hits)
            
            # If we got fewer than requested, we're on the last page
            if len(hits) < RESULTS_PER_PAGE:
                break
                
        except Exception as e:
            print(f"Error fetching page {page} from Borealis for '{extension}': {e}")
            break
            
    return records_found

def scrape(extensions=TARGET_EXTENSIONS, max_pages=MAX_PAGES, dry_run=False, max_runtime_hours=None):
    """Orchestrates the scraping, filtering, and downloading process for Borealis."""
    processed_dataset_ids = set()
    start_time = time.time()
    
    for ext in extensions:
        # Check runtime before starting a new extension
        if max_runtime_hours is not None:
            elapsed_hours = (time.time() - start_time) / 3600
            if elapsed_hours >= max_runtime_hours:
                runtime_display = f"{max_runtime_hours * 60:.0f} minutes" if max_runtime_hours < 1 else f"{max_runtime_hours:.2f} hours"
                print(f"\n[INFO] Maximum runtime of {runtime_display} reached. Stopping Borealis scraper.")
                break
                
        files = search_borealis(ext, max_pages)
        print(f"Found {len(files)} files for query '{ext}' in Borealis. Processing...")
        
        for file_item in files:
            # Borealis search returns files. We group them by dataset using the DOI.
            dataset_id = file_item.get("dataset_citation", "Unknown_Dataset")
            
            dataset_title = file_item.get("dataset_citation", f"Borealis_Dataset_{dataset_id}").split(',')[0].strip() # Take the first part of the citation (usually Author / Title)
            
            doi = file_item.get("dataset_persistent_id", "")
            if not doi:
                print(f"File '{file_item.get('name')}' has no dataset DOI. Skipping.")
                continue
                
            record_id = f"bor_{doi}" # Use DOI as the unique identifier for the processed dataset
            
            if record_id in processed_dataset_ids:
                # Already processed this dataset entirely
                continue 
            processed_dataset_ids.add(record_id)
            
            # Check if the folder already exists on disk using the intended dataset folder name
            folder_name_check = sanitize_folder_name(dataset_title)
            
            if (RAW_DIR / folder_name_check).exists():
                print(f"Skipping Borealis Dataset '{dataset_title}': Folder '{folder_name_check}' already exists in raw/. Skipping.")
                continue

            # Convert record_id to an integer offset (e.g. 1 billion base to avoid clashes)
            # Use hash of DOI to ensure determinism
            db_record_id = (hash(doi) % (10**8)) + 3000000000
                
            if not dry_run and is_downloaded(db_record_id):
                print(f"Skipping Borealis Dataset {db_record_id}: Already completely downloaded and logged in DB.")
                continue
                
            # Check runtime before downloading a new record
            if max_runtime_hours is not None:
                elapsed_hours = (time.time() - start_time) / 3600
                if elapsed_hours >= max_runtime_hours:
                    runtime_display = f"{max_runtime_hours * 60:.0f} minutes" if max_runtime_hours < 1 else f"{max_runtime_hours:.2f} hours"
                    print(f"\n[INFO] Maximum runtime of {runtime_display} reached. Stopping Borealis scraper.")
                    return
                
            print(f"\n[MATCH] Dataset '{dataset_title}' ({doi}) contains matching extension: {ext}")
            
            # Fetch ALL files for this Dataset
            print(f"  Fetching full file list for dataset {doi}...")
            try:
                dataset_url = f"{BOREALIS_API_BASE}/datasets/:persistentId/?persistentId={doi}"
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
                        "content": f"{BOREALIS_API_BASE}/access/datafile/{datafile.get('id')}"
                    }
                })
                        
            # FALSE POSITIVE CHECK: The Search API returned this dataset, but the dataset itself doesn't contain the target extension.
            # This happens because Borealis search matches on metadata text (like description) instead of strict file extensions.
            if not has_target_extension:
                print(f"  False positive in search API or all target files are restricted. No downloadable target extensions actually found in dataset {doi}. Skipping entire dataset.")
                continue

            print(f"  Found {len(files_to_download)} total files in dataset containing target extensions. Queuing ALL for download.")
            
            # Prepare dataset record metadata for downloader
            dataset_record = dataset_data.get("data", {})
            dataset_record["title"] = dataset_title # Inject "title" into dataset_record so downloader.py uses it
            
            # Perform Download
            total_dl, total_bytes, folder_name, downloaded_files = download_record(dataset_record, files_to_download)
            
            if total_dl > 0:
                parsed_meta = extract_dv_meta(dataset_record)
                project_url = f"{BOREALIS_API_BASE.replace('/api', '')}/dataset.xhtml?persistentId={doi}"
                
                insert_project(
                    project_id=db_record_id,
                    query_string=ext,
                    repository_name="Borealis",
                    repository_url=BOREALIS_API_BASE.replace('/api', ''),
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
                    insert_license(db_record_id, parsed_meta["license"])
                    
                for kw in parsed_meta["keywords"]:
                    if kw:
                        insert_keyword(db_record_id, str(kw))
                        
                for p in parsed_meta["persons"]:
                    if p["name"]:
                        insert_person_role(db_record_id, p["name"], p["role"])
                        
                for dl_file in downloaded_files:
                    file_type = dl_file.split(".")[-1] if "." in dl_file else ""
                    insert_file(
                        project_id=db_record_id,
                        file_name=dl_file,
                        file_type=file_type,
                        status="SUCCESS"
                    )
            else:
                print(f"  Failed to download any files for dataset {doi}. Not marking as completed in DB.")


def is_qualitative_dataset(dataset_data):
    """Check if a dataset is likely qualitative research by screening against exclusion keywords."""
    latest = dataset_data.get("latestVersion", {})
    blocks = latest.get("metadataBlocks", {})
    citation_fields = blocks.get("citation", {}).get("fields", [])
    
    title = ""
    description = ""
    for field in citation_fields:
        if field.get("typeName") == "title":
            title = field.get("value", "")
        elif field.get("typeName") == "dsDescription":
            desc_values = field.get("value", [])
            description = " ".join(
                d.get("dsDescriptionValue", {}).get("value", "") for d in desc_values
            )
    
    combined_text = f"{title} {description}".lower()
    
    for keyword in NON_QUALITATIVE_KEYWORDS:
        if keyword.lower() in combined_text:
            print(f"  [SKIP] Non-qualitative dataset detected (matched '{keyword}'): {title[:80]}")
            return False
    return True


def search_smart(query: str, max_pages: int = MAX_PAGES):
    """Search Borealis using a smart query, returning both file and dataset results."""
    print(f"\n--- Smart Query on Borealis: '{query}' ---")
    records_found = []
    
    for page in range(1, max_pages + 1):
        start = (page - 1) * RESULTS_PER_PAGE
        
        try:
            url = f"{BOREALIS_API_BASE}/search"
            full_url = f"{url}?q={requests.utils.quote(query)}&type=dataset&type=file&start={start}&per_page={RESULTS_PER_PAGE}&show_entity_ids=true"
            time.sleep(RATE_LIMIT_DELAY)
            response = requests.get(full_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            data_dict = data.get("data", {})
            hits = data_dict.get("items", [])
            
            if not hits:
                break
                
            records_found.extend(hits)
            
            if len(hits) < RESULTS_PER_PAGE:
                break
                
        except Exception as e:
            print(f"Error fetching page {page} from Borealis for smart query '{query}': {e}")
            break
            
    return records_found


def scrape_smart(queries=SMART_QUERIES, max_pages=MAX_PAGES, dry_run=False, max_runtime_hours=None):
    """Orchestrates smart-query-based scraping for Borealis."""
    processed_dataset_ids = set()
    start_time = time.time()
    
    for query in queries:
        if max_runtime_hours is not None:
            elapsed_hours = (time.time() - start_time) / 3600
            if elapsed_hours >= max_runtime_hours:
                print("\n[INFO] Maximum runtime reached. Stopping Borealis smart query scraper.")
                break
                
        results = search_smart(query, max_pages)
        print(f"Found {len(results)} results for smart query '{query}' in Borealis. Processing...")
        
        for item in results:
            if item.get("type") == "dataset":
                doi = item.get("global_id", "")
                dataset_title = item.get("name", f"Borealis_Dataset_{doi}")
            else:
                doi = item.get("dataset_persistent_id", "")
                dataset_title = item.get("dataset_citation", f"Borealis_Dataset_{doi}").split(',')[0].strip()
            
            if not doi:
                continue
                
            record_id = f"bor_smart_{doi}"
            
            if record_id in processed_dataset_ids:
                continue
            processed_dataset_ids.add(record_id)
            
            folder_name_check = sanitize_folder_name(dataset_title)
            
            if (RAW_DIR / folder_name_check).exists():
                print(f"Skipping Borealis Dataset '{dataset_title}': Folder already exists.")
                continue
            
            db_record_id = (hash(doi) % (10**8)) + 3500000000
                
            if not dry_run and is_downloaded(db_record_id):
                print(f"Skipping Borealis Smart Query Dataset {db_record_id}: Already downloaded.")
                continue
                
            if max_runtime_hours is not None:
                elapsed_hours = (time.time() - start_time) / 3600
                if elapsed_hours >= max_runtime_hours:
                    return
                
            print(f"\n[SMART MATCH] Dataset '{dataset_title}' ({doi}) matched smart query: {query}")
            
            print(f"  Fetching full file list for dataset {doi}...")
            try:
                dataset_url = f"{BOREALIS_API_BASE}/datasets/:persistentId/?persistentId={doi}"
                dataset_data = api_get(dataset_url)
                all_files = dataset_data.get("data", {}).get("latestVersion", {}).get("files", [])
            except Exception as e:
                print(f"  Failed to fetch dataset files for {doi}: {e}")
                continue
                
            if not all_files:
                print(f"  No files found in latest version of {doi}.")
                continue
            
            if not is_qualitative_dataset(dataset_data.get("data", {})):
                continue
                
            print(f"  Found {len(all_files)} total files in dataset.")
            
            if dry_run:
                continue
                
            files_to_download = []
            
            for f in all_files:
                if f.get("restricted", False) or f.get("dataFile", {}).get("restricted", False):
                    continue
                    
                datafile = f.get("dataFile", {})
                files_to_download.append({
                    "key": datafile.get("filename", f"file_{datafile.get('id')}"),
                    "size": datafile.get("filesize", 0),
                    "checksum": f"md5:{datafile.get('md5')}" if datafile.get("md5") else "",
                    "links": {
                        "content": f"{BOREALIS_API_BASE}/access/datafile/{datafile.get('id')}"
                    }
                })
            
            if not files_to_download:
                print(f"  No downloadable (non-restricted) files in dataset {doi}. Skipping.")
                continue
                        
            print(f"  Queuing {len(files_to_download)} files for download (smart query match).")
            
            dataset_record = dataset_data.get("data", {})
            dataset_record["title"] = dataset_title
            
            total_dl, total_bytes, folder_name, downloaded_files = download_record(dataset_record, files_to_download)
            
            if total_dl > 0:
                parsed_meta = extract_dv_meta(dataset_record)
                project_url = f"{BOREALIS_API_BASE.replace('/api', '')}/dataset.xhtml?persistentId={doi}"
                
                insert_project(
                    project_id=db_record_id,
                    query_string=query,
                    repository_name="Borealis",
                    repository_url=BOREALIS_API_BASE.replace('/api', ''),
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
                    insert_license(db_record_id, parsed_meta["license"])
                    
                for kw in parsed_meta["keywords"]:
                    if kw:
                        insert_keyword(db_record_id, str(kw))
                        
                for p in parsed_meta["persons"]:
                    if p["name"]:
                        insert_person_role(db_record_id, p["name"], p["role"])
                        
                for dl_file in downloaded_files:
                    file_type = dl_file.split(".")[-1] if "." in dl_file else ""
                    insert_file(
                        project_id=db_record_id,
                        file_name=dl_file,
                        file_type=file_type,
                        status="SUCCESS"
                    )
            else:
                print(f"  Failed to download any files for dataset {doi}. Not marking as completed in DB.")
