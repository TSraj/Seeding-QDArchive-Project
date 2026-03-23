import time
import requests
from .config import FIGSHARE_API_BASE, RATE_LIMIT_DELAY, MAX_PAGES, RESULTS_PER_PAGE, TARGET_EXTENSIONS, RAW_DIR, SMART_QUERIES, NON_QUALITATIVE_KEYWORDS
from .downloader import download_record, sanitize_folder_name
from .db import is_downloaded, insert_project, insert_file, insert_keyword, insert_person_role, insert_license

def extract_figshare_meta(article):
    title = article.get("title", "Unknown")
    description = article.get("description", "")
    language = ""
    version = str(article.get("version", ""))
    upload_date = article.get("published_date", "")
    
    keywords = article.get("tags", [])
    if isinstance(keywords, str): keywords = [keywords]
    
    lic = article.get("license", {}).get("name", "")
    
    persons = []
    for a in article.get("authors", []):
        persons.append({"name": a.get("full_name", ""), "role": "author"})
        
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

def api_request(method, url, params=None, json=None):
    """Wrapper for requests to handle rate limit delay gracefully."""
    time.sleep(RATE_LIMIT_DELAY)
    response = requests.request(method, url, params=params, json=json, timeout=30)
    response.raise_for_status()
    return response.json()

def search_figshare(extension: str, max_pages: int = MAX_PAGES):
    """Search Figshare articles for a specific extension."""
    print(f"\n--- Searching Figshare for extension: '{extension}' ---")
    articles_found = []
    
    # Figshare API search using :extension: search syntax
    search_url = f"{FIGSHARE_API_BASE}/articles/search"
    
    for page in range(1, max_pages + 1):
        # Figshare uses page and page_size
        json_body = {
            "search_for": f":extension:{extension}",
            "page": page,
            "page_size": RESULTS_PER_PAGE
        }
        
        try:
            hits = api_request("POST", search_url, json=json_body)
            
            if not hits:
                break
                
            articles_found.extend(hits)
            
            if len(hits) < RESULTS_PER_PAGE:
                break
                
        except Exception as e:
            print(f"Error fetching page {page} from Figshare for '{extension}': {e}")
            break
            
    return articles_found

def get_article_files(article_id: int):
    """Fetch the list of files for a specific Figshare article."""
    try:
        files_url = f"{FIGSHARE_API_BASE}/articles/{article_id}/files"
        return api_request("GET", files_url)
    except Exception as e:
        print(f"Error fetching files for Figshare article {article_id}: {e}")
        return []

def scrape(extensions=TARGET_EXTENSIONS, max_pages=MAX_PAGES, dry_run=False, max_runtime_hours=None):
    """Orchestrates the scraping, filtering, and downloading process for Figshare."""
    processed_article_ids = set()
    start_time = time.time()
    
    for ext in extensions:
        # Check runtime before starting a new extension
        if max_runtime_hours is not None:
            elapsed_hours = (time.time() - start_time) / 3600
            if elapsed_hours >= max_runtime_hours:
                runtime_display = f"{max_runtime_hours * 60:.0f} minutes" if max_runtime_hours < 1 else f"{max_runtime_hours:.2f} hours"
                print(f"\n[INFO] Maximum runtime of {runtime_display} reached. Stopping Figshare scraper.")
                break
                
        articles = search_figshare(ext, max_pages)
        print(f"Found {len(articles)} potential articles for extension '{ext}' in Figshare. Processing...")
        
        for article in articles:
            article_id = article.get("id")
            title = article.get("title", "Unknown_Figshare_Article")
            doi = article.get("doi", f"figshare_{article_id}")
            
            if not article_id:
                continue
                
            if article_id in processed_article_ids:
                continue 
            processed_article_ids.add(article_id)
            
            # Use DOI for DB tracking, or internal ID if DOI missing
            record_id = article_id
            
            # Check if the folder already exists on disk
            folder_name_check = sanitize_folder_name(title)
            if (RAW_DIR / folder_name_check).exists():
                print(f"Skipping Figshare Article '{title}': Folder already exists.")
                continue

            if not dry_run and is_downloaded(record_id):
                print(f"Skipping Figshare Article {record_id}: Already downloaded.")
                continue
                
            # Check runtime before downloading
            if max_runtime_hours is not None:
                elapsed_hours = (time.time() - start_time) / 3600
                if elapsed_hours >= max_runtime_hours:
                    return
            
            # Fetch full file list to verify target extension exists
            files = get_article_files(article_id)
            if not files:
                continue
                
            # Verify if ANY file actually has any of our target extensions
            actual_matched_extensions = set()
            has_target = False
            for f in files:
                filename = f.get("name", "").lower()
                for t_ext in TARGET_EXTENSIONS:
                    if filename.endswith(f".{t_ext.lower()}"):
                        has_target = True
                        actual_matched_extensions.add(t_ext)
            
            if not has_target:
                # False positive from the search API
                continue

            print(f"\n[MATCH] Figshare Article '{title}' ({article_id}) contains target extensions: {list(actual_matched_extensions)}")
            print(f"  Found {len(files)} total files. Queuing ALL for download.")
            
            if dry_run:
                continue
                
            # Map Figshare file format to our internal downloader format
            # Figshare download link is usually in 'download_url'
            files_to_download = []
            for f in files:
                files_to_download.append({
                    "key": f.get("name", f"file_{f.get('id')}"),
                    "size": f.get("size", 0),
                    "checksum": f"md5:{f.get('computed_md5')}" if f.get("computed_md5") else "",
                    "links": {
                        "content": f.get("download_url")
                    }
                })
            
            # Perform Download
            total_dl, total_bytes, folder_name, downloaded_files = download_record(article, files_to_download)
            
            if total_dl > 0:
                parsed_meta = extract_figshare_meta(article)
                project_url = article.get("url_public_html", "")
                
                insert_project(
                    project_id=record_id,
                    query_string=ext,
                    repository_name="Figshare",
                    repository_url="https://figshare.com",
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

def is_qualitative_dataset_figshare(article_data):
    """Check if a dataset is likely qualitative research by screening against exclusion keywords."""
    title = article_data.get("title", "")
    description = article_data.get("description", "")
    combined_text = f"{title} {description}".lower()
    
    for keyword in NON_QUALITATIVE_KEYWORDS:
        if keyword.lower() in combined_text:
            print(f"  [SKIP] Non-qualitative dataset detected (matched '{keyword}'): {title[:80]}")
            return False
    return True

def search_smart_figshare(query: str, max_pages: int = MAX_PAGES):
    """Search Figshare using a smart query."""
    print(f"\n--- Smart Query on Figshare: '{query}' ---")
    articles_found = []
    
    search_url = f"{FIGSHARE_API_BASE}/articles/search"
    
    for page in range(1, max_pages + 1):
        json_body = {
            "search_for": query,
            "page": page,
            "page_size": RESULTS_PER_PAGE
        }
        
        try:
            hits = api_request("POST", search_url, json=json_body)
            
            if not hits:
                break
                
            articles_found.extend(hits)
            
            if len(hits) < RESULTS_PER_PAGE:
                break
                
        except Exception as e:
            print(f"Error fetching page {page} from Figshare for smart query '{query}': {e}")
            break
            
    return articles_found

def scrape_smart(queries=SMART_QUERIES, max_pages=MAX_PAGES, dry_run=False, max_runtime_hours=None):
    """Orchestrates smart-query-based scraping for Figshare."""
    processed_article_ids = set()
    start_time = time.time()
    
    for query in queries:
        if max_runtime_hours is not None:
            elapsed_hours = (time.time() - start_time) / 3600
            if elapsed_hours >= max_runtime_hours:
                print("\n[INFO] Maximum runtime reached. Stopping Figshare smart query scraper.")
                break
                
        articles = search_smart_figshare(query, max_pages)
        print(f"Found {len(articles)} potential articles for smart query '{query}' in Figshare. Processing...")
        
        for article in articles:
            article_id = article.get("id")
            title = article.get("title", "Unknown_Figshare_Article")
            doi = article.get("doi", f"figshare_{article_id}")
            
            if not article_id:
                continue
                
            record_str = f"figshare_smart_{article_id}"
            
            if record_str in processed_article_ids:
                continue 
            processed_article_ids.add(record_str)
            
            folder_name_check = sanitize_folder_name(title)
            if (RAW_DIR / folder_name_check).exists():
                print(f"Skipping Figshare Article '{title}': Folder already exists.")
                continue

            db_record_id = (int(article_id) % (10**8)) + 8500000000

            if not dry_run and is_downloaded(db_record_id):
                print(f"Skipping Figshare Smart Query Article {db_record_id}: Already downloaded.")
                continue
                
            if max_runtime_hours is not None:
                elapsed_hours = (time.time() - start_time) / 3600
                if elapsed_hours >= max_runtime_hours:
                    return
            
            print(f"\n[SMART MATCH] Figshare Article '{title}' ({article_id}) matched smart query: {query}")
            
            # Fetch article details to check description for filtering
            try:
                article_url = f"{FIGSHARE_API_BASE}/articles/{article_id}"
                article_full = api_request("GET", article_url)
            except Exception as e:
                print(f"  Failed to fetch full article details for {article_id}: {e}")
                continue

            if not is_qualitative_dataset_figshare(article_full):
                continue

            files = get_article_files(article_id)
            if not files:
                print(f"  No files found in {article_id}. Skipping.")
                continue
                
            print(f"  Found {len(files)} total files. Queuing ALL for download.")
            
            if dry_run:
                continue
                
            files_to_download = []
            for f in files:
                files_to_download.append({
                    "key": f.get("name", f"file_{f.get('id')}"),
                    "size": f.get("size", 0),
                    "checksum": f"md5:{f.get('computed_md5')}" if f.get("computed_md5") else "",
                    "links": {
                        "content": f.get("download_url")
                    }
                })
            
            total_dl, total_bytes, folder_name, downloaded_files = download_record(article_full, files_to_download)
            
            if total_dl > 0:
                parsed_meta = extract_figshare_meta(article_full)
                project_url = article_full.get("url_public_html", "")
                
                insert_project(
                    project_id=db_record_id,
                    query_string=query,
                    repository_name="Figshare",
                    repository_url="https://figshare.com",
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
