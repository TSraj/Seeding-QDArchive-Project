import time
import requests
from .config import FIGSHARE_API_BASE, RATE_LIMIT_DELAY, MAX_PAGES, RESULTS_PER_PAGE, TARGET_EXTENSIONS, RAW_DIR
from .downloader import download_record, sanitize_folder_name
from .db import is_downloaded, mark_downloaded, insert_file_metadata

def extract_figshare_meta(article):
    author = "; ".join([a.get("full_name", "") for a in article.get("authors", [])])
    year = article.get("published_date", "")[:4] if article.get("published_date") else ""
    return author, year, author, "", ""

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
                mark_downloaded(
                    record_id=record_id,
                    title=title,
                    doi=doi,
                    folder_name=folder_name,
                    matched_extensions=list(actual_matched_extensions),
                    total_files=total_dl,
                    total_size_bytes=total_bytes
                )
                
                author, year, up_name, up_email, lic = extract_figshare_meta(article)
                for dl_file in downloaded_files:
                    file_info = next((f for f in files_to_download if f["key"] == dl_file), None)
                    file_url = file_info["links"]["content"] if file_info else ""
                    file_type = dl_file.split(".")[-1] if "." in dl_file else ""
                    insert_file_metadata(
                        file_url=file_url,
                        local_dir_name=folder_name,
                        local_file_name=dl_file,
                        context_repository="Figshare",
                        license=lic[:100],
                        uploader_name=up_name,
                        uploader_email=up_email,
                        doi=doi,
                        file_type=file_type,
                        year=year,
                        author=author
                    )
