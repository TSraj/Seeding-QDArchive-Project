import time
import requests
from .config import OSF_API_BASE, RATE_LIMIT_DELAY, MAX_PAGES, RESULTS_PER_PAGE, TARGET_EXTENSIONS, RAW_DIR
from .downloader import download_record, sanitize_folder_name
from .db import is_downloaded, mark_downloaded, insert_file_metadata

def extract_osf_meta(attrs):
    author = ""
    year = attrs.get("date_created", "")[:4] if attrs.get("date_created") else ""
    return author, year, author, "", ""

def api_get(url, params=None):
    """Wrapper for requests to handle rate limit delay gracefully."""
    time.sleep(RATE_LIMIT_DELAY)
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def search_osf(extension: str, max_pages: int = MAX_PAGES):
    """Search OSF nodes (projects) for a specific extension keyword."""
    print(f"\n--- Searching OSF for keyword: '{extension}' ---")
    nodes_found = []
    
    # OSF uses a nodes search endpoint
    search_url = f"{OSF_API_BASE}/nodes/"
    
    for page in range(1, max_pages + 1):
        params = {
            "q": extension,
            "page": page,
            "page[size]": RESULTS_PER_PAGE
        }
        
        try:
            data = api_get(search_url, params)
            hits = data.get("data", [])
            
            if not hits:
                break
                
            nodes_found.extend(hits)
            
            # Check for next page in links
            if not data.get("links", {}).get("next"):
                break
                
        except Exception as e:
            print(f"Error searching OSF for '{extension}': {e}")
            break
            
    return nodes_found

def get_all_node_files(node_id: str):
    """
    Recursively fetch all files from all storage providers of an OSF node.
    Returns a list of file info objects.
    """
    files_found = []
    
    try:
        # 1. Get storage providers
        providers_url = f"{OSF_API_BASE}/nodes/{node_id}/files/"
        providers_data = api_get(providers_url)
        providers = providers_data.get("data", [])
        
        for provider in providers:
            provider_name = provider.get("attributes", {}).get("name")
            # Usually 'osfstorage', but could be 'github', 'dropbox', etc.
            # We follow the 'files' link for each provider
            files_link = provider.get("relationships", {}).get("files", {}).get("links", {}).get("related", {}).get("href")
            if files_link:
                files_found.extend(_fetch_files_recursively(files_link))
                
    except Exception as e:
        print(f"Error fetching storage for OSF node {node_id}: {e}")
        
    return files_found

def _fetch_files_recursively(url):
    """Helper to traverse OSF file/folder tree."""
    results = []
    try:
        data = api_get(url)
        items = data.get("data", [])
        
        for item in items:
            kind = item.get("attributes", {}).get("kind")
            if kind == "file":
                results.append(item)
            elif kind == "folder":
                # Recurse into folder
                folder_link = item.get("relationships", {}).get("files", {}).get("links", {}).get("related", {}).get("href")
                if folder_link:
                    results.extend(_fetch_files_recursively(folder_link))
                    
        # Handle pagination for large folders
        next_link = data.get("links", {}).get("next")
        if next_link:
            results.extend(_fetch_files_recursively(next_link))
            
    except Exception as e:
        print(f"Error traversing OSF files at {url}: {e}")
        
    return results

def scrape(extensions=TARGET_EXTENSIONS, max_pages=MAX_PAGES, dry_run=False, max_runtime_hours=None):
    """Orchestrates the scraping, filtering, and downloading process for OSF."""
    processed_node_ids = set()
    start_time = time.time()
    
    for ext in extensions:
        # Check runtime
        if max_runtime_hours is not None:
            elapsed_hours = (time.time() - start_time) / 3600
            if elapsed_hours >= max_runtime_hours:
                break
                
        nodes = search_osf(ext, max_pages)
        print(f"Found {len(nodes)} potential nodes for extension '{ext}' in OSF. Processing...")
        
        for node in nodes:
            node_id = node.get("id")
            attrs = node.get("attributes", {})
            title = attrs.get("title", "Unknown_OSF_Node")
            
            if not node_id:
                continue
                
            if node_id in processed_node_ids:
                continue 
            processed_node_ids.add(node_id)
            
            # Use OSF Node ID for DB tracking
            record_id = f"osf_{node_id}"
            
            # Check if the folder already exists
            folder_name_check = sanitize_folder_name(title)
            if (RAW_DIR / folder_name_check).exists():
                print(f"Skipping OSF Node '{title}': Folder already exists.")
                continue

            if not dry_run and is_downloaded(record_id):
                print(f"Skipping OSF Node {record_id}: Already downloaded.")
                continue

            # Fetch ALL files recursively
            print(f"  Fetching full file list for OSF node {node_id}...")
            all_osf_files = get_all_node_files(node_id)
            if not all_osf_files:
                continue
                
            # Verify if ANY file has target extension
            has_target = False
            actual_matched_extensions = set()
            for f in all_osf_files:
                filename = f.get("attributes", {}).get("name", "").lower()
                for t_ext in TARGET_EXTENSIONS:
                    if filename.endswith(f".{t_ext.lower()}"):
                        has_target = True
                        actual_matched_extensions.add(t_ext)
            
            if not has_target:
                continue

            print(f"\n[MATCH] OSF Project '{title}' ({node_id}) matches: {list(actual_matched_extensions)}")
            print(f"  Found {len(all_osf_files)} total files. Queuing ALL for download.")
            
            if dry_run:
                continue
                
            # Prepare for downloader
            files_to_download = []
            for f in all_osf_files:
                f_attrs = f.get("attributes", {})
                # Note: OSF download link is in 'links' -> 'download'
                # But it might be in 'links' -> 'move' or other places depending on provider.
                # Usually v2 files have a 'download' link.
                download_url = f.get("links", {}).get("download")
                
                if download_url:
                    files_to_download.append({
                        "key": f_attrs.get("path", f_attrs.get("name")), # Use path for nested files
                        "size": f_attrs.get("size", 0),
                        "checksum": "", # OSF doesn't always provide simple MD5 in the same way
                        "links": {
                            "content": download_url
                        }
                    })
            
            # Perform Download
            total_dl, total_bytes, folder_name, downloaded_files = download_record(attrs, files_to_download)
            
            if total_dl > 0:
                mark_downloaded(
                    record_id=record_id,
                    title=title,
                    doi=node_id,
                    folder_name=folder_name,
                    matched_extensions=list(actual_matched_extensions),
                    total_files=total_dl,
                    total_size_bytes=total_bytes
                )
                
                author, year, up_name, up_email, lic = extract_osf_meta(attrs)
                for dl_file in downloaded_files:
                    file_info = next((f for f in files_to_download if f["key"] == dl_file), None)
                    file_url = file_info["links"]["content"] if file_info else ""
                    file_type = dl_file.split(".")[-1] if "." in dl_file else ""
                    insert_file_metadata(
                        file_url=file_url,
                        local_dir_name=folder_name,
                        local_file_name=dl_file,
                        context_repository="OSF",
                        license=lic[:100],
                        uploader_name=up_name,
                        uploader_email=up_email,
                        doi=node_id,
                        file_type=file_type,
                        year=year,
                        author=author
                    )
