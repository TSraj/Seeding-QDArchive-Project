import os
import json
import hashlib
import re
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from .config import RAW_DIR, MAX_DOWNLOAD_WORKERS

MAX_FOLDER_NAME_LENGTH = 200  # macOS limits filenames to 255 bytes

def sanitize_folder_name(title: str) -> str:
    """Remove special characters and truncate to avoid OS filename length limits."""
    sanitized = re.sub(r'[^\w\s-]', '', title).strip()
    if len(sanitized) > MAX_FOLDER_NAME_LENGTH:
        # Append a short hash of the full title to keep truncated names unique
        title_hash = hashlib.md5(title.encode()).hexdigest()[:8]
        sanitized = sanitized[:MAX_FOLDER_NAME_LENGTH].rstrip() + f"_{title_hash}"
    return sanitized

def compute_md5(file_path: Path) -> str:
    """Compute MD5 checksum of a file to compare with Zenodo's."""
    if not file_path.exists():
        return ""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return "md5:" + hash_md5.hexdigest()

def _download_single_file(file_info: dict, record_dir: Path) -> tuple:
    """
    Downloads a single file from a Zenodo record.
    Returns (success: bool, filename: str, file_size: int).
    """
    filename = file_info.get("key")
    if not filename:
        return (False, "", 0)

    file_path = record_dir / filename
    file_size = file_info.get("size", 0)
    expected_md5 = file_info.get("checksum", "")
    download_url = file_info.get("links", {}).get("content")

    if not download_url:
        print(f"  Skipping {filename}: No download link found.")
        return (False, filename, 0)

    # Check if already downloaded and verified
    if file_path.exists():
        local_md5 = compute_md5(file_path)
        if local_md5 == expected_md5:
            print(f"  Skipping {filename}: Already downloaded and verified.")
            return (True, filename, file_size)
        else:
            print(f"  Checksum mismatch for {filename}, re-downloading...")

    # Download file with progress bar
    print(f"  Downloading {filename}...")
    try:
        response = requests.get(download_url, stream=True, timeout=60)
        response.raise_for_status()

        with open(file_path, 'wb') as f:
            with tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024, desc=filename, leave=False) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

        # Verify newly downloaded file checksum
        if compute_md5(file_path) != expected_md5:
            print(f"  Warning: Checksum mismatch for downloaded file {filename}!")
            return (False, filename, 0)
        else:
            return (True, filename, file_size)
    except Exception as e:
        print(f"  Failed to download {filename}: {e}")
        return (False, filename, 0)

def download_record(record: dict, files: list) -> tuple:
    """
    Downloads all files from a Zenodo record in parallel and saves the metadata.
    Returns (total_files_downloaded, total_bytes_downloaded, folder_name).
    """
    title = record.get("title", "Untitled_Record_Unknown")
    folder_name = sanitize_folder_name(title)
    if not folder_name:
        folder_name = f"Zenodo_Record_{record.get('id')}"
        
    record_dir = RAW_DIR / folder_name
    record_dir.mkdir(parents=True, exist_ok=True)
    
    # Save full raw metadata
    metadata_path = record_dir / "metadata.json"
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(record, f, indent=4, ensure_ascii=False)
        
    total_files = 0
    total_bytes = 0
    downloaded_file_names = []
    
    print(f"Downloading record: {title} ({len(files)} files, {MAX_DOWNLOAD_WORKERS} threads)")
    
    # Submit all file downloads to the thread pool
    with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
        futures = {
            executor.submit(_download_single_file, file_info, record_dir): file_info
            for file_info in files
        }
        
        for future in as_completed(futures):
            success, filename, file_size = future.result()
            if success:
                total_files += 1
                total_bytes += file_size
                downloaded_file_names.append(filename)
                
    if total_files == 0:
        print(f"  No files were successfully downloaded for {title} (possibly due to restrictions or network errors). Cleaning up empty folder.")
        import shutil
        shutil.rmtree(record_dir, ignore_errors=True)
            
    return total_files, total_bytes, folder_name, downloaded_file_names
