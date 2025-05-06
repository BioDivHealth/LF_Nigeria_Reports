# src/utils/cloud_storage.py
"""
Cloud storage utilities for Lassa Reports Scraping Pipeline.

Handles uploading and downloading files to/from cloud storage (Backblaze B2).
"""

import os
import logging
import time
from pathlib import Path
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from b2sdk.v2.exception import B2Error

def get_b2_api():
    """
    Create and return a B2 API client using environment variables.
    """
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    application_key_id = os.environ.get('B2_APPLICATION_KEY_ID')
    application_key = os.environ.get('B2_APPLICATION_KEY')
    b2_api.authorize_account('production', application_key_id, application_key)
    return b2_api

# Cache for file existence checks to reduce API calls
_file_existence_cache = {}

# Cache for file listings to reduce API calls
_file_listing_cache = {}

def get_files_in_directory(bucket_obj, directory_prefix):
    """
    Get all files in a directory prefix, using cache when possible.
    
    Args:
        bucket_obj: B2 bucket object
        directory_prefix (str): Directory prefix to list
        
    Returns:
        list: List of file names in the directory
    """
    global _file_listing_cache
    
    # Check if we have this directory in cache
    if directory_prefix in _file_listing_cache:
        return _file_listing_cache[directory_prefix]
    
    try:
        # This is a Class C transaction, but we only do it once per directory
        file_names = []
        for file_info, _ in bucket_obj.ls(directory_prefix):
            file_names.append(file_info.file_name)
            # Also update the existence cache while we're at it
            _file_existence_cache[file_info.file_name] = True
        
        # Cache the results
        _file_listing_cache[directory_prefix] = file_names
        return file_names
    except B2Error as e:
        logging.warning(f"Error listing files in {directory_prefix}: {str(e)}")
        return []

def file_exists_in_bucket(bucket_obj, file_name):
    """
    Check if a file exists in the B2 bucket, using cache when possible.
    
    Args:
        bucket_obj: B2 bucket object
        file_name (str): File name/key to check
        
    Returns:
        tuple: (exists, file_info) - exists is a boolean, file_info is the file info if exists is True, None otherwise
    """
    global _file_existence_cache
    
    # Check if we have this file in cache
    if file_name in _file_existence_cache:
        exists = _file_existence_cache[file_name]
        return exists, None  # We don't cache file_info, just existence
    
    # Get the directory prefix (everything up to the last slash)
    directory_prefix = '/'.join(file_name.split('/')[:-1])
    if not directory_prefix:
        directory_prefix = ''
    
    # Get all files in the directory to efficiently check existence
    # This will also update our cache
    files_in_dir = get_files_in_directory(bucket_obj, directory_prefix)
    
    # Now check if our file is in the list
    exists = file_name in files_in_dir
    _file_existence_cache[file_name] = exists
    
    return exists, None

def upload_file(local_path, b2_key=None, bucket=None, skip_if_exists=True):
    """
    Upload a file to B2 storage.
    
    Args:
        local_path (str): Path to the local file
        b2_key (str, optional): Key for the B2 object. If None, uses local_path
        bucket (str, optional): B2 bucket name. If None, uses env var
        skip_if_exists (bool, optional): Skip upload if file already exists in bucket
        
    Returns:
        bool: True if upload succeeded or file already exists, False otherwise
    """
    local_path = Path(local_path)
    if not local_path.exists():
        logging.error(f"Upload failed: File not found: {local_path}")
        return False
        
    # Use default bucket from env if not specified
    if bucket is None:
        bucket = os.environ.get('B2_BUCKET_NAME')
        if not bucket:
            logging.error("Upload failed: No bucket specified and B2_BUCKET_NAME not set")
            return False
    
    # Use local path as the B2 key if not specified
    if b2_key is None:
        # Remove any drive letter (Windows) and convert to forward slashes
        b2_key = str(local_path).replace('\\', '/')
        # Remove leading slash and drive letter if present
        b2_key = b2_key.lstrip('/').split(':', 1)[-1].lstrip('/')
    
    try:
        b2_api = get_b2_api()
        bucket_obj = b2_api.get_bucket_by_name(bucket)
        
        # Check if file already exists in bucket
        if skip_if_exists:
            exists, file_info = file_exists_in_bucket(bucket_obj, b2_key)
            if exists:
                # File already exists, skip upload
                logging.info(f"Skipping upload for {local_path} - already exists in b2://{bucket}/{b2_key}")
                return True
        
        # Upload the file
        uploaded_file = bucket_obj.upload_local_file(
            local_file=str(local_path),
            file_name=b2_key,
            content_type='b2/x-auto'
        )
        
        logging.info(f"Successfully uploaded {local_path} to b2://{bucket}/{b2_key}")
        return True
    except B2Error as e:
        logging.error(f"Upload failed: {str(e)}")
        return False

def download_file(b2_key, local_path, bucket=None):
    """
    Download a file from B2 storage.
    
    Args:
        b2_key (str): B2 object key
        local_path (str): Path where to save the downloaded file
        bucket (str, optional): B2 bucket name. If None, uses env var
        
    Returns:
        bool: True if download succeeded, False otherwise
    """
    # Use default bucket from env if not specified
    if bucket is None:
        bucket = os.environ.get('B2_BUCKET_NAME')
        if not bucket:
            logging.error("Download failed: No bucket specified and B2_BUCKET_NAME not set")
            return False
    
    local_path = Path(local_path)
    # Create directory if it doesn't exist
    local_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        b2_api = get_b2_api()
        bucket_obj = b2_api.get_bucket_by_name(bucket)
        
        # Download the file
        download_dest = bucket_obj.download_file_by_name(
            file_name=b2_key,
            download_dest=str(local_path)
        )
        
        logging.info(f"Successfully downloaded b2://{bucket}/{b2_key} to {local_path}")
        return True
    except B2Error as e:
        logging.error(f"Download failed: {str(e)}")
        return False

def scan_directory(directory, file_extensions=None):
    """
    Scan a directory and return list of files to upload.
    
    Args:
        directory (str): Directory to scan
        file_extensions (list, optional): List of file extensions to include
        
    Returns:
        list: List of Path objects for files to upload
    """
    directory = Path(directory)
    if not directory.exists():
        return []
        
    all_files = []
    for path in directory.glob('**/*'):
        if path.is_file():
            if file_extensions is None or path.suffix.lower() in file_extensions:
                all_files.append(path)
    
    return all_files

def upload_directory(directory, b2_prefix=None, file_extensions=None, skip_if_exists=True, batch_size=50, delay_seconds=5):
    """
    Upload all files in a directory to B2 (including subdirectories) with batching to avoid API limits.
    
    Args:
        directory (str): Directory to upload
        b2_prefix (str, optional): B2 prefix to prepend to all keys
        file_extensions (list, optional): List of file extensions to include
        skip_if_exists (bool, optional): Skip upload if file already exists in bucket
        batch_size (int, optional): Number of files to process in each batch
        delay_seconds (int, optional): Delay in seconds between batches
        
    Returns:
        dict: Summary of upload results
    """
    directory = Path(directory)
    if not directory.exists():
        logging.error(f"Directory not found: {directory}")
        return {"success": 0, "failed": 0, "total": 0, "skipped": 0}
    
    all_files = scan_directory(directory, file_extensions)
    total_files = len(all_files)
    
    # Initialize B2 client and bucket once to reuse
    b2_api = None
    bucket_obj = None
    
    # Process files in batches
    results = {"success": 0, "failed": 0, "total": total_files, "skipped": 0}
    
    # Clear the file listing cache for this directory prefix
    global _file_listing_cache
    if b2_prefix:
        if b2_prefix in _file_listing_cache:
            del _file_listing_cache[b2_prefix]
    
    for i in range(0, total_files, batch_size):
        batch = all_files[i:i+batch_size]
        logging.info(f"Processing batch {i//batch_size + 1}/{(total_files + batch_size - 1)//batch_size} "
                     f"({len(batch)} files, {i}/{total_files} processed so far)")
        
        # Initialize B2 client and bucket if not already done
        if b2_api is None:
            b2_api = get_b2_api()
            bucket_name = os.environ.get('B2_BUCKET_NAME')
            bucket_obj = b2_api.get_bucket_by_name(bucket_name)
        
        # Pre-fetch file listings for directories in this batch to reduce API calls
        if skip_if_exists:
            directories = set()
            for local_path in batch:
                rel_path = local_path.relative_to(directory)
                if b2_prefix:
                    b2_key = f"{b2_prefix}/{rel_path}"
                else:
                    b2_key = str(rel_path)
                
                # Get the directory part
                dir_part = '/'.join(b2_key.split('/')[:-1])
                if dir_part:
                    directories.add(dir_part)
            
            # Fetch file listings for all directories in this batch
            for dir_prefix in directories:
                get_files_in_directory(bucket_obj, dir_prefix)
        
        # Process each file in the batch
        for local_path in batch:
            # Create B2 key with relative path from directory
            rel_path = local_path.relative_to(directory)
            if b2_prefix:
                b2_key = f"{b2_prefix}/{rel_path}"
            else:
                b2_key = str(rel_path)
            
            # Check if file exists (using our cached data)
            if skip_if_exists:
                exists, _ = file_exists_in_bucket(bucket_obj, b2_key)
                if exists:
                    logging.info(f"Skipping upload for {local_path} - already exists in bucket")
                    results["skipped"] += 1
                    continue
            
            # Upload the file
            try:
                uploaded_file = bucket_obj.upload_local_file(
                    local_file=str(local_path),
                    file_name=b2_key,
                    content_type='b2/x-auto'
                )
                logging.info(f"Successfully uploaded {local_path} to bucket/{b2_key}")
                results["success"] += 1
                # Update the cache
                _file_existence_cache[b2_key] = True
            except B2Error as e:
                logging.error(f"Upload failed for {local_path}: {str(e)}")
                results["failed"] += 1
        
        # Add delay between batches to avoid hitting API limits
        if i + batch_size < total_files and delay_seconds > 0:
            logging.info(f"Pausing for {delay_seconds} seconds to avoid API limits...")
            time.sleep(delay_seconds)
    
    logging.info(f"Processed {results['total']} files from {directory}: "
                 f"{results['success']} uploaded, {results['skipped']} skipped, {results['failed']} failed")
    return results