# src/utils/cloud_storage.py
"""
Cloud storage utilities for Lassa Reports Scraping Pipeline.

Handles uploading and downloading files to/from cloud storage (Backblaze B2).
"""

import os
import logging
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

def upload_file(local_path, b2_key=None, bucket=None):
    """
    Upload a file to B2 storage.
    
    Args:
        local_path (str): Path to the local file
        b2_key (str, optional): Key for the B2 object. If None, uses local_path
        bucket (str, optional): B2 bucket name. If None, uses env var
        
    Returns:
        bool: True if upload succeeded, False otherwise
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

def upload_directory(directory, b2_prefix=None, file_extensions=None):
    """
    Upload all files in a directory to B2 (including subdirectories).
    
    Args:
        directory (str): Directory to upload
        b2_prefix (str, optional): B2 prefix to prepend to all keys
        file_extensions (list, optional): List of file extensions to include
        
    Returns:
        dict: Summary of upload results
    """
    directory = Path(directory)
    if not directory.exists():
        logging.error(f"Directory not found: {directory}")
        return {"success": 0, "failed": 0, "total": 0}
    
    all_files = scan_directory(directory, file_extensions)
    
    results = {"success": 0, "failed": 0, "total": len(all_files)}
    for local_path in all_files:
        # Create B2 key with relative path from directory
        rel_path = local_path.relative_to(directory)
        if b2_prefix:
            b2_key = f"{b2_prefix}/{rel_path}"
        else:
            b2_key = str(rel_path)
        
        # Upload the file
        if upload_file(local_path, b2_key):
            results["success"] += 1
        else:
            results["failed"] += 1
    
    logging.info(f"Uploaded {results['success']}/{results['total']} files from {directory}")
    return results