# This script is a helper script to download files from a minio server bucket/folder
# Point it to the bucket and folder you'd like to download from and specify local destination

# You will need to have the following environment variables set:
# MINIO_ACCESS_KEY
# MINIO_SECRET_KEY

import os
from minio import Minio
from minio.error import S3Error
from tqdm import tqdm
from pathlib import Path

def download_files(minio_client, bucket_name, folder_name, local_directory):
    """
    Download all files from a specific folder in a MinIO bucket to a local directory.
    
    Args:
        minio_client: MinIO client object
        bucket_name: Name of the bucket
        folder_name: Folder path within the bucket (can be empty string for root)
        local_directory: Local directory to download files to
    """
    
    # Create local directory if it doesn't exist
    Path(local_directory).mkdir(parents=True, exist_ok=True)
    
    # List all objects in the specified folder
    try:
        objects = minio_client.list_objects(bucket_name, prefix=folder_name, recursive=True)
        
        # Collect all objects to download
        all_objects = []
        for obj in objects:
            # Skip if it's a directory (ends with /)
            if not obj.object_name.endswith('/'):
                all_objects.append(obj)
        
        if not all_objects:
            print(f"No files found in {bucket_name}/{folder_name}")
            return
            
        print(f"Found {len(all_objects)} files to download from {bucket_name}/{folder_name}")
        
        # Download files with progress bar
        for obj in tqdm(all_objects, desc="Downloading files"):
            object_name = obj.object_name
            
            # Create local file path
            if folder_name:
                # Remove the folder prefix to get relative path
                relative_path = object_name[len(folder_name):].lstrip('/')
            else:
                relative_path = object_name
                
            local_file_path = os.path.join(local_directory, relative_path)
            
            # Create subdirectories if needed
            local_file_dir = os.path.dirname(local_file_path)
            if local_file_dir:
                Path(local_file_dir).mkdir(parents=True, exist_ok=True)
            
            # Check if file already exists locally
            if os.path.exists(local_file_path):
                # Check if file sizes match (simple check for existing files)
                local_size = os.path.getsize(local_file_path)
                remote_size = obj.size
                if local_size == remote_size:
                    tqdm.write(f"File {relative_path} already exists with same size. Skipping.")
                    continue
                else:
                    tqdm.write(f"File {relative_path} exists but different size. Re-downloading.")
            
            try:
                # Download the file
                minio_client.fget_object(bucket_name, object_name, local_file_path)
                tqdm.write(f"Downloaded {object_name} to {local_file_path}")
                
            except S3Error as e:
                tqdm.write(f"Failed to download {object_name}")
                tqdm.write(str(e))
            except Exception as e:
                tqdm.write(f"Unexpected error downloading {object_name}: {e}")
                
    except S3Error as e:
        print(f"Error accessing bucket {bucket_name}: {e}")
        return
    
    print("Download complete!")

def list_bucket_contents(minio_client, bucket_name, folder_name=""):
    """
    List contents of a bucket/folder for inspection.
    
    Args:
        minio_client: MinIO client object
        bucket_name: Name of the bucket
        folder_name: Optional folder path to list (empty for root)
    """
    try:
        objects = minio_client.list_objects(bucket_name, prefix=folder_name, recursive=False)
        
        print(f"\nContents of {bucket_name}/{folder_name}:")
        print("-" * 50)
        
        folders = []
        files = []
        
        for obj in objects:
            if obj.object_name.endswith('/'):
                folders.append(obj.object_name)
            else:
                files.append((obj.object_name, obj.size))
        
        if folders:
            print("Folders:")
            for folder in sorted(folders):
                print(f"  📁 {folder}")
        
        if files:
            print("Files:")
            for filename, size in sorted(files):
                size_mb = size / (1024 * 1024)
                print(f"  📄 {filename} ({size_mb:.2f} MB)")
                
        if not folders and not files:
            print("  (Empty)")
            
    except S3Error as e:
        print(f"Error listing bucket contents: {e}")

if __name__ == '__main__':
    # Initialize MinIO client
    minio_client = Minio("admin.nmdcdemo.emsl.pnl.gov",
                         access_key=os.environ["MINIO_ACCESS_KEY"],
                         secret_key=os.environ["MINIO_SECRET_KEY"],
                         secure=True)
    
    # Configuration
    bucket_name = "metabolomics"  # Specify the bucket name
    folder_name = "bioscales_11_r2h77870/processed_20251013"  # Specify the folder name within the bucket (empty string for root)
    local_directory = "/Volumes/LaCie/nmdc_data/_bioscales_lcms/processed_20251013"  # Local destination directory
    
    # Check if the bucket exists
    if not minio_client.bucket_exists(bucket_name):
        print(f"Bucket {bucket_name} does not exist.")
    else:
        # List contents first to see what's available
        print(f"Checking contents of bucket: {bucket_name}")
        list_bucket_contents(minio_client, bucket_name, folder_name)
        
        # Proceed with download automatically
        print(f"\nStarting download to {local_directory}")
        download_files(minio_client, bucket_name, folder_name, local_directory)