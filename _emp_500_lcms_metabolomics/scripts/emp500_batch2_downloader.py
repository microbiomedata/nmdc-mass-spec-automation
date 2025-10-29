"""
EMP500 Batch 2 Downloader Script

This script performs the following operations:

1. Loads mapped_raw_data_files.csv and removes files already processed in batch 1
   (using mapped_raw_data_files_20250812_batch1.csv as reference)

2. Checks /Volumes/LaCie/nmdc_data/_emp500/raw for existing files and downloads
   missing raw data files using HTTPS URLs converted from FTP locations

3. Downloads all processed files from MinIO metabolomics bucket under 
   emp500_11_547rwq94_lcms/processed_20251021/ to local folder
   /Volumes/LaCie/nmdc_data/_emp500/processed_20251021/, skipping files that already exist locally

4. Saves the batch 2 file list for further processing

Requirements:
- Environment variables: MINIO_ACCESS_KEY and MINIO_SECRET_KEY

"""
import os
import urllib
import pandas as pd
import requests
from tqdm import tqdm
from minio import Minio
from minio.error import S3Error


def convert_ftp_to_https_massive(ftp_url):
    """
    Convert FTP URL to HTTPS URL for MASSIVE.
    """
    if ftp_url.startswith("ftp:/"):
        # ftp://massive-ftp.ucsd.edu/v02/MSV000083475/raw/RAW/PLATE1and2/1A10_1_12_mayer-34-s008-a04.raw
        # turns into
        # https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f.MSV000083475/raw/RAW/PLATE1and2/1A10_1_12_mayer-34-s008-a04.raw&forceDownload=true
        # Remove "ftp://massive-ftp.ucsd.edu" and replace with "https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file="
        https_stub = "https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f."
        # remove everything before MSV
        file_loc = "MSV" + ftp_url.split("MSV")[1]
        # url encode the ftp_path
        file_loc_encoded = urllib.parse.quote(file_loc, safe='')
        https_url = https_stub + file_loc_encoded + "&forceDownload=true"
        return https_url
    else:
        raise ValueError("FTP URL must start with 'ftp://massive-ftp.ucsd.edu'")
    
# Define file paths
mapped_files_path = '/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/mapped_raw_data_files.csv'
processed_files_path = '/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/mapped_raw_data_files_20250812_batch1.csv'
raw_download_folder = '/Volumes/LaCie/nmdc_data/_emp500/raw'
processed_download_folder = '/Volumes/LaCie/nmdc_data/_emp500/processed_20251021'

print("Loading CSV files...")
# Load the CSV files
mapped_df = pd.read_csv(mapped_files_path)
# remove "wget " from the start of the url column if present
mapped_df["url"] = mapped_df["url"].str.replace(r'^wget\s+', '', regex=True)
mapped_df["raw_data_url"] = mapped_df["url"].apply(convert_ftp_to_https_massive)

processed_df = pd.read_csv(processed_files_path)    

print(f"Found {len(mapped_df)} total files in mapped_raw_data_files.csv")
print(f"Found {len(processed_df)} already processed files in batch1")

# Get the list of already processed filenames (use correct column name)
processed_filenames = set(processed_df['raw_data_file_short'])  # or 'file_name' depending on column

# Filter out already processed files
unprocessed_df = mapped_df[~mapped_df['raw_data_file_short'].isin(processed_filenames)]

# Filter out files with missing biosample_ids
unprocessed_df = unprocessed_df.dropna(subset=['biosample_id'])

print(f"Files in batch 2: {len(unprocessed_df)}")

# Create download folders if they don't exist
os.makedirs(raw_download_folder, exist_ok=True)
os.makedirs(processed_download_folder, exist_ok=True)

# Check which files need to be downloaded
files_to_download = []
files_already_exist = []

print("Checking which files already exist locally...")
for _, row in unprocessed_df.iterrows():
    filename = row['raw_data_file_short']
    local_path = os.path.join(raw_download_folder, filename)
    
    if os.path.exists(local_path):
        files_already_exist.append(filename)
    else:
        files_to_download.append(row)

print(f"Files already downloaded: {len(files_already_exist)}")
print(f"Files to download: {len(files_to_download)}")

# Download missing files
if files_to_download:
    print("Starting downloads...")
    
    for row in tqdm(files_to_download, desc="Downloading files"):
        filename = row['raw_data_file_short']
        url = row['raw_data_url']  # or 'url' depending on column name
        local_path = os.path.join(raw_download_folder, filename)
        
        try:
            # Download the file
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Save the file
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"✓ Downloaded: {filename}")
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Error downloading {filename}: {e}")
        except Exception as e:
            print(f"✗ Unexpected error downloading {filename}: {e}")

else:
    print("No files need to be downloaded!")

print("Raw file download process complete!")

# Now download processed files from MinIO
print("\n" + "="*50)
print("DOWNLOADING PROCESSED FILES FROM MINIO")
print("="*50)

try:
    # Initialize MinIO client - requires environment variables MINIO_ACCESS_KEY and MINIO_SECRET_KEY
    if "MINIO_ACCESS_KEY" not in os.environ or "MINIO_SECRET_KEY" not in os.environ:
        raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables must be set")
        
    client = Minio(
        "admin.nmdcdemo.emsl.pnl.gov",
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=True
    )
    
    bucket_name = "metabolomics"
    prefix = "emp500_11_547rwq94_lcms/processed_20251021/"
    
    print(f"Connecting to MinIO bucket '{bucket_name}' with prefix '{prefix}'...")
    
    # List all objects in the specified folder
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    
    processed_files_to_download = []
    processed_files_already_exist = []
    
    print("Checking which processed files need to be downloaded...")
    for obj in objects:
        # Preserve the full folder structure from MinIO
        # Remove the prefix to get the relative path within the processed folder
        relative_path = obj.object_name[len(prefix):] if obj.object_name.startswith(prefix) else obj.object_name
        
        if relative_path:  # Skip if it's empty (root directory)
            local_path = os.path.join(processed_download_folder, relative_path)
            
            if os.path.exists(local_path):
                processed_files_already_exist.append(relative_path)
            else:
                processed_files_to_download.append(obj)
    
    print(f"Processed files already downloaded: {len(processed_files_already_exist)}")
    print(f"Processed files to download: {len(processed_files_to_download)}")
    
    # Download missing processed files
    if processed_files_to_download:
        print("Starting processed file downloads...")
        
        for obj in tqdm(processed_files_to_download, desc="Downloading processed files"):
            # Preserve the full folder structure from MinIO
            relative_path = obj.object_name[len(prefix):] if obj.object_name.startswith(prefix) else obj.object_name
            local_path = os.path.join(processed_download_folder, relative_path)
            
            # Create subdirectories if needed
            local_dir = os.path.dirname(local_path)
            if local_dir:
                os.makedirs(local_dir, exist_ok=True)
            
            try:
                # Download the file from MinIO
                client.fget_object(bucket_name, obj.object_name, local_path)
                print(f"✓ Downloaded: {relative_path}")
                
            except S3Error as e:
                print(f"✗ MinIO error downloading {relative_path}: {e}")
            except Exception as e:
                print(f"✗ Unexpected error downloading {relative_path}: {e}")
    else:
        print("No processed files need to be downloaded!")
        
    print("Processed file download complete!")
    
except Exception as e:
    print(f"Error connecting to MinIO or downloading processed files: {e}")
    if "MINIO_ACCESS_KEY" not in os.environ or "MINIO_SECRET_KEY" not in os.environ:
        print("Make sure MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables are set")
    print("Continuing without processed file downloads...")

# Save the list of files for batch 2 processing
batch2_df = unprocessed_df.copy()
batch2_output_path = '/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/mapped_raw_data_files_batch2.csv'


# Make the following modifications to the batch2_df
#1. Open /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/biosample_processedsample_lookup_emp500batch2.csv , join it to batch2_df on 'biosample_id' to add 'processed_sample_id' column as 'sample_id'.  Remove biosample_id column after the join.
#2. Fix the file paths to the raw and processed files to be absolute paths on /Volumes/LaCie/nmdc_data/...
#3. Add manifest_id column (all = "nmdc:manif-12-y3xvdp79" for batch 2 which matches batch 1)

processed_sample_lookup = pd.read_csv("/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/biosample_processedsample_lookup_emp500batch2.csv")

batch2_df = batch2_df.merge(processed_sample_lookup[['biosample_id', 'last_processed_sample']], on='biosample_id', how='left')
batch2_df = batch2_df.drop(columns=['biosample_id'])
batch2_df = batch2_df.rename(columns={'last_processed_sample': 'sample_id'})

# Fix file paths to be absolute paths on /Volumes/LaCie/nmdc_data/
# Convert existing raw_data_file paths to new location structure
batch2_df['raw_data_file'] = batch2_df['raw_data_file'].str.replace(
    '/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_emp500_lcms/RAW/to_process',
    '/Volumes/LaCie/nmdc_data/_emp500/raw'
)

# Convert existing processed_data_directory paths to new location structure  
batch2_df['processed_data_directory'] = batch2_df['processed_data_directory'].str.replace(
    '/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_emp500_lcms/processed/202507',
    '/Volumes/LaCie/nmdc_data/_emp500/processed_20251021'
)

# Add manifest_id column
batch2_df['manifest_id'] = "nmdc:manif-12-y3xvdp79"
# Add processing_institution_workflow = NMDC and	processing_institution_generation = JGI
batch2_df['processing_institution_workflow'] = "NMDC"
batch2_df['processing_institution_generation'] = "JGI"
batch2_df['instrument_used'] = "Thermo Orbitrap Q-Exactive"

batch2_df.to_csv(batch2_output_path, index=False)
print(f"\nSaved batch 2 file list to: {batch2_output_path}")
print("All downloads complete!")