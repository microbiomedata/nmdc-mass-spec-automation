"""
This script will look at /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/mapped_raw_data_files.csv and remove files that have already been processed (using _emp_500_lcms_metabolomics/mapped_raw_data_files_20250812_batch1.csv as a reference).

It will look into the folder /Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_emp500_lcms/RAW/to_process to see if the files are there, and if not it will download them (using the url column in mapped_raw_data_files.csv).

"""
import os
import urllib
import pandas as pd
import requests
from tqdm import tqdm


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
download_folder = '/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_emp500_lcms/RAW/to_process'

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

print(f"Files remaining to process: {len(unprocessed_df)}")

# Create download folder if it doesn't exist
os.makedirs(download_folder, exist_ok=True)

# Check which files need to be downloaded
files_to_download = []
files_already_exist = []

print("Checking which files already exist locally...")
for _, row in unprocessed_df.iterrows():
    filename = row['raw_data_file_short']
    local_path = os.path.join(download_folder, filename)
    
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
        local_path = os.path.join(download_folder, filename)
        
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

print("Download process complete!")

# Save the list of files for batch 2 processing
batch2_df = unprocessed_df.copy()
batch2_output_path = '/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/mapped_raw_data_files_batch2.csv'
batch2_df.to_csv(batch2_output_path, index=False)
print(f"Saved batch 2 file list to: {batch2_output_path}")