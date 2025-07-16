"""
This script will read in the file _bioscales_lcms_metabolomics/bioscales_massive_ftp_locs.txt
and pull the ftp locations from that txt file. Then it will make a dataframe with the ftp locations and the file names (shortened to the root name).
Then it will download all the rhizome files from the ftp locations and save them to /Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_bioscales_lcms/to_process using wget
"""

import pandas as pd
import os
import subprocess
import tqdm

def pull_ftp_locations(file_path):
    """
    Reads the file containing FTP locations and returns a DataFrame with file names and their corresponding FTP URLs.
    """
    ftp_locs = []
    ftp_file = file_path
    with open(ftp_file, "r") as f:
        for line in f:
            if line.endswith(".mzML\n"):
                ftp_locs.append(line.strip())
    ftp_locs = [loc.split(" ")[-1] for loc in ftp_locs]
    ftp_locs_df = pd.DataFrame(ftp_locs, columns=["ftp_location"])
    ftp_locs_df.drop_duplicates(inplace=True)
    ftp_locs_df["raw_data_file_short"] = ftp_locs_df["ftp_location"].str.extract(r'([^/]+\.mzML)$')[0]
    
    return ftp_locs_df

def download_file(ftp_location, download_path):
    """
    Downloads a file from the given FTP location to the specified download path using wget.
    """
    subprocess.run(['wget', '-q', '-O', download_path, ftp_location], check=True)

if __name__ == "__main__":
    # Define the path to the file containing FTP locations
    ftp_file_path = '_bioscales_lcms_metabolomics/bioscales_massive_ftp_locs.txt'
    
    # Pull FTP locations and create DataFrame
    ftp_df = pull_ftp_locations(ftp_file_path)

    # Drop rows without "rhizo" in the file name
    ftp_df = ftp_df[ftp_df['raw_data_file_short'].str.contains('rhizo', na=False)]

    # Write the DataFrame to a CSV file for reference as "bioscales_ftp_locs.csv"
    ftp_df.to_csv("_bioscales_lcms_metabolomics/bioscales_ftp_locs.csv", index=False)

    # Define the directory to save downloaded files
    download_dir = '/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_bioscales_lcms/to_process'
    
    # Ensure the download directory exists
    os.makedirs(download_dir, exist_ok=True)
    
    # Download each file using wget
    print(f"Starting download of {len(ftp_df)} files...")
    for index, row in tqdm.tqdm(ftp_df.iterrows(), total=len(ftp_df), desc="Downloading files"):
        # Check if the file already exists
        if os.path.exists(os.path.join(download_dir, row['raw_data_file_short'])):
            print(f"File {row['raw_data_file_short']} already exists. Skipping download.")
            continue
        ftp_location = row['ftp_location']
        file_name = row['raw_data_file_short']
        download_path = os.path.join(download_dir, file_name)

        # Use wget to download the file
        download_file(ftp_location, download_path)

    print("All files downloaded successfully.")