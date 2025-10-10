import pandas as pd
import requests
import os
from tqdm import tqdm

def download_csvs(csv_file_path, download_folder="downloads"):
    """
    Read the CSV file containing LC-MS URLs and download them as CSV files.
    """
    
    # Read the CSV file
    try:
        urls_df = pd.read_csv(csv_file_path)
        print(f"Found {len(urls_df)} CSV files to download")
    except FileNotFoundError:
        print(f"CSV file {csv_file_path} not found.")
        return {}
    
    # Create download folder if it doesn't exist
    os.makedirs(download_folder, exist_ok=True)
    
    # Dictionary to store downloaded dataframes
    dataframes = {}
    
    # Download each CSV file
    for index, row in tqdm(urls_df.iterrows(), total=len(urls_df), desc="Downloading CSV files"):
        url = row['url']
        file_id = row['id']
        
        # Create filename from ID
        filename = f"{file_id}.csv"
        local_path = os.path.join(download_folder, filename)
        
        try:
            # Download the CSV file
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Read into dataframe
            df = pd.read_csv(url)
            dataframes[file_id] = df
            
            # Save locally
            df.to_csv(local_path, index=False)
            
        except Exception as e:
            print(f"Error downloading {file_id}: {e}")
    
    print(f"Successfully downloaded {len(dataframes)} CSV files to {download_folder}/")
    return dataframes

if __name__ == "__main__":
    # File path
    csv_file = "emp_500_lcms_metab_result_urls.csv"
    
    # Download CSV files
    dataframes = download_csvs(csv_file)
    
    # Show summary
    if dataframes:
        print(f"\nLoaded {len(dataframes)} dataframes:")