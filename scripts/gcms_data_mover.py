"""
# This script will move the processed .csv files from a bunch of subdirectories into a single directory

Inputs will be 
1. path to the directory containing a bunch of subdirectories with the processed .csv files
2. path to the output directory
"""

import os
from pathlib import Path
import shutil

def move_files(input_dir, output_dir):
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Get the list of subdirectories in the input directory
    subdirs = [d for d in Path(input_dir).iterdir() if d.is_dir()]

    # Loop through each subdirectory and move the .csv files to the output directory
    for subdir in subdirs:
        csv_files = list(subdir.glob("*.csv"))
        for csv_file in csv_files:
            # Construct the new file path
            new_file_path = Path(output_dir) / csv_file.name
            # Move the file
            shutil.copy(csv_file, new_file_path)
            print(f"Copied {csv_file} to {new_file_path}")

if __name__ == "__main__":
    # Must be set for the script to run
    input_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_blanchard_metabolomics/processed_20250205/output_files"
    output_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_blanchard_metabolomics/processed_20250205"

    move_files(input_dir, output_dir)
