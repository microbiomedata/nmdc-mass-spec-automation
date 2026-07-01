#!/usr/bin/env python3
"""
Download data files from a DMS data package manifest.

This script reads an Excel file containing dataset information from a DMS query
and copies the raw data files to a local directory structure.

Edited to run on WSL2 on Windows, requires protoapps share drive(s) to be mounted in ubuntu

Usage:
    python batch_and_download.py <manifest_file> --batch-num <batch_number> --output-dir <output_directory>

"""

import sys
import argparse
import shutil
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup

def convert_protoapps_url_to_filepath(url: str) -> Path:
    # check that this url points to the protoapps drive you have mounted
    if not isinstance(url, str) or not url.startswith("https://proto-9.pnl.gov/SciMax01/"):
        print(f"Skipping row with invalid URL: {url}")
        return None
    relative_path = url[len("https://proto-9.pnl.gov/SciMax01/") :]
    file_path = Path("/mnt/proto-9_SciMax01") / relative_path
    dataset_name = file_path.name
    dataset_dir = file_path / (dataset_name + ".d")
    if not dataset_dir.is_dir():
        print(f"Expected dataset directory not found: {dataset_dir}")
        return None
    return dataset_dir

def check_and_download_srfa(manifest_df: pd.DataFrame, srfa_dir: Path, output_dir: Path):
    """
    Check if the SRFA dataset is present in the srfa directory and download it if necessary.
    
    Parameters
    ----------
    manifest_df : pd.DataFrame
        DataFrame containing the manifest information
    srfa_dir : Path
        Path to the directory containing the SRFA dataset
    output_dir : Path
        Output directory where the SRFA dataset should be copied
    """
    # filter to rows with "SRFA" in the dataset name
    df = manifest_df[manifest_df["dataset"].str.contains("SRFA", case=False, na=False)]

    # for each row in the filtered dataframe, check if the dataset exists in the srfa_dir
    for idx, row in df.iterrows():
        dataset_name = row['dataset']
        dataset_url = row['data_folder_path']
        source_path = convert_protoapps_url_to_filepath(dataset_url)
        srfa_path_local = srfa_dir / (dataset_name + ".d")

        if not srfa_path_local.exists() or not any(srfa_path_local.iterdir()):
            # download the dataset from the source path to the srfa_dir
            print(f"Downloading {dataset_name} from {source_path} to {srfa_path_local}...")
            try:
                shutil.copytree(source_path, srfa_path_local)
                print(f"Downloaded {dataset_name} successfully")
            except Exception as e:  
                print(f"ERROR: {e}")
                continue

    # copy all files in srfa dir to output dir, skipping if destination directory exists and is not empty
    for srfa_dataset in srfa_dir.iterdir():
        dest_dir = output_dir / srfa_dataset.name
        if dest_dir.exists() and any(dest_dir.iterdir()):
            print(f"SRFA dataset {srfa_dataset.name} already exists in output dir, skipping")
            continue
        try:
            shutil.copytree(srfa_dataset, dest_dir)
            print(f"Copied {srfa_dataset.name} to output dir successfully")
        except Exception as e:
            print(f"ERROR: {e}")
            continue


def download_datasets(manifest_path: Path, output_dir: Path, batch_number: int):
    """
    Download all data files from an excel file of DMS dataset info 
    that you filtered and marked up based on comments etc.
    
    Parameters
    ----------
    manifest_path : Path
        Path to the manifest file
    output_dir : Path, optional
        Output directory. If None, uses MONET_DATA_PATH environment variable
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Read in the "working" tab of the provided Excel file as a Pandas dataframe
    df = pd.read_excel(manifest_path, sheet_name="working")

    check_and_download_srfa(df, Path("/home/bmeluch/NMDC/nom_processing/monet_nom_srfa/raw"), output_dir)

    # Filter to download_batch == batch number and not datasets containing "SRFA" in the dataset name
    df = df[~df["dataset"].str.contains("SRFA", case=False, na=False)]
    df = df[df["download_batch"] == batch_number]

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}")
    
    # Process each dataset
    print(f"\nFound {len(df)} datasets in manifest")
    successful = 0
    failed = 0
    
    for idx, row in df.iterrows():
        dataset_name = row['dataset']
        dataset_url = row['data_folder_path']
        
        # Construct full source path
        source_path = convert_protoapps_url_to_filepath(dataset_url)
        if source_path is None:
            failed += 1
            continue
        
        # Construct destination path
        dest_dir = output_dir / (dataset_name + ".d")

        # Copy directory
        print(f"Copying {source_path} to {dest_dir}...")        
        if not source_path.exists():
            print(f"ERROR: Source directory not found")
            failed += 1
            continue
        
        if dest_dir.exists():
            print(f"Directory already exists, skipping")
            successful += 1
            continue
        
        try:
            shutil.copytree(source_path, dest_dir)
            print(f"Copied successfully")
            successful += 1
        except Exception as e:
            print(f"ERROR: {e}")
            failed += 1
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Download complete!")
    print(f"  Successful: {successful}/{len(df)}")
    print(f"  Failed:     {failed}/{len(df)}")
    print(f"  Output:     {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description='Download data files from a manually batched DMS data package manifest',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        'manifest',
        type=Path,
        help='Path to the manifest file (Excel)'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        help='Path to raw data directory'
    )
    parser.add_argument(
        '--batch-num',
        type=int,
        help='Download batch number to filter on in the manifest file (column "download_batch")'
    )
    
    args = parser.parse_args()
    
    if not args.manifest.exists():
        print(f"Error: Manifest file not found: {args.manifest}", file=sys.stderr)
        sys.exit(1)
    
    download_datasets(args.manifest, args.output_dir, args.batch_num)


if __name__ == '__main__':
    main()