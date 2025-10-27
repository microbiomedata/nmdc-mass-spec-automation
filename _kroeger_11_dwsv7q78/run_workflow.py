#!/usr/bin/env python3
"""
Kroeger study workflow runner.
Run this from the root directory: /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing

Study: Microbial regulation of soil water repellency to control soil degradation
MASSIVE ID: MSV000094090
"""

import sys
from pathlib import Path

# Add the utils directory to path (assuming run from root directory)
sys.path.append(str(Path.cwd() / "nmdc_dp_utils"))

from study_manager import NMDCStudyManager

def main():
    """Run the Kroeger study workflow."""
    
    # Initialize study manager with the Kroeger config (assuming run from root directory)
    config_path = Path.cwd() / "_kroeger_11_dwsv7q78" / "config.json"
    study = NMDCStudyManager(str(config_path))
    
    print(f"=== {study.study_name.upper()} WORKFLOW ===")
    
    # Step 1: Create study structure
    print("\n1. Creating study structure...")
    study.create_study_structure()
    
    # Step 2: Get FTP URLs from MASSIVE dataset
    print("\n2. Getting FTP URLs from MASSIVE...")
    ftp_df = study.get_massive_ftp_urls()
    assert len(ftp_df) > 0, "No FTP URLs found in MASSIVE dataset."
    
    # Step 3: Download raw data from MASSIVe (this will be skipped if already downloaded)
    print("\n3. Downloading raw data from MASSIVE...")
    downloaded_files = study.download_from_massive()
    print(f"Downloaded {len(downloaded_files)} files")
    
    # Step 4: Generate WDL JSON files for processing
    print("\n4. Generating WDL JSON files...")
    json_count = study.generate_wdl_jsons()
    print(f"Generated {json_count} WDL JSON files")
           
    print("\n=== NEXT STEPS ===")
    print("1. Run WDL workflows using the generated JSON files")
    print("2. Use study.upload_to_minio() to upload processed results")
    print("3. Use study.download_from_minio() to download results elsewhere")
    #TODO: these next steps don't exist yet
    print("4. Use study.raw_data_inspector() to review raw data files to get configurations and parameters correct as well as start and end times for metadata")
    print("5. Make a mapping file that links raw and processed data to NMDC biosample IDs and other metadata")
    print("6. Use study.generate_nmdc_submission() to create NMDC submission packages for processed data")
    print(f"\nStudy directory: {study.study_path}")

if __name__ == "__main__":
    main()