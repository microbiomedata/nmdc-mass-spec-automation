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
    print(f"Skip triggers: {study.config.get('skip_triggers', {})}")
    
    # Step 1: Create study structure
    print("\n1. Creating study structure...")
    study.create_study_structure()
    
    # Step 2: Get FTP URLs from MASSIVE dataset
    print("\n2. Getting FTP URLs from MASSIVE...")
    ftp_df = study.get_massive_ftp_urls()
    if not study.should_skip('raw_data_downloaded'):
        assert len(ftp_df) > 0, "No FTP URLs found in MASSIVE dataset."
    
    # Step 3: Download raw data from MASSIVe
    print("\n3. Downloading raw data from MASSIVE...")
    downloaded_files = study.download_from_massive()
    print(f"Available files: {len(downloaded_files)}")

    # Step 4: Map raw data files to biosamples
    print("\n4. Mapping raw data files to biosamples...")
    biosample_csv = study.get_biosample_attributes()
    print(f"Biosample attributes saved to: {biosample_csv}")
    
    mapping_script = study.generate_biosample_mapping_script()
    print(f"Mapping script generated: {mapping_script}")
    
    mapping_success = study.run_biosample_mapping_script()
    if not mapping_success:
        print("⚠️  Biosample mapping needs manual review - check the mapping file and customize the script")
        print("   Re-run after making changes to improve matching")
    else:
        print("✅ Biosample mapping completed successfully")
    
    # Step 5: Generate WDL JSON files for processing
    print("\n5. Generating WDL JSON files...")
    json_count = study.generate_wdl_jsons()
    print(f"WDL JSON files: {json_count}")
    
    # Step 6: Generate WDL runner script
    print("\n6. Generating WDL runner script...")
    script_path = study.generate_wdl_runner_script()
    print(f"Generated script: {script_path}")
    
    # Step 7: Run WDL workflows
    print("\n7. Running WDL workflows...")
    study.run_wdl_script(script_path)
    assert study.should_skip('data_processed'), "WDL workflows must complete successfully to proceed"
           
    print("\n=== NEXT STEPS ===")
    print("1. Use study.upload_to_minio() to upload processed results")
    print("2. Use study.download_from_minio() to download results elsewhere")
    #TODO: these next steps don't exist yet
    print("3. Use study.raw_data_inspector() to review raw data files to get configurations and parameters correct as well as start and end times for metadata")
    print("4. Make a mapping file that links raw and processed data to NMDC biosample IDs and other necessary input to metadata generation")
    print("5. Use study.generate_nmdc_submission() to create NMDC submission packages for processed data")
    
    print(f"\nStudy directory: {study.study_path}")
    print(f"WDL runner script: {script_path}")
    print(f"Current skip triggers: {study.config.get('skip_triggers', {})}")
    print("\nTo reset workflow and start over, set skip triggers to false in config.json")

if __name__ == "__main__":
    main()