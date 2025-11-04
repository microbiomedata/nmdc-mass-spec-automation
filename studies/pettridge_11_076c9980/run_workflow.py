#!/usr/bin/env python3
"""
Singer study workflow runner.
Run this from the root directory: /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing
"""

import sys
from pathlib import Path

# Add the utils directory to path (assuming run from root directory)
sys.path.append(str(Path.cwd() / "nmdc_dp_utils"))

from study_manager import NMDCStudyManager

def main():
    """Run the study workflow."""

    # Initialize study manager with the Pettridge config (assuming run from root directory)
    config_path = Path.cwd() / "studies" / "pettridge_11_076c9980" / "pettridge_config.json"
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

    # Step 3: Download a subset of raw data from MASSIVE based on configured file patterns
    print("\n3. Downloading raw data from MASSIVE...")
    downloaded_files = study.download_from_massive()
    print(f"Available files: {len(downloaded_files)}")

    # Step 4: Map raw data files to biosamples by generating a template mapping script, modifying it accordingly, and running it
    print("\n4. Mapping raw data files to biosamples...")
    biosample_csv = study.get_biosample_attributes()
    print(f"Biosample attributes saved to: {biosample_csv}")
    
    mapping_script = study.generate_biosample_mapping_script()
    print(f"Mapping script generated: {mapping_script}")
    
    mapping_success = study.run_biosample_mapping_script()
    if not mapping_success:
        assert False, "Biosample mapping failed."
    else:
        print("✅ Biosample mapping completed successfully")
    
    # Step 5: Inspect raw data files for metadata and QC
    print("\n5. Inspecting raw data files...")
    _ = study.raw_data_inspector(
        cores=1,    # Single core for .raw files to prevent Docker crashes
        limit=None  # Process all files  
    )
    assert study.should_skip('raw_data_inspected'), "Raw data inspection must complete successfully to proceed"

    # Step 6: Generate metadata mapping files with URL validation
    print("\n6. Generating metadata mapping files with URL validation...")
    metadata_success = study.generate_metadata_mapping_files()
    assert study.should_skip('metadata_mapping_generated'), "Metadata mapping generation must complete successfully to proceed"
    assert metadata_success, "Metadata mapping generation failed."

    # Step 7: Generate WDL JSON files for processing, make runner script, and process them
    print("\n7. Generating WDL JSON files...")
    json_count = study.generate_wdl_jsons()
    print(f"WDL JSON files: {json_count}")
    print("\n...Generating WDL runner script...")
    script_path = study.generate_wdl_runner_script()
    print(f"Generated script: {script_path}")

    # Step 8: Run WDL workflows
    print("\n8. Running WDL workflows...")
    study.run_wdl_script(script_path)
    assert study.should_skip('data_processed'), "WDL workflows must complete successfully to proceed"

    # Step 9: Upload processed data to MinIO
    print("\n9. Uploading processed data to MinIO...")
    upload_success = study.upload_processed_data_to_minio()
    if upload_success:
        print("✅ Processed data uploaded to MinIO successfully")
    else:
        print("⚠️  MinIO upload failed or was skipped")
    assert study.should_skip('processed_data_uploaded_to_minio'), "Processed data upload to MinIO must complete successfully to proceed"

if __name__ == "__main__":
    main()
