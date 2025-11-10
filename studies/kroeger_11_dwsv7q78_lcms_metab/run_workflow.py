#!/usr/bin/env python3
"""
Kroeger study, lcms_metabolomics workflow runner.
Study: Microbial regulation of soil water repellency to control soil degradation
MASSIVE ID: MSV000094090
"""

import sys
from pathlib import Path

# Add the utils directory to path (assuming run from root directory)
sys.path.append(str(Path.cwd() / "nmdc_dp_utils"))

from workflow_manager import NMDCWorkflowManager

def main():
    """Run the Kroeger study workflow."""
    
    # Initialize study manager with the Kroeger config (assuming run from root directory)
    config_path = Path.cwd() / "studies" / "kroeger_11_dwsv7q78_lcms_metab" / "kroeger_lcms_metab_config.json"
    manager = NMDCWorkflowManager(str(config_path))
    
    print(f"=== {manager.workflow_name.upper()} WORKFLOW ===")
    
    # Step 1: Create workflow structure
    print("\n1. Creating workflow structure...")
    manager.create_workflow_structure()
    
    # Step 2: Get FTP URLs from MASSIVE dataset
    print("\n2. Getting FTP URLs from MASSIVE...")
    ftp_df = manager.get_massive_ftp_urls()
    if not manager.should_skip('raw_data_downloaded'):
        assert len(ftp_df) > 0, "No FTP URLs found in MASSIVE dataset."

    # Step 3: Download a subset of raw data from MASSIVE based on configured file patterns
    print("\n3. Downloading raw data from MASSIVE...")
    downloaded_files = manager.download_from_massive()
    print(f"Available files: {len(downloaded_files)}")

    # Step 4: Map raw data files to biosamples by generating mapping script and running it
    print("\n4. Mapping raw data files to biosamples...")
    biosample_csv = manager.get_biosample_attributes()
    print(f"Biosample attributes saved to: {biosample_csv}")
    
    mapping_script = manager.generate_biosample_mapping_script()
    print(f"Mapping script generated: {mapping_script}")
    
    mapping_success = manager.run_biosample_mapping_script()
    if not mapping_success:
        print("⚠️  Biosample mapping needs manual review - check the mapping file and customize the script")
        print("   Re-run after making changes to improve matching")
    else:
        print("✅ Biosample mapping completed successfully")
    
    # Step 5: Inspect raw data files for metadata and QC
    print("\n5. Inspecting raw data files...")
    inspection_result = manager.raw_data_inspector(
        cores=4,    # Use multiple cores for faster processing
        limit=None  # Process all files  
    )
    assert manager.should_skip('raw_data_inspected'), "Raw data inspection must complete successfully to proceed"
    assert inspection_result, "Raw data inspection did not return any results."

    # Step 6: Generate metadata mapping files with URL validation
    print("\n6. Generating metadata mapping files with URL validation...")
    metadata_success = manager.generate_metadata_mapping_files()
    assert manager.should_skip('metadata_mapping_generated'), "Metadata mapping generation must complete successfully to proceed"
    assert metadata_success, "Metadata mapping generation failed."

    # Step 7: Generate WDL JSON files for processing, make runner script, and process them
    print("\n7. Generating WDL JSON files...")
    json_count = manager.generate_wdl_jsons()
    print(f"WDL JSON files: {json_count}")
    print("\n...Generating WDL runner script...")
    script_path = manager.generate_wdl_runner_script()
    print(f"Generated script: {script_path}")

    # Step 8: Run WDL workflows
    print("\n8. Running WDL workflows...")
    manager.run_wdl_script(script_path)
    assert manager.should_skip('data_processed'), "WDL workflows must complete successfully to proceed"

    # Step 9: Upload processed data to MinIO
    print("\n9. Uploading processed data to MinIO...")
    upload_success = manager.upload_processed_data_to_minio()
    if upload_success:
        print("✅ Processed data uploaded to MinIO successfully")
    else:
        print("⚠️  MinIO upload failed or was skipped")
    assert manager.should_skip('processed_data_uploaded_to_minio'), "Processed data upload to MinIO must complete successfully to proceed"

    # Step 10: Generate NMDC submission packages
    print("\n10. Generating NMDC submission packages for workflow metadata...")
    packages_success = manager.generate_nmdc_metadata_for_workflow()
    if packages_success:
        print("✅ NMDC submission packages generated successfully")
    else:
        print("⚠️  Package generation needs review")

    # Step 11: Submit to NMDC development environment
    print("\n11. Submitting to NMDC development environment...")
    dev_success = manager.submit_metadata_packages(environment='dev')
    if dev_success:
        print("✅ Submitted to NMDC development successfully")
    
    # Step 12: Submit to NMDC production environment
    print("\n12. Submitting to NMDC production environment...")
    prod_success = manager.submit_metadata_packages(environment='prod')
    if prod_success:
        print("✅ Submitted to NMDC production successfully")

if __name__ == "__main__":
    main()