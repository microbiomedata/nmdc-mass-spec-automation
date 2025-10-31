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

    # Step 3: Download a subset of raw data from MASSIVE based on configured file patterns
    print("\n3. Downloading raw data from MASSIVE...")
    downloaded_files = study.download_from_massive()
    print(f"Available files: {len(downloaded_files)}")

    # Step 4: Map raw data files to biosamples by generating mapping script and running it
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
    
    # Step 5: Inspect raw data files for metadata and QC
    print("\n5. Inspecting raw data files...")
    inspection_result = study.raw_data_inspector(
        cores=4,    # Use multiple cores for faster processing
        limit=None  # Process all files
        
    )
    if inspection_result:
        print(f"✅ Raw data inspection completed: {inspection_result}")
    else:
        print("⚠️  Raw data inspection completed but no results returned")

    # Step 6: Generate metadata mapping files with URL validation
    print("\n6. Generating metadata mapping files with URL validation...")
    metadata_success = study.generate_metadata_mapping_files()
    if metadata_success:
        print("✅ Metadata mapping files generated and URLs validated successfully")
    else:
        print("⚠️  Metadata mapping generation needs review")

    # Step 7: Generate WDL JSON files for processing
    print("\n7. Generating WDL JSON files...")
    json_count = study.generate_wdl_jsons()
    print(f"WDL JSON files: {json_count}")

    # Step 8: Generate WDL runner script
    print("\n8. Generating WDL runner script...")
    script_path = study.generate_wdl_runner_script()
    print(f"Generated script: {script_path}")

    # Step 9: Run WDL workflows
    print("\n9. Running WDL workflows...")
    study.run_wdl_script(script_path)
    assert study.should_skip('data_processed'), "WDL workflows must complete successfully to proceed"

    # Step 10: Upload processed data to MinIO
    print("\n10. Uploading processed data to MinIO...")
    #study.upload_to_minio()



    # Step 11: Generate NMDC submission packages
    print("\n11. Generating NMDC submission packages...")
    packages_success = study.generate_nmdc_submission_packages()
    if packages_success:
        print("✅ NMDC submission packages generated successfully")
    else:
        print("⚠️  Package generation needs review")

    # Step 12: Submit to NMDC development environment
    print("\n12. Submitting to NMDC development environment...")
    dev_success = study.submit_metadata_packages(environment='dev')
    if dev_success:
        print("✅ Submitted to NMDC development successfully")
    
    # Step 13: Submit to NMDC production environment
    print("\n13. Submitting to NMDC production environment...")
    prod_success = study.submit_metadata_packages(environment='prod')
    if prod_success:
        print("✅ Submitted to NMDC production successfully")


           
    print("\n=== WORKFLOW COMPLETE ===")
    print("✅ All workflow steps completed!")
    print("📁 Metadata mapping files: metadata/metadata_gen_input_csvs/")
    print("📦 NMDC submission packages: metadata/nmdc_submission_packages/")
    print("🚀 Metadata submitted to NMDC dev and prod environments")
    
    print(f"\nStudy directory: {study.study_path}")
    print(f"WDL runner script: {script_path}")
    print(f"Current skip triggers: {study.config.get('skip_triggers', {})}")
    print("\nTo reset workflow and start over, set skip triggers to false in config.json")

if __name__ == "__main__":
    main()