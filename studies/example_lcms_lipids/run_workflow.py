#!/usr/bin/env python3
"""
Example Lipid study, lcms_lipidomics workflow runner.
"""

import sys
from pathlib import Path

# Add the utils directory to path (assuming run from root directory)
sys.path.append(str(Path.cwd() / "nmdc_dp_utils"))

from workflow_manager import NMDCWorkflowManager

def main():
    """Run the Example Lipid study workflow."""
    
    # Initialize study manager
    config_path = "studies/example_lcms_lipids/example_config_lcms_lipids.json"
    manager = NMDCWorkflowManager(str(config_path))
    
    print(f"=== {manager.workflow_name.upper()} WORKFLOW ===")
    
    # Step 1: Create workflow structure
    print("\n1. Creating workflow structure...")
    manager.create_workflow_structure()
    
    # Step 2: Get Raw Data from MASSIVE
    print("\n2. Getting FTP URLs from MASSIVE...")
    _ = manager.get_massive_ftp_urls()
    _ = manager.download_from_massive()

    # Step 3: Map raw data files to biosamples by generating mapping script and running it
    print("\n3. Mapping raw data files to biosamples...")
    manager.get_biosample_attributes()
    manager.generate_biosample_mapping_script()
    
    mapping_success = manager.run_biosample_mapping_script()
    if not mapping_success:
        print("⚠️  Biosample mapping needs manual review - check the mapping file and customize the script")
        print("   Re-run after making changes to improve matching")
    else:
        print("✅ Biosample mapping completed successfully")
    
    # Step 5: Inspect raw data files for metadata and QC
    print("\n5. Inspecting raw data files...")
    manager.raw_data_inspector(cores=4)

    # Step 7: Generate WDL JSON files for processing, make runner script, and process them
    print("\n7. Generating WDL JSON files, runner script, and running workflow...")
    manager.generate_wdl_jsons()
    manager.generate_wdl_runner_script()
    manager.run_wdl_script()
    assert manager.should_skip('data_processed'), "WDL workflows must complete successfully to proceed"

    # Step 8: Upload processed data to MinIO
    print("\n8. Uploading processed data to MinIO...")
    _ = manager.upload_processed_data_to_minio()
    assert manager.should_skip('processed_data_uploaded_to_minio'), "Processed data upload to MinIO must complete successfully to proceed"

    # Step 6: Generate metadata mapping files with URL validation
    print("\n6. Generating metadata mapping files with URL validation...")
    metadata_success = manager.generate_workflow_metadata_generation_inputs()
    assert manager.should_skip('metadata_mapping_generated'), "Metadata mapping generation must complete successfully to proceed"
    assert metadata_success, "Metadata mapping generation failed."

    # Step 9: Generate NMDC submission packages
    print("\n9. Generating NMDC submission packages for workflow metadata...")
    _ = manager.generate_nmdc_metadata_for_workflow()
    assert manager.should_skip('nmdc_metadata_generated'), "NMDC metadata package generation must complete successfully to proceed"

    # Step 10: Submit to NMDC development environment
    print("\n10. Submitting to NMDC development environment...")
    dev_success = manager.submit_metadata_packages(environment='dev')
    if dev_success:
        print("✅ Submitted to NMDC development successfully")
    
    # Step 11: Submit to NMDC production environment
    print("\n11. Submitting to NMDC production environment...")
    prod_success = manager.submit_metadata_packages(environment='prod')
    if prod_success:
        print("✅ Submitted to NMDC production successfully")

if __name__ == "__main__":
    main()