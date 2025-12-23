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
    
    # Step 2: Fetch raw data (MinIO or MASSIVE based on config)
    print("\n2. Getting FTP URLs from MASSIVE...")
    manager.fetch_raw_data()

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
    
    # Step 4: Inspect raw data files for metadata and QC
    print("\n4. Inspecting raw data files...")
    manager.raw_data_inspector(cores=4)

    # Step 5: Process data (generate WDL configs and execute workflows)
    print("\n5. Processing data with WDL workflows...")
    manager.process_data(execute=True)
    assert manager.should_skip('data_processed'), "WDL workflows must complete successfully to proceed"

    # Step 6: Upload processed data to MinIO
    print("\n6. Uploading processed data to MinIO...")
    manager.upload_processed_data_to_minio()
    assert manager.should_skip('processed_data_uploaded_to_minio'), "Processed data upload to MinIO must complete successfully to proceed"

    # Step 7: Generate and submit NMDC metadata packages
    print("\n7. Generating NMDC metadata packages...")
    manager.generate_nmdc_metadata_for_workflow()
    assert manager.should_skip('metadata_packages_generated'), "NMDC metadata package generation must complete successfully to proceed"

if __name__ == "__main__":
    main()