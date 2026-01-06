#!/usr/bin/env python3
"""
Kroeger study, lcms_metabolomics workflow runner.
Study: Microbial regulation of soil water repellency to control soil degradation
MASSIVE ID: MSV000094090
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path so package `nmdc_dp_utils` is importable
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager

def main():
    """Run the Kroeger study workflow."""

    # Initialize study manager
    config_path = "studies/kroeger_11_dwsv7q78_lcms_metab/kroeger_lcms_metab_config.json"
    manager = NMDCWorkflowManager(str(config_path))

    logger = manager.logger
    logger.info(f"=== {manager.workflow_name.upper()} WORKFLOW ===")

    # Step 1: Create workflow structure
    logger.info("1. Creating workflow structure...")
    manager.create_workflow_structure()

    # Step 2: Fetch raw data (MinIO or MASSIVE based on config)
    logger.info("2. Fetching raw data...")
    manager.fetch_raw_data()

    # Step 3: Map raw data files to biosamples by generating mapping script and running it
    logger.info("3. Mapping raw data files to biosamples...")
    manager.get_biosample_attributes()
    manager.generate_biosample_mapping_script()

    mapping_success = manager.run_biosample_mapping_script()
    if not mapping_success:
        logger.warning("Biosample mapping needs manual review - check the mapping file and customize the script")
        logger.warning("Re-run after making changes to improve matching")
    else:
        logger.info("Biosample mapping completed successfully")

    # Step 4: Inspect raw data files for metadata and QC
    logger.info("4. Inspecting raw data files...")
    manager.raw_data_inspector(cores=4)

    # Step 5: Process data (generate WDL configs and execute workflows)
    logger.info("5. Processing data with WDL workflows...")
    manager.process_data(execute=True)
    assert manager.should_skip('data_processed'), "WDL workflows must complete successfully to proceed"

    # Step 6: Upload processed data to MinIO
    logger.info("6. Uploading processed data to MinIO...")
    manager.upload_processed_data_to_minio()
    assert manager.should_skip('processed_data_uploaded_to_minio'), "Processed data upload to MinIO must complete successfully to proceed"

    # Step 7: Generate and submit NMDC metadata packages
    logger.info("7. Generating NMDC metadata packages...")
    manager.generate_nmdc_metadata_for_workflow(test=True)
    assert manager.should_skip('metadata_packages_generated'), "NMDC metadata package generation must complete successfully to proceed"

if __name__ == "__main__":
    main()