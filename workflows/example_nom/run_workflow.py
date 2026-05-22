#!/usr/bin/env python3
"""
Miesel study, nom workflow runner.
EMSL 60009
"""

import sys
from pathlib import Path
import asyncio

# Ensure project root is on sys.path so package `nmdc_dp_utils` is importable
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager

async def main():
    """Run the Miesel study workflow."""

    # Initialize study manager
    config_path = "workflows/miesel_11_8bjf2432_nom/miesel_nom_config.json"
    manager = NMDCWorkflowManager(str(config_path))

    logger = manager.logger
    logger.info(f"=== {manager.workflow_name.upper()} WORKFLOW ===")

    # Step 1: Create workflow structure
    logger.info("1. Creating workflow structure...")
    manager.create_workflow_structure()

    # Step 2: Generate protocol YAML outline using LLM
    logger.info("2. Generating protocol YAML outline using LLM...")
    await manager.generate_material_processing_yaml()

    # upload local raw data to minio
    # zip .d files first 
    logger.info("Uploading local raw data to MinIO...")
    manager.zip_bruker_files(
        local_directory=str(manager.raw_data_directory)
    )
    manager.upload_to_minio(
        local_directory=str(manager.raw_data_directory),
        bucket_name=manager.config.get("minio", {}).get("bucket"),
        folder_name=str(Path(manager.study_name) / "raw"),
        file_pattern="*.zip"
    )

    # Step 3: Fetch raw data from minio
    # leave this in because it skips files that already exist and writes out a file list for mapping
    logger.info("3. Fetching raw data...")
    manager.fetch_raw_data()

    # Step 4: Map raw data files to biosamples
    logger.info("4. Mapping raw data files to biosamples using LLM...")
    manager.get_biosample_attributes()
    mapping_success = await manager.generate_llm_biosample_mapping()
    
    if not mapping_success:
        logger.warning("Biosample mapping failed - review logs and add additional context if needed")
    else:
        logger.info("Biosample mapping completed successfully")

    # Step 5: Inspect raw data files for metadata and QC
    logger.info("5. Inspecting raw data files...")
    manager.raw_data_inspector(cores=4)

    # Step 6: Process data (generate WDL configs and execute workflows)
    logger.info("6. Processing data with WDL workflows...")
    manager.process_data(execute=True)
    assert manager.should_skip('data_processed'), "WDL workflows must complete successfully to proceed"

    # debugging file transfer
    manager._move_processed_files(working_dir="/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/miesel_11_8bjf2432_nom/wdl_execution", clean_up=False)

    # Step 7: Upload processed data to MinIO
    logger.info("7. Uploading processed data to MinIO...")
    manager.upload_processed_data_to_minio()
    assert manager.should_skip('processed_data_uploaded_to_minio'), "Processed data upload to MinIO must complete successfully to proceed"

    # Step 8: Generate and submit NMDC metadata packages
    logger.info("8. Generating NMDC metadata packages...")
    manager.generate_nmdc_metadata_for_workflow() # Set test to FALSE for actual run.
    assert manager.should_skip('metadata_packages_generated'), "NMDC metadata package generation must complete successfully to proceed"

    # Step 9: Submit metadata packages to dev environment
    logger.info("9. Submitting metadata packages to dev environment...")
    dev_success = manager.submit_metadata_packages_to_dev()
    if not dev_success:
        logger.error("Failed to submit metadata packages to dev environment")
        logger.error("Please fix the issues and re-run. Skipping production submission.")
        return  # Exit without proceeding to prod
    else:
        logger.info("Successfully submitted metadata packages to dev environment")

    # Step 10: Submit metadata packages to prod environment (will only run if dev submission was successful)
    logger.info("10. Submitting metadata packages to prod environment...")
    prod_success = manager.submit_metadata_packages_to_prod()
    if not prod_success:
        logger.error("Failed to submit metadata packages to prod environment")
    else:
        logger.info("Successfully submitted metadata packages to prod environment")

if __name__ == "__main__":
    asyncio.run(main())