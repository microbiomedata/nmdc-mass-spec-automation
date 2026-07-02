#!/usr/bin/env python3
"""
MONet - NOM workflow
all FT-ICR, non-MALDI data released on ScienceCentral as of 21 May 2026
Batch 1 of ??? so many files.
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
    """Run the study workflow."""

    # Initialize study manager
    config_path = "workflows/monet_nom/monet_nom_config.json"
    manager = NMDCWorkflowManager(str(config_path))

    logger = manager.logger
    logger.info(f"=== {manager.workflow_name.upper()} WORKFLOW ===")

    # Step 1: Create workflow structure
    logger.info("1. Creating workflow structure...")
    manager.create_workflow_structure()

    # # Step 2: upload local raw data to minio
    # # zip .d files first 
    # logger.info("2. Uploading local raw data to MinIO...")
    # manager.zip_bruker_files(
    #     local_directory=str(manager.raw_data_directory)
    # )
    # manager.upload_to_minio(
    #     local_directory=str(manager.raw_data_directory),
    #     bucket_name=manager.config.get("minio", {}).get("bucket"),
    #     folder_name=str(Path("monet_nom") / "raw"), # set static string here so we don't get a separate folder for each download batch
    #     file_pattern="*.zip"
    # )

    # downloaded_files.csv already exists because you made it manually. to list all the files we needed to generate metadata for. so don't use manager.fetch raw data

    # Step 4: Inspect raw data files for metadata and QC
    logger.info("4. Inspecting raw data files...")
    manager.raw_data_inspector(cores=4)

    # Step 6: Process data (generate WDL configs and execute workflows)
    logger.info("6. Processing data with WDL workflows...")
    manager._generate_mapped_files_list()
    manager.process_data(execute=True)
    assert manager.should_skip('data_processed'), "WDL workflows must complete successfully to proceed"

    # Step 7: Upload processed data to MinIO
    logger.info("7. Uploading processed data to MinIO...")
    manager.upload_processed_data_to_minio()
    assert manager.should_skip('processed_data_uploaded_to_minio'), "Processed data upload to MinIO must complete successfully to proceed"

    # Step 8: Generate and submit NMDC metadata packages
    logger.info("8. Generating NMDC metadata packages...")
    manager.generate_nmdc_metadata_for_workflow() # Set test to FALSE for actual run.
    assert manager.should_skip('metadata_packages_generated'), "NMDC metadata package generation must complete successfully to proceed"

    # # Step 9: Submit metadata packages to dev environment
    # logger.info("9. Submitting metadata packages to dev environment...")
    # dev_success = manager.submit_metadata_packages_to_dev()
    # if not dev_success:
    #     logger.error("Failed to submit metadata packages to dev environment")
    #     logger.error("Please fix the issues and re-run. Skipping production submission.")
    #     return  # Exit without proceeding to prod
    # else:
    #     logger.info("Successfully submitted metadata packages to dev environment")

    # # Step 10: Submit metadata packages to prod environment (will only run if dev submission was successful)
    # logger.info("10. Submitting metadata packages to prod environment...")
    # prod_success = manager.submit_metadata_packages_to_prod()
    # if not prod_success:
    #     logger.error("Failed to submit metadata packages to prod environment")
    # else:
    #     logger.info("Successfully submitted metadata packages to prod environment")

if __name__ == "__main__":
    asyncio.run(main())