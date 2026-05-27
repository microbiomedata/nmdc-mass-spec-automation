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

    # Step 2: Upload local raw data to MinIO, zipping any .d files first 
    logger.info("2. Uploading local raw data to MinIO...")
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

    # Step 4: Inspect raw data files for metadata and QC
    logger.info("4. Inspecting raw data files...")
    manager.raw_data_inspector(cores=4)

    # Step 5: Process data (generate WDL configs and execute workflows)
    logger.info("5. Processing data with WDL workflows...")
    manager.process_data(execute=True)
    assert manager.should_skip('data_processed'), "WDL workflows must complete successfully to proceed"

    # debugging file transfer
    # manager._move_processed_files(working_dir="/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/miesel_11_8bjf2432_nom/wdl_execution", clean_up=False)

    # Step 6: Upload processed data to MinIO
    logger.info("6. Uploading processed data to MinIO...")
    manager.upload_processed_data_to_minio()
    assert manager.should_skip('processed_data_uploaded_to_minio'), "Processed data upload to MinIO must complete successfully to proceed"

if __name__ == "__main__":
    asyncio.run(main())