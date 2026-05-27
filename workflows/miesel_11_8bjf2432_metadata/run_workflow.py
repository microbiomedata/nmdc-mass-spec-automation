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
    config_path = "workflows/miesel_11_8bjf2432_metadata/miesel_metadata_config.json"
    manager = NMDCWorkflowManager(str(config_path))

    logger = manager.logger
    logger.info(f"=== {manager.workflow_name.upper()} WORKFLOW ===")

    # Step 1: Create workflow structure
    logger.info("1. Creating workflow structure...")
    manager.create_workflow_structure()

    # Step 2: Generate protocol YAML outline using LLM
    logger.info("2. Generating protocol YAML outline using LLM...")
    await manager.generate_material_processing_yaml()

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
    
    # Step 4: Inspect raw data files for metadata and QC
    logger.info("4. Inspecting raw data files...")
    manager.raw_data_inspector(cores=4)

    # Step 5: Generate NMDC metadata packages
    logger.info("5. Generating NMDC metadata packages...")
    manager.generate_nmdc_metadata_for_workflow() # Set test to FALSE for actual run.
    assert manager.should_skip('metadata_packages_generated'), "NMDC metadata package generation must complete successfully to proceed"

    # # Step 6: Submit metadata packages to dev environment
    # logger.info("6. Submitting metadata packages to dev environment...")
    # dev_success = manager.submit_metadata_packages_to_dev()
    # if not dev_success:
    #     logger.error("Failed to submit metadata packages to dev environment")
    #     logger.error("Please fix the issues and re-run. Skipping production submission.")
    #     return  # Exit without proceeding to prod
    # else:
    #     logger.info("Successfully submitted metadata packages to dev environment")

    # # Step 7: Submit metadata packages to prod environment (will only run if dev submission was successful)
    # logger.info("7. Submitting metadata packages to prod environment...")
    # prod_success = manager.submit_metadata_packages_to_prod()
    # if not prod_success:
    #     logger.error("Failed to submit metadata packages to prod environment")
    # else:
    #     logger.info("Successfully submitted metadata packages to prod environment")

if __name__ == "__main__":
    asyncio.run(main())