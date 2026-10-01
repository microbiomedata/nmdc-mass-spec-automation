#!/usr/bin/env python3
"""
MONet - NOM workflow
all FT-ICR, non-MALDI data released on ScienceCentral as of 21 May 2026
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
    config_path = "workflows/monet_nom_metadata/monet_nom_metadata_config.json"
    manager = NMDCWorkflowManager(str(config_path))

    logger = manager.logger
    logger.info(f"=== {manager.workflow_name.upper()} WORKFLOW ===")

    # Step 1: Create workflow structure
    logger.info("1. Creating workflow structure...")
    manager.create_workflow_structure()

    # Step 2: Generate protocol YAML outline using LLM
    logger.info("2. Generating protocol YAML outline using LLM...")
    await manager.generate_material_processing_yaml()

    # Step 3: Manual list of all raw data files
    logger.info("3. Using provided downloaded_files.csv")

    # Step 4: Map raw data files to biosamples
    logger.info("4. Mapping raw data files to biosamples using LLM...")
    manager.get_biosample_attributes()
    # mapping_success = await manager.generate_llm_biosample_mapping()
    
    # if not mapping_success:
    #     logger.warning("Biosample mapping failed - review logs and add additional context if needed")
    # else:
    #     logger.info("Biosample mapping completed successfully")

    # I give up trying to get this LLM to map files to biosamples. It's so straightforward and it's being so dumb.
    # Generate mapped raw files csv using manually created llm_biosample_raw_file_mapper.csv
    manager._generate_mapped_files_list()

    # Step 5: Generate material processing metadata
    logger.info("5. Generating material processing metadata packages...")
    manager.generate_material_processing_metadata()
    assert manager.should_skip('material_processing_metadata_generated'), "NMDC material processing metadata package generation must complete successfully to proceed"

    # Step 6: Submit metadata packages to dev environment
    logger.info("6. Submitting metadata packages to dev environment...")
    dev_success = manager.submit_metadata_packages_to_dev()
    if not dev_success:
        logger.error("Failed to submit metadata packages to dev environment")
        logger.error("Please fix the issues and re-run. Skipping production submission.")
        return  # Exit without proceeding to prod
    else:
        logger.info("Successfully submitted metadata packages to dev environment")

    # Step 7: Submit metadata packages to prod environment (will only run if dev submission was successful)
    logger.info("7. Submitting metadata packages to prod environment...")
    prod_success = manager.submit_metadata_packages_to_prod()
    if not prod_success:
        logger.error("Failed to submit metadata packages to prod environment")
    else:
        logger.info("Successfully submitted metadata packages to prod environment")

if __name__ == "__main__":
    asyncio.run(main())