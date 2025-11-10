#!/usr/bin/env python3
"""
Singer study workflow runner with LLM-powered biosample mapping.
Run this from the root directory: /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing

This version uses an LLM to automatically generate and refine the biosample mapping script.

Prerequisites:
- Create a .env file in the project root with:
  GITHUB_TOKEN_LLM=your_github_token (for GitHub Models API)
  OR
  OPENAI_API_KEY=your_openai_key (for OpenAI API)
"""

import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the utils directory to path (assuming run from root directory)
sys.path.append(str(Path.cwd() / "nmdc_dp_utils"))

from study_manager import NMDCStudyManager

def main():
    """Run the study workflow with LLM-powered biosample mapping."""

    # Initialize study manager with the Singer config (assuming run from root directory)
    config_path = Path.cwd() / "studies" / "singer_11_46aje659" / "singer_config.json"
    study = NMDCStudyManager(str(config_path))
    
    print(f"=== {study.study_name.upper()} WORKFLOW (LLM-POWERED) ===")
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

    # Step 4: Get biosample attributes
    print("\n4. Getting biosample attributes...")
    biosample_csv = study.get_biosample_attributes()
    print(f"Biosample attributes saved to: {biosample_csv}")
    
    mapping_script = study.generate_biosample_mapping_script()
    print(f"Mapping script generated: {mapping_script}")    

    # Step 5: LLM-POWERED BIOSAMPLE MAPPING (replaces manual template customization)
    print("\n5. LLM-powered biosample mapping...")
    print("This will automatically:")
    print("  - Analyze filename patterns and biosample attributes")
    print("  - Generate a customized mapping script")
    print("  - Test and iteratively refine the script")
    print("  - Target: 70% match rate (configurable in config)")
    
    mapping_success = study.llm_generate_and_refine_mapping_script(
        max_iterations=5,        # Try up to 5 refinement iterations
        use_github_models=True   # Use GitHub Models (set to False for OpenAI)
    )
    
    if not mapping_success:
        print("\n⚠️  LLM mapping did not achieve target success rate")
        print("You can:")
        print("  1. Review the generated script and manually refine it")
        print("  2. Increase max_iterations")
        print("  3. Lower llm_mapping_success_threshold in config")
        print("  4. Check unmapped files for patterns the LLM might have missed")
        
        # Optionally continue anyway if some files were mapped
        response = input("\nContinue with current mapping? (y/n): ")
        if response.lower() != 'y':
            print("Exiting. Fix mapping issues and try again.")
            return
    else:
        print("✅ Biosample mapping completed successfully with LLM!")
    
    # Step 6: Inspect raw data files for metadata and QC
    print("\n6. Inspecting raw data files...")
    _ = study.raw_data_inspector(
        cores=1,    # Single core for .raw files to prevent Docker crashes
        limit=None  # Process all files  
    )
    assert study.should_skip('raw_data_inspected'), "Raw data inspection must complete successfully to proceed"

    # Step 7: Generate metadata mapping files with URL validation
    print("\n7. Generating metadata mapping files with URL validation...")
    metadata_success = study.generate_metadata_mapping_files()
    assert study.should_skip('metadata_mapping_generated'), "Metadata mapping generation must complete successfully to proceed"
    assert metadata_success, "Metadata mapping generation failed."

    # Step 8: Generate WDL JSON files for processing, make runner script, and process them
    print("\n8. Generating WDL JSON files...")
    json_count = study.generate_wdl_jsons()
    print(f"WDL JSON files: {json_count}")
    print("\n...Generating WDL runner script...")
    script_path = study.generate_wdl_runner_script()
    print(f"Generated script: {script_path}")

    # Step 9: Run WDL workflows
    print("\n9. Running WDL workflows...")
    study.run_wdl_script(script_path)
    assert study.should_skip('data_processed'), "WDL workflows must complete successfully to proceed"

    # Step 10: Upload processed data to MinIO
    print("\n10. Uploading processed data to MinIO...")
    upload_success = study.upload_processed_data_to_minio()
    if upload_success:
        print("✅ Processed data uploaded to MinIO successfully")
    else:
        print("⚠️  MinIO upload failed or was skipped")
    assert study.should_skip('processed_data_uploaded_to_minio'), "Processed data upload to MinIO must complete successfully to proceed"

    # Step 11: Generate NMDC metadata
    print("\n11. Generating NMDC metadata submission packages...")
    metadata_packages_success = study.generate_nmdc_metadata_for_workflow()
    assert metadata_packages_success, "NMDC metadata package generation failed."

if __name__ == "__main__":
    main()
