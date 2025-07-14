"""
# This script will write multiple .json files to run the lipidomics wdl workflow, with 50 samples per batch
# Example json below:
{
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.file_paths": [
        "./test_data/test_lipid_data/Blanch_Nat_Lip_C_4_AB_M_08_NEG_25Jan18_Brandi-WCSH5801.raw",
        "./test_data/test_lipid_data/Blanch_Nat_Lip_C_4_AB_M_08_POS_23Jan18_Brandi-WCSH5801.raw"
    ],
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.output_directory": "output",
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.corems_toml_path": "./configuration/lcms_metab_configs/emsl_lcms_metab_corems_params.toml",
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.msp_file_path": "./test_data/test_lcms_metab_data/database.msp",
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.scan_translator_path": "./configuration/lcms_metab_configs/emsl_lcms_metab_scan_translator.toml",
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.cores": 1
}

Inputs will be:
1. path to the directory containing the raw data files
2. path to the output directory (default is output)
3. path to the corems toml file
4. path to the msp file
5. path to the scan translator file
6. number of cores to use (default is 1)
7. batch size (default is 50)
8. output directory for json files
"""

import json
from pathlib import Path

def generate_batch_jsons(
        raw_data_dir, 
        corems_toml, 
        db_location, 
        scan_translator_path, 
        cores=1, 
        output_dir="output",
        batch_size=50,
        json_output_dir=".",
        problem_files=None
    ):
    # Get the list of all raw data files
    raw_data_files = [str(f) for f in Path(raw_data_dir).rglob("*.raw")]
    
    print(f"Found {len(raw_data_files)} raw data files")
    
    # Filter out problem files if specified
    if problem_files:
        original_count = len(raw_data_files)
        raw_data_files = [f for f in raw_data_files if not any(problem in f for problem in problem_files)]
        filtered_count = original_count - len(raw_data_files)
        print(f"Filtered out {filtered_count} problem files")
        print(f"Remaining {len(raw_data_files)} files for processing")

    # Split files into batches
    batches = [raw_data_files[i:i + batch_size] for i in range(0, len(raw_data_files), batch_size)]
    
    print(f"Creating {len(batches)} batches with up to {batch_size} files each")
    
    # Create JSON file for each batch
    for batch_num, batch_files in enumerate(batches, 1):
        json_obj = {
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.file_paths": batch_files,
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.output_directory": output_dir,
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.corems_toml_path": corems_toml,
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.msp_file_path": db_location,
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.scan_translator_path": scan_translator_path,
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.cores": cores
        }
        
        # Create output filename
        output_json = f"{json_output_dir}/run_metaMS_lcms_metabolomics_batch{batch_num}.json"
        
        # Write the json object to a file
        with open(output_json, "w") as f:
            json.dump(json_obj, f, indent=4)
        
        print(f"Batch {batch_num}: {len(batch_files)} files written to {output_json}")

if __name__ == "__main__":
    # Must be set for the script to run
    raw_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_emp500_lcms/RAW/to_process"
    corems_toml = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/metadata/emp_lcms_metab_corems_params.toml"
    db_location = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/metams/test_data/test_lcms_metab_data/20250407_database.msp"
    scan_translator_path = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/metadata/emp500_scan_translator.toml"

    # Optional arguments
    cores = 1
    output_dir = "output"
    batch_size = 50
    json_output_dir = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/wdl_jsons"

    # Known problem files to exclude
    problem_files = [
        "1E11_2_27_bowen-74-s010-a04.raw"
    ]

    generate_batch_jsons(
        raw_data_dir, 
        corems_toml, 
        db_location, 
        scan_translator_path, 
        cores, 
        output_dir, 
        batch_size,
        json_output_dir,
        problem_files=problem_files
    )