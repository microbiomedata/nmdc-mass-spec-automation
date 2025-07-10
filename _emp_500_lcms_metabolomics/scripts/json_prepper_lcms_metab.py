"""
# This script will write a .json file to run the lipidomics wdl workflow
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
7. output json file name (default is run_metaMS_lcms_metabolomics.json)
"""

import json
from pathlib import Path

def generate_json(
        raw_data_dir, 
        corems_toml, 
        db_location, 
        scan_translator_path, 
        cores=1, 
        output_dir="output",
        output_json="run_metaMS_lcms_metabolomics.json"
    ):
    # Get the list of raw data files
    raw_data_files = [str(f) for f in Path(raw_data_dir).rglob("*.raw")]

    # Create the json object
    json_obj = {
        "lcmsLipidomics.runMetaMSLCMSMetabolomics.file_paths": raw_data_files,
        "lcmsLipidomics.runMetaMSLCMSMetabolomics.output_directory": output_dir,
        "lcmsLipidomics.runMetaMSLCMSMetabolomics.corems_toml_path": corems_toml,
        "lcmsLipidomics.runMetaMSLCMSMetabolomics.msp_file_path": db_location,
        "lcmsLipidomics.runMetaMSLCMSMetabolomics.scan_translator_path":scan_translator_path,
        "lcmsLipidomics.runMetaMSLCMSMetabolomics.cores": cores
    }

    # Write the json object to a file
    with open(output_json, "w") as f:
        json.dump(json_obj, f, indent=4)

    print(f"JSON file written to {output_json}")

if __name__ == "__main__":
    # Must be set for the script to run
    raw_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_emp500_lcms/RAW/to_process"
    corems_toml = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/metadata/emp_lcms_metab_corems_params.toml"
    db_location = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/metams/test_data/test_lcms_metab_data/20250407_database.msp"
    scan_translator_path = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/configurations/emsl_lipidomics_scan_translator.toml"

    # Optional arguments
    cores = 1
    output_dir = "output"
    output_json = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/run_metaMS_lcms_metabolomics.json"

    generate_json(
        raw_data_dir, 
        corems_toml, 
        db_location, 
        scan_translator_path, 
        cores, 
        output_dir, 
        output_json
    )