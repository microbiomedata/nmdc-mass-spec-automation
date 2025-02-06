"""
# This script will write a .json file to run the lipidomics wdl workflow
# Example json below:
{
    "lcmsLipidomics.runMetaMSLCMSLipidomics.file_paths": [
        "./data/raw_data/Blanch_Nat_Lip_C_12_AB_M_17_NEG_25Jan18_Brandi-WCSH5801.raw"
    ],
    "lcmsLipidomics.runMetaMSLCMSLipidomics.output_directory": "output",
    "lcmsLipidomics.runMetaMSLCMSLipidomics.corems_toml_path": "./configuration/lipid_configs/emsl_lipidomics_corems_params.toml",
    "lcmsLipidomics.runMetaMSLCMSLipidomics.metabref_token_path": "./configuration/gcms_corems.toml",
    "lcmsLipidomics.runMetaMSLCMSLipidomics.scan_translator_path": "./configuration/lipid_configs/emsl_lipidomics_scan_translator.toml",
    "lcmsLipidomics.runMetaMSLCMSLipidomics.cores": 1
}

Inputs will be 
1. path to the directory containing the raw data files
2. path to the output directory (default is output)
3. path to the corems toml file
4. path to the metabref token file
5. path to the scan translator file
6. number of cores to use
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
        output_json="run_metaMS_lipidomics.json"
    ):
    # Get the list of raw data files
    raw_data_files = [str(f) for f in Path(raw_data_dir).rglob("*.raw")]

    # Create the json object
    json_obj = {
        "lcmsLipidomics.runMetaMSLCMSLipidomics.file_paths": raw_data_files,
        "lcmsLipidomics.runMetaMSLCMSLipidomics.output_directory": output_dir,
        "lcmsLipidomics.runMetaMSLCMSLipidomics.corems_toml_path": corems_toml,
        "lcmsLipidomics.runMetaMSLCMSLipidomics.db_location": db_location,
        "lcmsLipidomics.runMetaMSLCMSLipidomics.scan_translator_path":scan_translator_path,
        "lcmsLipidomics.runMetaMSLCMSLipidomics.cores": cores
    }

    # Write the json object to a file
    with open(output_json, "w") as f:
        json.dump(json_obj, f, indent=4)

    print(f"JSON file written to {output_json}")

if __name__ == "__main__":
    # Must be set for the script to run
    raw_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_stegen_lipidomics/raw"
    corems_toml = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/configurations/emsl_lipidomics_corems_params.toml"
    db_location = "/Users/heal742/LOCAL/05_NMDC/00_Lipid_Databse/lipid_db/lipid_ref.sqlite"
    scan_translator_path = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/configurations/emsl_lipidomics_scan_translator.toml"

    # Optional arguments
    cores = 1
    output_dir = "output"
    output_json = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_stegen_11_aygzgv51/run_metaMS_lipidomics.json"

    generate_json(
        raw_data_dir, 
        corems_toml, 
        db_location, 
        scan_translator_path, 
        cores, 
        output_dir, 
        output_json
    )