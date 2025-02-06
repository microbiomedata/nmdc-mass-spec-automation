"""
# This script will write a .json file to run the lipidomics wdl workflow
# Example json below:
{
    "gcmsMetabolomics.runMetaMSGCMS.file_paths": [
    "./data/raw_data/GCMS_FAMES_01_GCMS-01_20191023.cdf",
    "./data/raw_data/GCMS_FAMES_01_GCMS-01_20191023.cdf"
],
"gcmsMetabolomics.runMetaMSGCMS.calibration_file_path": "./data/raw_data/GCMS_FAMES_01_GCMS-01_20191023.cdf",
"gcmsMetabolomics.runMetaMSGCMS.output_directory": "test_output",
"gcmsMetabolomics.runMetaMSGCMS.output_filename": "test_dataset",
"gcmsMetabolomics.runMetaMSGCMS.output_type": "csv",
"gcmsMetabolomics.runMetaMSGCMS.corems_toml_path": "./configuration/gcms_corems.toml",
"gcmsMetabolomics.runMetaMSGCMS.jobs_count": 4
}

Inputs will be 
1. path to the directory containing the raw data files
2. name of the calibration file within the raw data directory to be used for calibration
3. path to the output directory (default is output)
4. ouput_type (default is csv)
5. path to the corems toml file (default is ./configurations/emsl_gcms_corems_params.toml)
"""

import argparse
import os
import json
from pathlib import Path

def generate_json(
        raw_data_dir,
        calibration_file_name, 
        corems_toml="/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/configurations/emsl_gcms_corems_params.toml",
        output_type="csv",
        cores=1, 
        output_dir="output",
        output_json="run_metaMS_gcms.json"
    ):
    # Get the list of raw data files
    raw_data_files = [str(f) for f in Path(raw_data_dir).rglob("*.cdf")]

    # From the list of raw data files, find the calibration file
    calibration_file = None
    for file in raw_data_files:
        if calibration_file_name in file:
            calibration_file = file
            break
    if calibration_file is None:
        raise ValueError(f"Calibration file {calibration_file_name} not found in {raw_data_dir}")
    
    # Create the json object
    json_obj = {
        "gcmsMetabolomics.runMetaMSGCMS.file_paths": raw_data_files,
        "gcmsMetabolomics.runMetaMSGCMS.calibration_file_path": calibration_file,
        "gcmsMetabolomics.runMetaMSGCMS.output_directory": output_dir,
        "gcmsMetabolomics.runMetaMSGCMS.output_type": output_type,
        "gcmsMetabolomics.runMetaMSGCMS.corems_toml_path": corems_toml,
        "gcmsMetabolomics.runMetaMSGCMS.jobs_count": cores,
        "gcmsMetabolomics.runMetaMSGCMS.output_filename": "test_dataset" # This is not used in the current version of the workflow
    }

    # Write the json object to a file
    with open(output_json, "w") as f:
        json.dump(json_obj, f, indent=4)

    print(f"JSON file written to {output_json}")

if __name__ == "__main__":
    # Must be set for the script to run
    raw_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_blanchard_metabolomics/raw"
    calibration_file_name = "GCMS_FAMEs_01_GCMS01_20180115.cdf"

    # Optional arguments
    cores = 5
    output_dir = "output"
    output_json = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_blanchard_metabolomics/run_metaMS_gcms.json"

    generate_json(raw_data_dir, calibration_file_name, cores=cores, output_dir=output_dir, output_json=output_json)