"""
# This script will write several json files to be used for processing the raw data in batches.
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

import os
import json
from pathlib import Path
from json_prepper_gcms import generate_json

if __name__ == "__main__":
    # Outer "batch directory" for the raw data
    batched_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_emp500_metabolomics/batched_raw"
    json_output_dir = "_emp_500_metabolomics"

    # Get the list of batch directories
    batch_dirs = [str(f) for f in Path(batched_data_dir).rglob("batch_*")]

    # For each batch directory, find the calibration file and generate the json file
    for batch_dir in batch_dirs:
        # Get the list of raw data files in the batch directory
        raw_data_files = [str(f) for f in Path(batch_dir).rglob("*.cdf")]
        # Get the raw data file name that contains "FAME"
        calibration_file_name = None
        for file in raw_data_files:
            if "FAME" in file:
                calibration_file_name = os.path.basename(file)
                print(calibration_file_name + " is the calibration file for " + batch_dir)
                break
        
        cores = 5
        output_dir = "output_" + os.path.basename(batch_dir)
        output_json = os.path.join(json_output_dir, "run_metaMS_gcms_" + os.path.basename(batch_dir) + ".json")

        # Generate the json file
        generate_json(
            raw_data_dir=batch_dir,
            calibration_file_name=calibration_file_name,
            cores=cores,
            output_dir=output_dir,
            output_json=output_json
        )