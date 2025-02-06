"""
# This script will add the ###_ tag to the metadata mapping files

Parameters: 
File to mapping data (example here: /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_brodie_11_dcqce727/nmdc_sty-11-dcqce727_mappings.csv)
Path to raw data (example here: /Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_brodie_lipidomics/raw)

Instructions:
Read in the mapping file, read in all raw data, match and add the ###_ tag to the metadata mapping file fields

"""

import os
import pandas as pd
from pathlib import Path

def add_tag_to_mapping(mapping_file, raw_data_dir, output_file):
    # Read in the mapping file
    mapping_df = pd.read_csv(mapping_file)
    mapping_df['full_dms_name'] = ""

    # Get the list of raw data files
    raw_data_files = [str(f) for f in Path(raw_data_dir).rglob("*.raw")]

    # Match the raw data files with the mapping file and add the ###_ tag
    for raw_data_file in raw_data_files:
        raw_data_file_name = os.path.basename(raw_data_file)
        for i, row in mapping_df.iterrows():
            # Get the file name without the extension by removing the leading ###_ tag (where ### is a variable number of digits)
            file_name_no_ext = raw_data_file_name.split(".")[0].split("_", 1)[1]
            if file_name_no_ext in row["dms_dataset_name"]:
                # Remove ".raw" from the end of the file name and add it as the full_dms_name
                mapping_df.at[i, "full_dms_name"] = raw_data_file_name.split(".")[0]
    
    # Replace the dms_dataset_name with the full_dms_name in the "Raw Data File" column
    for i, row in mapping_df.iterrows():
        if row["full_dms_name"] != "":
            mapping_df.at[i, "Raw Data File"] = mapping_df.at[i, "Raw Data File"].replace(row["dms_dataset_name"], row["full_dms_name"])
            mapping_df.at[i, "Processed Data Directory"] = mapping_df.at[i, "Processed Data Directory"].replace(row["dms_dataset_name"], row["full_dms_name"])

    # Write the updated mapping file
    mapping_df.to_csv(output_file, index=False)
    print(f"Updated mapping file written to {output_file}")

if __name__ == "__main__":
    mapping_file = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_stegen_11_aygzgv51/nmdc_sty-11-aygzgv51_mappings.csv"
    raw_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_stegen_lipidomics/raw"
    output_file = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_stegen_11_aygzgv51/nmdc_sty-11-aygzgv51_mappings_updated.csv"

    add_tag_to_mapping(mapping_file, raw_data_dir, output_file)