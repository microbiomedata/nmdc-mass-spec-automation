"""
This script will read in the generated metadata files and gather the ids and name within the data_generation_set 
"""

import json
import pandas as pd


metadata_files = [
    "studies/_bioscales_lcms_metabolomics/metadata/workflow_metadata_HILIC_neg.json",
    "studies/_bioscales_lcms_metabolomics/metadata/workflow_metadata_HILIC_pos.json",
    "studies/_bioscales_lcms_metabolomics/metadata/workflow_metadata_RP_neg.json",
    "studies/_bioscales_lcms_metabolomics/metadata/workflow_metadata_RP_pos.json"
]

data_generation_set_mapping = {}
for metadata_file in metadata_files:
    with open(metadata_file, "r") as f:
        metadata = json.load(f)
        data_generation_set = metadata["data_generation_set"]
        for dg in data_generation_set:
            dg_id = dg["id"]
            dg_name = dg["name"]
            data_generation_set_mapping[dg_id] = dg_name

# Turn into a dataframe and export as csv
dg_df = pd.DataFrame.from_dict(data_generation_set_mapping, orient="index", columns=["name"])
dg_df.index.name = "data_generation_set_id"
dg_df.to_csv("studies/_bioscales_lcms_metabolomics/metadata/bioscales_dg_raw_mapping.csv")