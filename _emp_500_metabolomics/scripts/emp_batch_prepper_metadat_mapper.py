# This script will organize the EMP500 raw data into directories that are to be processed batchwise.
# Each batch will be associated with a single fames file.
# It will also create a metadata file that will be used as an input for the metadata generation script

import pandas as pd
import os
import shutil
from nmdc_api_utilities.biosample_search import BiosampleSearch 

# Should we move the batched samples again?
rebatch = False

# Set raw data directory
raw_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_emp500_metabolomics/raw/"
batched_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_emp500_metabolomics/batched_raw"
processed_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_emp500_metabolomics/processed_20250212"

# Read in excel file (first sheet) from here: _emp_500_metabolomics/EMP500_Biosample_Mapping_SOP_04.xlsx
df = pd.read_excel("_emp_500_metabolomics/EMP500_Biosample_Mapping_SOP_04.xlsx")

# Subset df to only columns of interest, Dataset_Num, Dataset_ID, Acq_Time_Start
df = df[["Dataset_Num", "Acq_Time_Start", "Acq_Time_End", "GOLD bioproject", "nmdc biosample id"]]

# Reorder by Acq_Time_Start and reset index
df = df.sort_values(by="Acq_Time_Start")
df = df.reset_index(drop=True)

# If "FAMES" somewhere in Dataset_Num, then add to new column "FAMES", with value of the Dataset_Num
df["FAMES"] = pd.NA
df.loc[df["Dataset_Num"].str.contains("FAME"), "FAMES"] = df["Dataset_Num"]

# Create a new column "Batch" that is the index of the first non-null value in FAMES
df["Batch"] = df["FAMES"].notnull().cumsum()

# Fill in FAMES and Batch values forward
df["FAMES"] = df["FAMES"].fillna(method="ffill")

# Add Dataset_Path column
df["Dataset_Path"] = raw_data_dir + df["Dataset_Num"] + ".cdf"

# For each batch, create a new directory called "batch_#" within the batched data directory and copy all files associated with that batch into the directory
if rebatch:
    for batch in df["Batch"].unique():
        # Create new directory
        batch_dir = os.path.join(batched_data_dir, f"batch_{batch}")
        os.makedirs(batch_dir, exist_ok=True)

        # Get all files associated with that batch
        batch_files = df[df["Batch"] == batch]["Dataset_Path"].tolist()

        # Copy files to new directory
        for file in batch_files:
            shutil.copy(file, batch_dir)

# Clean up and prepare for metadata file
df_new = df[["Dataset_Path", "Batch", "FAMES", "GOLD bioproject", "nmdc biosample id"]].drop_duplicates()

# biosample_id from "nmdc biosample id" column
df_new["biosample_id"] = df_new["nmdc biosample id"]
df_new["biosample.associated_studies"] = "['nmdc:sty-11-547rwq94']"
df_new["raw_data_file"] = df_new["Dataset_Path"]
df_new["processed_data_file"] = processed_data_dir + "/" + df_new["Dataset_Path"].str.split("/").str[-1].str.replace(".cdf", ".csv")
df_new["calibration_file"] = raw_data_dir + df_new["FAMES"] + ".cdf"
df_new["mass_spec_configuration_name"] = "EMSL metabolomics GC/MS mass spectrometry method"
df_new["chromat_configuration_name"] = "EMSL GC method for metabolites"
df_new["instrument_used"] = "Agilent GC-MS"
df_new["processing_institution"] = "EMSL"
df_new["gold_id"] = df_new["GOLD bioproject"]
df_new["instrument_analysis_start_date"] = df["Acq_Time_Start"]
df_new["instrument_analysis_end_date"] = df["Acq_Time_End"]
df_new["execution_resource"] = "EMSL-RZR"

# drop If "FAMES" somewhere in Dataset_Num
df_new = df_new[~df["Dataset_Num"].str.contains("FAME")]

# Get new biosample_ids from mongo dev by matching to gold_biosample_identifiers
# in mongo-dev, pull out biosamples from study nmdc:sty-11-547rwq94, get id and alternative identifiers only
## instantiate biosample_search
biosample_search = BiosampleSearch(env="dev")
biosamples = biosample_search.get_record_by_attribute(
    attribute_name="associated_studies",
    attribute_value="nmdc:sty-11-547rwq94",
    max_page_size=500,
    all_pages=True,
    exact_match=True) 
## pull out the id and gold_biosample_identifiers keys from each biosample
biosample_subs = [{"biosample_id": b["id"], "gold_id": b["gold_biosample_identifiers"], "biosample.name": b["name"]} for b in biosamples]
## create a dataframe from the biosample_subs
biosample_subs_df = pd.DataFrame(biosample_subs).explode("gold_id")
## remove the "gold:" prefix from the gold_id
biosample_subs_df["gold_id"] = biosample_subs_df["gold_id"].str.replace("gold:", "")

## replace the biosample_id in df_new with the biosample_id from biosample_subs_df
df_new.drop(columns=["biosample_id"], inplace=True)
df_new = df_new.merge(biosample_subs_df, on="gold_id", how="left")


# Keep only the new columns
df_new = df_new[[
    "biosample_id", "biosample.associated_studies","biosample.name",
    "raw_data_file", 
    "processed_data_file", "calibration_file", "mass_spec_configuration_name", 
    "chromat_configuration_name", "instrument_used", "processing_institution",
    "instrument_analysis_start_date", "instrument_analysis_end_date", "execution_resource",
    "gold_id"
    ]]

# Separate rows without biosample_id to a separate file
df_new_missing = df_new[df_new["biosample_id"].isnull()]
df_new = df_new[~df_new["biosample_id"].isnull()]

# Make a small subest for testing
df_new_test = df_new.sample(10)

# write to csv
df_new.to_csv("_emp_500_metabolomics/emp500_metabolomics_metadata.csv", index=False)
df_new_missing.to_csv("_emp_500_metabolomics/emp500_metabolomics_biosamples_missing.csv", index=False)
df_new_test.to_csv("_emp_500_metabolomics/emp500_metabolomics_metadata_test.csv", index=False)