# This script will organize the EMP500 raw data into directories that are to be processed batchwise.
# Each batch will be associated with a single fames file.

import pandas as pd
import os
import shutil

# Set raw data directory
raw_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_emp500_metabolomics/raw/"
batched_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_emp500_metabolomics/batched_raw"

# Read in excel file (first sheet) from here: _emp_500_metabolomics/EMP500_Biosample_Mapping_SOP_04.xlsx
df = pd.read_excel("_emp_500_metabolomics/EMP500_Biosample_Mapping_SOP_04.xlsx")

# Subset df to only columns of interest, Dataset_Num, Dataset_ID, Acq_Time_Start
df = df[["Dataset_Num", "Acq_Time_Start"]]

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
for batch in df["Batch"].unique():
    # Create new directory
    batch_dir = os.path.join(batched_data_dir, f"batch_{batch}")
    os.makedirs(batch_dir, exist_ok=True)

    # Get all files associated with that batch
    batch_files = df[df["Batch"] == batch]["Dataset_Path"].tolist()

    # Copy files to new directory
    for file in batch_files:
        shutil.copy(file, batch_dir)
