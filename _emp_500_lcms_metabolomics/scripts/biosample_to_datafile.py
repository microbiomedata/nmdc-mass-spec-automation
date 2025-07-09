"""
This script will read in the biosample metadata file (biosample_attributes.csv) and attempt to match them to the raw data files names 
in the LCMS data directory (/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_emp500_lcms/RAW/to_process).
It will then create a mapping from biosample id to raw data file name.
"""

import pandas as pd
import os

##### Read in the biosample metadata file and raw data files and convert them to dataframes =============
biosample_metadata_file = "_emp_500_lcms_metabolomics/biosample_attributes.csv"
biosample_metadata = pd.read_csv(biosample_metadata_file)
raw_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_emp500_lcms/RAW/to_process"
raw_data_files = os.listdir(raw_data_dir)
raw_data_files_df = pd.DataFrame(raw_data_files, columns=["raw_data_file"])
print(f"Number of raw data files found: {raw_data_files_df.shape[0]}")

##### Map raw data files based on submitter_id =============
biosample_metadata["submitter_id_short"] = biosample_metadata["submitter_id"].str.extract(r'(\w+\.\d+\.s\d+)')[0]
biosample_metadata["submitter_id_short"] = biosample_metadata["submitter_id_short"].str.replace('.', '-', regex=False)
raw_data_files_df["submitter_id_short"] = raw_data_files_df["raw_data_file"].str.extract(r'_([^_]+-\d+-s\d+)(?:-a\d+)?\.raw')[0]
biosample_metadata_no_nas = biosample_metadata[biosample_metadata["submitter_id_short"].notna()]
mapped_raw_data_files = raw_data_files_df.merge(biosample_metadata_no_nas[["submitter_id_short", "id"]], on="submitter_id_short", how="left")
mapped_raw_data_files = mapped_raw_data_files[mapped_raw_data_files["id"].notna()]
# Report how many records from raw_data_files_df have been mapped to biosample_metadata
print(f"Number of mapped raw files: {mapped_raw_data_files.shape[0]} out of {raw_data_files_df.shape[0]} after merging on submitter_id_short")

##### Map raw data files based on lcms_name =============
biosample_metadata_with_lcms_name = biosample_metadata[biosample_metadata["lcms_name"].notna()].copy()
biosample_metadata_with_lcms_name = biosample_metadata_with_lcms_name[~biosample_metadata_with_lcms_name["id"].isin(mapped_raw_data_files["id"].dropna())]
biosample_metadata_with_lcms_name["lcms_name"] = biosample_metadata_with_lcms_name["lcms_name"] + ".raw"
mapped_raw_data_files_lcms = raw_data_files_df.merge(biosample_metadata_with_lcms_name[["lcms_name", "id"]], left_on="raw_data_file", right_on="lcms_name", how="left")
mapped_raw_data_files_lcms = mapped_raw_data_files_lcms[mapped_raw_data_files_lcms["id"].notna()]
mapped_raw_data_files = pd.concat([mapped_raw_data_files, mapped_raw_data_files_lcms])
# Report how many records from raw_data_files_df have been mapped to biosample_metadata after merging on lcms_name
print(f"Number of mapped raw files: {mapped_raw_data_files.shape[0]} out of {raw_data_files_df.shape[0]} after merging on lcms_name")

##### Map raw data files based on NCBI biosample name =============
unmapped_raw_data_files = raw_data_files_df[~raw_data_files_df["raw_data_file"].isin(mapped_raw_data_files["raw_data_file"])].copy()
biosample_metadata["biosample_name_short"] = biosample_metadata["name"].str.extract(r' - ([^ ]+)$')[0]
biosample_metadata["biosample_name_short"] = biosample_metadata["biosample_name_short"].str.replace('.', '-', regex=False)
biosample_metadata["biosample_name_short"] = biosample_metadata["biosample_name_short"].str.lower()
biosample_metadata_with_name = biosample_metadata[~biosample_metadata["id"].isin(mapped_raw_data_files["id"].dropna())]
unmapped_raw_data_files["biosample_name_short"] = unmapped_raw_data_files["raw_data_file"].str.extract(r'_(.+)\.raw')[0]
unmapped_raw_data_files["biosample_name_short"] = unmapped_raw_data_files["biosample_name_short"].str.lower()
mapped_raw_data_files_name = unmapped_raw_data_files.merge(biosample_metadata_with_name[["biosample_name_short", "id"]], on="biosample_name_short", how="left")
mapped_raw_data_files_name = mapped_raw_data_files_name[mapped_raw_data_files_name["id"].notna()]
mapped_raw_data_files = pd.concat([mapped_raw_data_files, mapped_raw_data_files_name])
# Report how many records from raw_data_files_df have been mapped to biosample
print(f"Number of mapped raw files: {mapped_raw_data_files.shape[0]} out of {raw_data_files_df.shape[0]} after merging on biosample_name_short")

##### Map raw data files for stegn 36 ============
unmapped_raw_data_files = unmapped_raw_data_files[~unmapped_raw_data_files["raw_data_file"].isin(mapped_raw_data_files["raw_data_file"])].copy()
biosample_metadata_stegen = biosample_metadata[~biosample_metadata["id"].isin(mapped_raw_data_files["id"].dropna())]
biosample_metadata_stegen = biosample_metadata_stegen[biosample_metadata_stegen["submitter_id"].str.contains("stegen.36", case=False, na=False)].copy()
biosample_metadata_stegen["biosample_name_short"] = biosample_metadata_stegen["biosample_name_short"].str.replace('36', '', regex=False)
mapped_raw_data_files_stegen = unmapped_raw_data_files.merge(biosample_metadata_stegen[["biosample_name_short", "id"]], on="biosample_name_short", how="left")
mapped_raw_data_files_stegen = mapped_raw_data_files_stegen[mapped_raw_data_files_stegen["id"].notna()]
mapped_raw_data_files = pd.concat([mapped_raw_data_files, mapped_raw_data_files_stegen])
# Report how many records from raw_data_files_df have been mapped
print(f"Number of mapped raw files: {mapped_raw_data_files.shape[0]} out of {raw_data_files_df.shape[0]} after merging on stegen 36 biosample_name_short")

##### Map raw data files for Makhalanyane 46 ============
unmapped_raw_data_files = unmapped_raw_data_files[~unmapped_raw_data_files["raw_data_file"].isin(mapped_raw_data_files["raw_data_file"])].copy()
biosample_metadata_makhalanyane = biosample_metadata[~biosample_metadata["id"].isin(mapped_raw_data_files["id"].dropna())]
biosample_metadata_makhalanyane = biosample_metadata_makhalanyane[biosample_metadata_makhalanyane["submitter_id"].str.contains("makhalanyane.46", case=False, na=False)].copy()
biosample



print("here")
"""
# SOME NOTES:
72 samples are still not mapped.

# 48 samples cannot be mapped to NMDC biosamples because they are not in NMDC
23 of these are "thomas 19" samples, which are not in NMDC because they are not associated with metagenomes and therefore do not have a biosample record in NMDC.
24 of these are from "pinto 63" samples, which are not in NMDC because they are not associated with metagenomes and therefore do not have a biosample record in NMDC.
1 is "13114.rohwer.84.s006" which is not in NMDC because it is not associated with a metagenome and therefore does not have a biosample record in NMDC.

# 35 samples can be mapped to NMDC biosamples, and we will do this manually
3 are reruns (they have extra numbers at the end of the raw file name e.g. "3B12_6_18_palenik-42-s003-a02_20190209212644.raw" instead of "3B12_6_18_palenik-42-s003-a02.raw").  We can map these to biosample ids.
10 are from Makhalanyane46 but do not have the study id in the raw file name ("Makhalanyane46.TGP13" is just "TGP13" in the raw file name).  We can map these to biosample ids.  These are all on plate 8.
10 are from Shade23 but do not have the study id in the raw file name ("Shade23.Cen14.12102015" is just "Cen14-12102015.raw" in the raw file name).  We can map these to biosample ids. These are on plates 8 and 9.
## This will give us a total of 621 samples that can be mapped to biosample ids and processed!  Wahoo!

# We should be able to get the extraction protocol from NCBI in the "lcms_extraction_protocol" attribute
"""


print('here')









print("here")
