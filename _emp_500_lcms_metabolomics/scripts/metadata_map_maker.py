"""
This script will read in the biosample metadata file (biosample_attributes.csv) and use it to map raw data files to biosample ids.

It will then clean up the output for a final mapping as an input to the metadata generation script.
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
print(f"Number of mapped raw files: {mapped_raw_data_files.shape[0]} out of {raw_data_files_df.shape[0]} after merging on biosample_name_short")

##### Map raw data files for stegn 36 ============
unmapped_raw_data_files = unmapped_raw_data_files[~unmapped_raw_data_files["raw_data_file"].isin(mapped_raw_data_files["raw_data_file"])].copy()
biosample_metadata_stegen = biosample_metadata[~biosample_metadata["id"].isin(mapped_raw_data_files["id"].dropna())]
biosample_metadata_stegen = biosample_metadata_stegen[biosample_metadata_stegen["submitter_id"].str.contains("stegen.36", case=False, na=False)].copy()
biosample_metadata_stegen["biosample_name_short"] = biosample_metadata_stegen["biosample_name_short"].str.replace('36', '', regex=False)
mapped_raw_data_files_stegen = unmapped_raw_data_files.merge(biosample_metadata_stegen[["biosample_name_short", "id"]], on="biosample_name_short", how="left")
mapped_raw_data_files_stegen = mapped_raw_data_files_stegen[mapped_raw_data_files_stegen["id"].notna()]
mapped_raw_data_files = pd.concat([mapped_raw_data_files, mapped_raw_data_files_stegen])
print(f"Number of mapped raw files: {mapped_raw_data_files.shape[0]} out of {raw_data_files_df.shape[0]} after merging on stegen 36 biosample_name_short")

##### Map raw data files for Makhalanyane 46 ============
unmapped_raw_data_files = unmapped_raw_data_files[~unmapped_raw_data_files["raw_data_file"].isin(mapped_raw_data_files["raw_data_file"])].copy()
biosample_metadata_makhalanyane = biosample_metadata[~biosample_metadata["id"].isin(mapped_raw_data_files["id"].dropna())]
biosample_metadata_makhalanyane = biosample_metadata_makhalanyane[biosample_metadata_makhalanyane["submitter_id"].str.contains("makhalanyane", case=False, na=False)].copy()
biosample_metadata_makhalanyane["biosample_name_short"] = biosample_metadata_makhalanyane["biosample_name_short"].str.replace('makhalanyane46-', '', regex=False)
mapped_raw_data_files_makhalanyane = unmapped_raw_data_files.merge(biosample_metadata_makhalanyane[["biosample_name_short", "id"]], on="biosample_name_short", how="left")
mapped_raw_data_files_makhalanyane = mapped_raw_data_files_makhalanyane[mapped_raw_data_files_makhalanyane["id"].notna()]
mapped_raw_data_files = pd.concat([mapped_raw_data_files, mapped_raw_data_files_makhalanyane])
print(f"Number of mapped raw files: {mapped_raw_data_files.shape[0]} out of {raw_data_files_df.shape[0]} after merging on makhalanyane biosample_name_short")

##### Map raw data files for Shade23 ============
unmapped_raw_data_files = unmapped_raw_data_files[~unmapped_raw_data_files["raw_data_file"].isin(mapped_raw_data_files["raw_data_file"])].copy()
biosample_metadata_shade23 = biosample_metadata[~biosample_metadata["id"].isin(mapped_raw_data_files["id"].dropna())]
biosample_metadata_shade23 = biosample_metadata_shade23[biosample_metadata_shade23["submitter_id"].str.contains("shade.23", case=False, na=False)].copy()
biosample_metadata_shade23["biosample_name_short"] = biosample_metadata_shade23["biosample_name_short"].str.replace('shade23-', '', regex=False)
mapped_raw_data_files_shade23 = unmapped_raw_data_files.merge(biosample_metadata_shade23[["biosample_name_short", "id"]], on="biosample_name_short", how="left")
mapped_raw_data_files_shade23 = mapped_raw_data_files_shade23[mapped_raw_data_files_shade23["id"].notna()]
mapped_raw_data_files = pd.concat([mapped_raw_data_files, mapped_raw_data_files_shade23])
print(f"Number of mapped raw files: {mapped_raw_data_files.shape[0]} out of {raw_data_files_df.shape[0]} after merging on shade23 biosample_name_short")

##### Map re-runs based on raw data file name suffix ============
unmapped_raw_data_files = unmapped_raw_data_files[~unmapped_raw_data_files["raw_data_file"].isin(mapped_raw_data_files["raw_data_file"])].copy()
unmapped_raw_data_files["submitter_id_short_rerun"] = unmapped_raw_data_files["raw_data_file"].str.extract(r'_([^_]+-\d+-s\d+)(?:-a\d+)?(?:_\d+)?\.raw')[0]
rerun_samples = unmapped_raw_data_files[unmapped_raw_data_files["submitter_id_short_rerun"].notna()]
mapped_rerun_samples = rerun_samples.merge(biosample_metadata_no_nas[["submitter_id_short", "id"]], left_on="submitter_id_short_rerun", right_on="submitter_id_short", how="left")
mapped_rerun_samples = mapped_rerun_samples[mapped_rerun_samples["id"].notna()]
mapped_raw_data_files = pd.concat([mapped_raw_data_files, mapped_rerun_samples[["raw_data_file", "id"]]])
print(f"Number of mapped raw files: {mapped_raw_data_files.shape[0]} out of {raw_data_files_df.shape[0]} after merging on rerun samples")

##### Add back unmapped raw data files and clean up output ============
final_mapped_raw_data_files = mapped_raw_data_files[["raw_data_file", "id"]].copy()
unmapped_raw_data_files = unmapped_raw_data_files[~unmapped_raw_data_files["raw_data_file"].isin(mapped_raw_data_files["raw_data_file"])].copy()
final_mapped_raw_data_files = pd.concat([final_mapped_raw_data_files, unmapped_raw_data_files[["raw_data_file"]]], ignore_index=True)
# rename id to biosample_id, add associated_studies as ['nmdc:sty-11-547rwq94']
final_mapped_raw_data_files.rename(columns={"id": "biosample_id"}, inplace=True)
final_mapped_raw_data_files.rename(columns={"raw_data_file": "raw_data_file_short"}, inplace=True)
final_mapped_raw_data_files["biosample.associated_studies"] = "['nmdc:sty-11-547rwq94']"

# Add additional columns to the final mapped raw data files
final_mapped_raw_data_files["material_processing_type"] = "unknown"
final_mapped_raw_data_files["raw_data_file"] = raw_data_dir + "/" + final_mapped_raw_data_files["raw_data_file_short"]
final_mapped_raw_data_files["processed_data_directory"] = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_emp500_lcms/processed/202507/" + final_mapped_raw_data_files["raw_data_file_short"].str.replace('.raw', '.corems')
final_mapped_raw_data_files["mass_spec_configuration_name"] = "LC-MS Metabolomics Method for EMP 500 Samples"
final_mapped_raw_data_files["chromat_configuration_name"] = "LC-MS Chromatography Configuration for EMP 500 Samples"
final_mapped_raw_data_files["execution_resource"] = "EMSL-RZR"

##### Save the final mapped raw data files to a CSV file ============
output_file = "_emp_500_lcms_metabolomics/mapped_raw_data_files.csv"
final_mapped_raw_data_files.to_csv(output_file, index=False)

"""
#TODO:
Add the following columns to the final mapped raw data files:
material_processing_type = "unknown"
instrument_used = "TBD"
instrument_analysis_start_date = "TBD"
instrument_analysis_end_date = "TBD"
execution_resource = "EMSL-RZR"
"""
