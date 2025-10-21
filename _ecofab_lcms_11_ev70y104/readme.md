# Study id
nmdc:sty-11-ev70y104

# Notes
Only HILIC POS data, same configuration as bioscales (no new configurations needed).
Sample mapping can be found here: https://figshare.com/articles/dataset/Raw_data/26401315?backTo=%2Fcollections%2FEcoFAB_2_0_Root_Microbiome_Ring_Trial%2F7373842&file=56719151 on (downloaded but git ignored due to size).
See tab called "NMDC-Seq-LCMS", columns starting at AA.

# Raw data
Massive id = MSV000095476
ftp link = ftp://massive-ftp.ucsd.edu/v08/MSV000095476/

First pull the ftp locations using the following command:
```bash
wget --spider -r -nd ftp://massive-ftp.ucsd.edu/v08/MSV000095476/raw -o _ecofab_lcms_11_ev70y104/massive_ftp_log.txt
```

Next download the raw files using the `_ecofab_lcms_11_ev70y104/scripts/file_puller.py`.
This will download files to `/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_ecofab_lcms/to_process` and write a file `_ecofab_lcms_11_ev70y104/ftp_locs.csv` with the ftp locations of the files that were downloaded and the raw file name.  Note that this will not download files that do not have "MS2" in the file name (this removes the QC files and those collected in only MS1).

# Inspect raw data files' metadata
To get start and end times, instrument details using `scripts/raw_file_info_pull_logger.py` script (writes `_ecofab_lcms_11_ev70y104/raw_file_info_TIMESTAMP.csv` and `_ecofab_lcms_11_ev70y104/processing_errors_TIMESTAMP.csv`).
Inspect the output csv files to make sure everything looks good and determine if we need to generate any new configurations or instrument records.

# Sample mapping and metadata generation input file prep

Files are mapped to exisiting NMDC biosamples, configurations, and instruments in the script `_bioscales_lcms_metabolomics/scripts/biosample_mapper.py`.
Output are written to `_bioscales_lcms_metabolomics/metadata/mapped_biosamples.json`.

# Generate wdl json files for processing the data
The script `_ecofab_lcms_11_ev70y104/scripts/wdl_json_generator.py` will generate the wdl json files needed to run the data processing.
Output files are written to `_ecofab_lcms_11_ev70y104/wdl_jsons/`.  Note only HILIC POS data are included, so only one batch is needed.

These use the configurations and mapped biosamples generated in the previous steps.

# Shell script to run the batches: `_bioscales_lcms_metabolomics/scripts/bioscales_metams_wdl_runner.sh`
`bash /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/scripts/bioscales_metams_wdl_runner.sh`