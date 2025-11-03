"""
This script will use the NMDC API utilities package to get all the biosamples from
the EcoFAB project (nmdc:sty-11-ev70y104)

Then it will create a mapping from biosample id to raw data file name.
"""
import pandas as pd
import re
from nmdc_api_utilities.biosample_search import BiosampleSearch
import urllib.parse



def prep_biosample_df ():
    # Fetch all biosamples associated with the study "nmdc:sty-11-ev70y104" in the NMDC database
    biosample_search = BiosampleSearch()
    biosample_search.base_url = "https://api-backup.microbiomedata.org"
    biosamples = biosample_search.get_record_by_filter(
        filter='{"associated_studies":"nmdc:sty-11-ev70y104"}',
        max_page_size=1000,
        fields="id,name,samp_name",
        all_pages=True,
    )
    biosample_df = pd.DataFrame(biosamples)

    return biosample_df

def map_files_to_biosamples(ftp_df, biosample_df, mapping_df):
    """
    Map raw data files to biosamples based on filename patterns.
    """
    # Extract short raw data file names from ftp_df
    ftp_df['raw_data_file_short'] = ftp_df['ftp_location'].apply(lambda x: x.split('/')[-1])

    # Merge ftp_df with mapping_df to get biosample names
    merged_df = pd.merge(ftp_df, mapping_df, left_on='raw_data_file_short', right_on='raw_data_file_short', how='left')

    # Merge with biosample_df to get biosample IDs
    final_mapped_df = pd.merge(merged_df, biosample_df, left_on='biosample_name', right_on='name', how='left')

    # Drop rows where biosample_name is NaN (no mapping found)
    final_mapped_df = final_mapped_df[final_mapped_df['biosample_name'].notna()]

    # Select relevant columns
    mapped_df = final_mapped_df[['ftp_location', 'raw_data_file_short', 'id']]
    mapped_df = mapped_df.rename(columns={'id': 'biosample_id'})

    return mapped_df

def map_files_to_instruments(mapped_df):
    """
    Add a column called "instrument_used" to each, all mapped to "Thermo Orbitrap Exploris 120".
    """
    mapped_df['instrument_used'] ="Thermo Orbitrap Exploris 120"

    return mapped_df

def map_files_to_chromat_config(mapped_df):
    """
    Add a column called "chromat_configuration_name" to each row in the mapped_df DataFrame.
    Use "JGI/LBNL Metabolomics Standard LC Method - Polar HILIC-Z" for all files.
    """
    mapped_df['chromat_configuration_name'] = "JGI/LBNL Metabolomics Standard LC Method - Polar HILIC-Z"
    
    return mapped_df

def map_files_to_mass_spec_config(mapped_df):
    """
    Add a column called "mass_spec_configuration_name" to each row in the mapped_df DataFrame.
    Use "JGI/LBNL Standard Metabolomics Method, positive @20CE" for all files.

    """
    mapped_df['mass_spec_configuration_name'] = "JGI/LBNL Standard Metabolomics Method, positive @20CE"

    return mapped_df

def convert_ftp_to_https_massive(ftp_url):
    """
    Convert FTP URL to HTTPS URL for MASSIVE.
    """
    if ftp_url.startswith("ftp:/"):
        # ftp://massive-ftp.ucsd.edu/v02/MSV000083475/raw/RAW/PLATE1and2/1A10_1_12_mayer-34-s008-a04.raw
        # turns into
        # https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f.MSV000083475/raw/RAW/PLATE1and2/1A10_1_12_mayer-34-s008-a04.raw&forceDownload=true
        # Remove "ftp://massive-ftp.ucsd.edu" and replace with "https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file="
        https_stub = "https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f."
        # remove everything before MSV
        file_loc = "MSV" + ftp_url.split("MSV")[1]
        # url encode the ftp_path
        file_loc_encoded = urllib.parse.quote(file_loc, safe='')
        https_url = https_stub + file_loc_encoded + "&forceDownload=true"
        return https_url
    else:
        raise ValueError("FTP URL must start with 'ftp://massive-ftp.ucsd.edu'")

def add_instrument_times(mapped_df, file_info_file, serial_numbers_to_remove=None):
    # Read the file info file
    file_info_df = pd.read_csv(file_info_file)
    # Rename and standardize columns
    file_info_df['instrument_instance_specifier'] = file_info_df['instrument_serial_number'].astype(str)
    file_info_df['instrument_analysis_end_date'] = pd.to_datetime(file_info_df["write_time"]).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    file_info_df['raw_data_file_short'] = file_info_df['file_name']

    # For any serial numbers to remove, replace them with NaN
    if serial_numbers_to_remove:
        file_info_df['instrument_instance_specifier'] = file_info_df['instrument_instance_specifier'].replace(serial_numbers_to_remove, pd.NA)

    # Print out unique instrument_instance_specifier values and date range
    print("Unique instrument_instance_specifier values:", file_info_df['instrument_instance_specifier'].unique())
    print("Date range before conversion:", file_info_df['instrument_analysis_end_date'].min(), "to", file_info_df['instrument_analysis_end_date'].max())
    # Print date range before conversion
    # Keep only relevant columns
    file_info_df = file_info_df[['raw_data_file_short', 'instrument_analysis_end_date', 'instrument_instance_specifier']]
    # Merge on raw_data_file_short to get start and end times
    merged_df = pd.merge(mapped_df, file_info_df, on='raw_data_file_short', how='left')
    assert len(merged_df) == len(mapped_df), "Merge changed the number of rows!"

    return merged_df

def add_raw_data_file(mapped_df, raw_data_dir):
    # Add raw_data_file column by prefixing with the raw_data_dir
    mapped_df['raw_data_file'] = raw_data_dir + mapped_df['raw_data_file_short']
    return mapped_df

def add_processed_data_directory(mapped_df, processed_data_dir):
    # Add processed_data_directory column by prefixing with the processed_data_dir
    mapped_df['processed_data_directory'] = processed_data_dir + mapped_df['raw_data_file_short'] + '.corems/'
    return mapped_df

# Example usage:
if __name__ == "__main__":
    # Load your data
    # Actual file paths from MASSIVE
    ftp_df = pd.read_csv('_ecofab_lcms_11_ev70y104/ftp_locs.csv')
    # File names to biosample names mapping provided by the ECoFAB team
    mapping_df = pd.read_csv('_ecofab_lcms_11_ev70y104/nmdc_ecofab_lcms_mapping.csv')
    # Actual biosample data from NMDC
    biosample_df = prep_biosample_df()
    
    # Perform mapping
    print("\nPerforming mapping...")
    mapped_df = map_files_to_biosamples(ftp_df, biosample_df, mapping_df)
        
    # Map to instrument
    mapped_df = map_files_to_instruments(mapped_df)

    # Map to chromatographic configuration
    mapped_df = map_files_to_chromat_config(mapped_df)

    # Map to mass spectrometry configuration
    mapped_df = map_files_to_mass_spec_config(mapped_df)

    # Convert FTP URLs to HTTPS
    mapped_df["raw_data_url"] = mapped_df["ftp_location"].apply(convert_ftp_to_https_massive)

    # Add static metadata
    mapped_df["biosample.associated_studies"] = "['nmdc:sty-11-ev70y104']"
    mapped_df["material_processing_type"] = "unknown"
    mapped_df['processing_institution_workflow'] = "NMDC"
    mapped_df['processing_institution_generation'] = "JGI"

    # Add instrument times and instance specifiers
    mapped_df = add_instrument_times(
        mapped_df=mapped_df, 
        file_info_file='_ecofab_lcms_11_ev70y104/raw_file_info/raw_file_info_20251021_115348.csv',
        serial_numbers_to_remove=['Exactive Series slot #1']
    )

    # Add raw data file paths
    mapped_df = add_raw_data_file(
        mapped_df=mapped_df,
        raw_data_dir='Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_ecofab_lcms/to_process'
    )

    mapped_df = add_processed_data_directory(
        mapped_df, 
        processed_data_dir='/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_ecofab_lcms/processed_DATESTAMP/'
    )

    # Reorder for readability
    final_columns = [
        'biosample_id',"biosample.associated_studies", "raw_data_file", 'processed_data_directory', 'mass_spec_configuration_name',
        'chromat_configuration_name', 'instrument_used', 'processing_institution_workflow', 'processing_institution_generation',
        'instrument_analysis_end_date', 'instrument_instance_specifier',
        'raw_data_url'
    ]

    # Loop through each of the final_columns and if it is not in mapped_df raise an error
    for col in final_columns:
        if col not in mapped_df.columns:
            raise ValueError(f"Column {col} is missing from the final DataFrame.")

    # Save the final mapped DataFrame
    mapped_df[final_columns].to_csv('_ecofab_lcms_11_ev70y104/mapped_files_to_biosamples.csv', index=False)
    print(f"\nResults saved to '_ecofab_lcms_11_ev70y104/mapped_files_to_biosamples.csv'")