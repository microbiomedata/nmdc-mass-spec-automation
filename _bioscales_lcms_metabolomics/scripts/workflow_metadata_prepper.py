# This script is to prepare the metadata for the workflow metadata generation.
# Input is _bioscales_lcms_metabolomics/mapped_files_to_biosamples.csv ; we will add and adjust columns as needed
import pandas as pd

def add_raw_data_file(mapped_df, raw_data_dir):
    # Add raw_data_file column by prefixing with the raw_data_dir
    mapped_df['raw_data_file'] = raw_data_dir + mapped_df['raw_data_file_short']
    return mapped_df

def add_processed_data_directory(mapped_df, processed_data_dir):
    # Add processed_data_directory column by prefixing with the processed_data_dir
    mapped_df['processed_data_directory'] = processed_data_dir + mapped_df['raw_data_file_short'] + '.corems/'
    return mapped_df

def add_instrument_times(mapped_df, file_info_file):
    # Read the file info file
    file_info_df = pd.read_csv(file_info_file)
    # Pull out just file_name (conver), instrument_analysis_start_date, instrument_analysis_end_date, and instrument_serial_number (convert this to instrument_instance_specifier)
    file_info_df['instrument_instance_specifier'] = file_info_df['instrument_serial_number'].astype(str)
    file_info_df = file_info_df[['raw_data_file_short', 'instrument_analysis_start_date', 'instrument_analysis_end_date', 'instrument_instance_specifier']]
    # Merge on raw_data_file_short to get start and end times
    merged_df = pd.merge(mapped_df, file_info_df[['raw_data_file_short', 'instrument_analysis_start_date', 'instrument_analysis_end_date']], on='raw_data_file_short', how='left')
    return merged_df

if __name__ == "__main__":
    mapped_df = pd.read_csv('_bioscales_lcms_metabolomics/mapped_files_to_biosamples.csv')
    file_info_df = pd.read_csv('_bioscales_lcms_metabolomics/bioscales_file_info/raw_file_info_20241030_1530.csv')



    mapped_df = add_raw_data_file(mapped_df, raw_data_dir='/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_bioscales_lcms/to_process/')
    mapped_df = add_processed_data_directory(mapped_df, processed_data_dir='/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_bioscales_lcms/processed_20251010/')
    mapped_df['processing_institution_workflow'] = "NMDC"
    mapped_df['processing_institution_generation'] = "JGI"

    # Reorder for readability
    final_columns = [
        'biosample_id',"biosample.associated_studies", "raw_data_file", 'processed_data_directory', 'mass_spec_configuration_name',
        'chromat_configuration_name', 'instrument_used', 'processing_institution_workflow', 'processing_institution_generation',
        'instrument_analysis_start_date', 'instrument_analysis_end_date', 'manifest_name', 'instrument_instance_specifier',
        'url'
    ]
    #TODO: finish this

    # Loop through each of the final_columns and if it is not in mapped_df raise an error
    for col in final_columns:
        if col not in mapped_df.columns:
            raise ValueError(f"Column {col} is missing from the final DataFrame.")
        
    print("All final columns are present.")
