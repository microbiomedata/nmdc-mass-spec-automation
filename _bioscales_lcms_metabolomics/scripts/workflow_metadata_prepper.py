# This script is to prepare the metadata for the workflow metadata generation.
# Input is _bioscales_lcms_metabolomics/mapped_files_to_biosamples.csv ; we will add and adjust columns as needed
import os
import pandas as pd

def add_raw_data_file(mapped_df, raw_data_dir):
    # Add raw_data_file column by prefixing with the raw_data_dir
    mapped_df['raw_data_file'] = raw_data_dir + mapped_df['raw_data_file_short']
    return mapped_df

def add_processed_data_directory(mapped_df, processed_data_dir):
    # Add processed_data_directory column by prefixing with the processed_data_dir
    # remove any trailing .mzML (case-insensitive) before appending .corems
    mapped_df['processed_data_directory'] = (
        processed_data_dir
        + mapped_df['raw_data_file_short'].str.replace(r'(?i)\.mzml$', '', regex=True)
        + '.corems'
    )
    return mapped_df

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

def separate_by_config(mapped_df):
    dfs = {}
    # Standardize mass_spec_configuration_name for separating
    # Remove everything after @ sign
    mapped_df['mass_spec_configuration_name_short'] = mapped_df['mass_spec_configuration_name'].str.split('@').str[0].str.strip()
    # Separate out into 4 dataframes based on mass_spec_configuration_name and chromat_configuration_name, remove rows that are not present in the processed data directory
    config_combinations = mapped_df[['mass_spec_configuration_name_short', 'chromat_configuration_name']].drop_duplicates()
    for _, row in config_combinations.iterrows():
        subset_df = pd.DataFrame()
        mass_spec = row['mass_spec_configuration_name_short']
        chromat = row['chromat_configuration_name']
        subset_df = mapped_df[
            (mapped_df['mass_spec_configuration_name_short'] == mass_spec) & 
            (mapped_df['chromat_configuration_name'] == chromat) 
        ]
        if len(subset_df) == 0:
            print(f"No valid rows for configuration {mass_spec} and {chromat}, skipping.")
            continue
        # Clean up mass_spec string for filename
        mass_spec_clean = (
            mass_spec.replace(' ', '_')
            .replace('-', '_')
            .replace('/', '')
            .replace('(', '')
            .replace(')', '')
            .replace(',', '')
        )
        chromat_clean = (
            chromat.replace(' ', '_')
            .replace('-', '_')
            .replace('/', '')
            .replace('(', '')
            .replace(')', '')
            .replace(',', '')
        )
        dfs[f'{mass_spec_clean}_{chromat_clean}'] = subset_df
    # for each of the dfs, remove the mass_spec_configuration_name_short column and reset index
    for key, df in dfs.items():
        df = df.drop(columns=['mass_spec_configuration_name_short']).reset_index(drop=True)
        dfs[key] = df

    return dfs

if __name__ == "__main__":
    final_dir = '_bioscales_lcms_metabolomics/metadata/metadata_gen_input_csvs/'
    # Make this dir, empty if it exists
    os.makedirs(final_dir, exist_ok=True)
    for f in os.listdir(final_dir):
        os.remove(os.path.join(final_dir, f))
    # Read in the mapped files to biosamples csv
    mapped_df = pd.read_csv('_bioscales_lcms_metabolomics/mapped_files_to_biosamples.csv')
    mapped_df = add_instrument_times(
        mapped_df=mapped_df, 
        file_info_file='_bioscales_lcms_metabolomics/bioscales_file_info/raw_file_info_20250806_170035.csv',
        serial_numbers_to_remove=['Exactive Series slot #1']
    )
    mapped_df = add_raw_data_file(
        mapped_df, 
        raw_data_dir='/Volumes/LaCie/nmdc_data/_bioscales_lcms/raw/'
    )
    mapped_df = add_processed_data_directory(
        mapped_df, 
        processed_data_dir='/Volumes/LaCie/nmdc_data/_bioscales_lcms/processed_20251013/'
    )
    mapped_df['processing_institution_workflow'] = "NMDC"
    mapped_df['processing_institution_generation'] = "JGI"
    mapped_df['raw_data_url'] = mapped_df['url']
    mapped_df['sample_id'] = mapped_df['biosample_id']

    # Fix instrument_used values - if instrument_used == QExactHF03, change it to "Thermo Orbitrap Q-Exactive"
    mapped_df['instrument_used'] = mapped_df['instrument_used'].replace({'QExactHF03': 'Thermo Orbitrap Q-Exactive'})

    # Reorder for readability
    final_columns = [
        'sample_id',"biosample.associated_studies", "raw_data_file", 'processed_data_directory', 'mass_spec_configuration_name',
        'chromat_configuration_name', 'instrument_used', 'processing_institution_workflow', 'processing_institution_generation',
        'instrument_analysis_end_date', 'instrument_instance_specifier',
        'raw_data_url'
    ]

    # Loop through each of the final_columns and if it is not in mapped_df raise an error
    for col in final_columns:
        if col not in mapped_df.columns:
            raise ValueError(f"Column {col} is missing from the final DataFrame.")
    
        'sample_id',"biosample.associated_studies", "raw_data_file", 'processed_data_directory', 'mass_spec_configuration_name',
    # Check raw_data_file existence and processed_data_directory validity,
    # move any rows with missing files to missing_files_df instead of raising errors.
    def _processed_dir_valid(d):
        try:
            if not isinstance(d, str) or not os.path.exists(d):
                return False
            files = os.listdir(d)
            toml = any(f.lower().endswith('.toml') for f in files)
            csv = any(f.lower().endswith('.csv') for f in files)
            hdf5 = any(f.lower().endswith('.hdf5') for f in files)
            return toml and csv and hdf5
        except Exception:
            return False

    raw_exists_mask = mapped_df['raw_data_file'].apply(lambda p: isinstance(p, str) and os.path.exists(p))
    proc_ok_mask = mapped_df['processed_data_directory'].apply(_processed_dir_valid)

    ok_mask = raw_exists_mask & proc_ok_mask

    # Rows with any missing/raw/processed files go to missing_files_df
    missing_files_df = mapped_df.loc[~ok_mask].copy().reset_index(drop=True)
    mapped_df = mapped_df.loc[ok_mask].copy().reset_index(drop=True)

    print(f"Moved {len(missing_files_df)} rows with missing files to missing_files_df; {len(mapped_df)} rows remain for processing.")
    if len(missing_files_df) > 0:
        print("Examples of missing entries (raw_data_file, processed_data_directory):")
        print(missing_files_df[['raw_data_file', 'processed_data_directory']].head(10))


    # Separate out into 4 dataframes based on mass_spec_configuration_name and chromat_configuration_name, remove rows that are not present in the processed data directory
    # and write out four separate csv files
    mapped_dfs = separate_by_config(mapped_df)
    for key, df in mapped_dfs.items():
        df = df[final_columns]
        #write this into final_dir with name {key}_metadata.csv
        df.to_csv(os.path.join(final_dir, f'{key}_metadata.csv'), index=False)
        print(f"Wrote {key}_metadata.csv with {len(df)} rows.")
        
    print("All final columns are present.")
