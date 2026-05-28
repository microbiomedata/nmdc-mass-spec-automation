import pandas as pd
import yaml
import re

# Define file paths
BIOSAMPLE_ATTRIBUTES_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/miesel_11_8bjf2432_gcms_metab/metadata/biosample_attributes.csv'
RAW_FILES_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/miesel_11_8bjf2432_gcms_metab/metadata/downloaded_files.csv'
YAML_OUTLINE_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/miesel_11_8bjf2432_gcms_metab/protocol_info/llm_generated_protocol_outline.yaml'
OUTPUT_CSV_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/miesel_11_8bjf2432_gcms_metab/metadata/llm_biosample_raw_file_mapper.csv'

def standardize_name_for_match(name, is_filename=False):
    """
    Standardizes a biosample name or raw filename for robust matching.
    Handles space to underscore, '+' to 'plus', and removes '.cdf' for filenames.
    """
    processed_name = name
    if is_filename:
        # Remove .cdf extension
        processed_name = processed_name.replace('.cdf', '')
    
    # Replace spaces with underscores
    processed_name = processed_name.replace(' ', '_')
    
    # Replace '+' with 'plus' for consistency with filenames (e.g., MST+5 becomes MSTplus5)
    processed_name = processed_name.replace('+', 'plus')
    
    # Convert to lowercase
    processed_name = processed_name.lower()
    
    return processed_name

def main():
    # Load biosample attributes
    biosamples_df = pd.read_csv(BIOSAMPLE_ATTRIBUTES_PATH)
    
    # Create a mapping for EMSL biosamples: {standardized_biosample_name_from_file_like_string: (biosample_id, original_biosample_name)}
    eml_biosamples_map = {}
    for _, row in biosamples_df.iterrows():
        original_bs_name = row['name']
        
        # Only process biosamples that have 'EMSL' in their name as per instructions
        if 'EMSL' in original_bs_name:
            # Standardize the biosample name for matching with raw filenames
            # The raw file name structure implies that the part after 'EMSL_X_' directly matches
            # the biosample 'name' as it appears in biosample_attributes.csv after standardizing.
            # Example: biosample name "EMSL 17 NIWO_A_PRE_NotGr_NA_NA" maps to raw file "EMSL_17_NIWO_A_PRE_NotGr_NA_NA.cdf"
            standardized_bs_name = standardize_name_for_match(original_bs_name)
            eml_biosamples_map[standardized_bs_name] = (row['id'], original_bs_name)
    
    # Load raw file names
    raw_files_df = pd.read_csv(RAW_FILES_PATH)
    
    # Fixed values for the GCMS protocol from the provided YAML outline
    material_processing_protocol_id = 'ALL' 
    processed_sample_placeholder_for_gcms = 'ProcessedSample4_GCMS' 

    # Prepare list for output data
    output_data = []

    # Iterate through raw files and perform mapping
    for _, rf_row in raw_files_df.iterrows():
        raw_file_name = rf_row['raw_data_file_short']
        
        # Initialize output row data with None for optional fields
        row_output = {
            'raw_data_identifier': raw_file_name,
            'biosample_id': None,
            'biosample_name': None,
            'processedsample_placeholder': None,
            'material_processing_protocol_id': None,
            'match_confidence': None
        }

        # Standardize the raw file name for matching
        standardized_rf_name = standardize_name_for_match(raw_file_name, is_filename=True)

        if raw_file_name.startswith('GCMS_Blank'):
            # Blank files: no biosample mapping, no processed sample, no confidence.
            # All None values are acceptable according to validation rules for this case.
            pass 
        elif raw_file_name.startswith('GCMS_FAMEs'):
            # Calibrant files: explicitly do not map to a biosample, but require specific metadata.
            row_output['match_confidence'] = 'calibrant'
            row_output['processedsample_placeholder'] = processed_sample_placeholder_for_gcms
            row_output['material_processing_protocol_id'] = material_processing_protocol_id
        else:
            # Attempt to find a matching EMSL biosample for other raw files
            if standardized_rf_name in eml_biosamples_map:
                bs_id, bs_name = eml_biosamples_map[standardized_rf_name]
                row_output['biosample_id'] = bs_id
                row_output['biosample_name'] = bs_name
                row_output['processedsample_placeholder'] = processed_sample_placeholder_for_gcms
                row_output['material_processing_protocol_id'] = material_processing_protocol_id
                row_output['match_confidence'] = 'high'
            # If an EMSL raw file does not match any biosample in eml_biosamples_map,
            # its mapping fields (biosample_id, biosample_name, etc.) will remain None,
            # and match_confidence will remain None. This is also acceptable.

        output_data.append(row_output)
    
    # Create output DataFrame and save to CSV
    output_df = pd.DataFrame(output_data)
    output_df.to_csv(OUTPUT_CSV_PATH, index=False)

if __name__ == '__main__':
    main()