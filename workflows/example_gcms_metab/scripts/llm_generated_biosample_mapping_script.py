import pandas as pd
import yaml
import re

def map_raw_files_to_biosamples():
    # Define file paths
    biosample_attributes_path = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/example_gcms_metab/metadata/biosample_attributes.csv"
    raw_files_path = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/example_gcms_metab/metadata/downloaded_files.csv"
    yaml_path = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/example_gcms_metab/protocol_info/llm_generated_protocol_outline.yaml"
    output_csv_path = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/example_gcms_metab/metadata/llm_biosample_raw_file_mapper.csv"

    # Load data
    biosamples_df = pd.read_csv(biosample_attributes_path)
    raw_files_df = pd.read_csv(raw_files_path)
    
    with open(yaml_path, 'r') as f:
        protocol_yaml = yaml.safe_load(f)

    # Prepare biosample mapping: name to id
    biosample_name_to_id = dict(zip(biosamples_df['name'], biosamples_df['id']))

    # Extract the main protocol name (top-level key in the YAML)
    if not protocol_yaml or not isinstance(protocol_yaml, dict):
        raise ValueError("YAML is empty or not a dictionary.")
    protocol_name_yaml_key = list(protocol_yaml.keys())[0] 
    protocol_data = protocol_yaml[protocol_name_yaml_key]

    # Determine the final processed sample key from the YAML structure
    final_processed_sample_key = None
    
    if 'processedsamples' in protocol_data and protocol_data['processedsamples']:
        last_processed_sample_entry = protocol_data['processedsamples'][-1]
        
        # The last_processed_sample_entry should be a dictionary with a single key
        # representing the ProcessedSample ID (e.g., 'ProcessedSample4_gcms_metabolomics')
        if isinstance(last_processed_sample_entry, dict) and len(last_processed_sample_entry) == 1:
            final_processed_sample_key = list(last_processed_sample_entry.keys())[0]
            
            # Additional check for robustness, though not strictly needed for the direct output fields
            processed_sample_obj_wrapper = last_processed_sample_entry[final_processed_sample_key]
            if not (isinstance(processed_sample_obj_wrapper, dict) and 'ProcessedSample' in processed_sample_obj_wrapper):
                raise ValueError(f"YAML parsing error: Expected 'ProcessedSample' object wrapper for key '{final_processed_sample_key}'. Found: {processed_sample_obj_wrapper}")
            
        else:
            raise ValueError(f"YAML parsing error: Last entry in 'processedsamples' is not a single-key dictionary. Found: {last_processed_sample_entry}")
    else:
        raise ValueError("YAML parsing error: 'processedsamples' section is missing or empty.")

    if not final_processed_sample_key:
        raise ValueError("Critical: Final processed sample key could not be extracted from YAML.")

    # Prepare output list
    output_records = []

    # Process each raw file
    for _, row in raw_files_df.iterrows():
        raw_data_identifier = row['raw_data_file_short']
        
        # Initialize default values for output columns
        biosample_id = None
        biosample_name = None # Added biosample_name column
        processedsample_placeholder = None
        material_processing_protocol_id = None
        match_confidence = None

        # 1. Check for calibrant files
        if "FAMEs" in raw_data_identifier: 
            match_confidence = 'calibrant'
            # For calibrants, biosample_id, biosample_name, processedsample_placeholder, material_processing_protocol_id remain None
        else:
            # 2. Attempt to match to biosamples
            biosample_name_prefix = None
            
            # Regex to extract relevant parts (C/H, number (with optional B), AB, M/O)
            match = re.search(r'_(C|H)_(\d+B?)_AB_(M|O)', raw_data_identifier)
            if match:
                part1 = match.group(1) # 'C' or 'H'
                part2 = match.group(2) # Number, e.g., '12', '2B'
                part3 = match.group(3) # 'M' or 'O'
                
                # Special handling for '2B' in filename which corresponds to '2' in biosample name
                if part2 == '2B':
                    part2 = '2'

                # Construct potential biosample names as they appear in biosample_attributes.csv
                potential_biosample_name_base = f"BW-{part1}-{part2}-{part3}"
                potential_biosample_name_inc = f"Inc-{potential_biosample_name_base}" # For Inc- samples

                # Prioritize matching with 'Inc-' prefix for 'O' samples
                if part3 == 'O' and potential_biosample_name_inc in biosample_name_to_id:
                    biosample_name_prefix = potential_biosample_name_inc
                elif potential_biosample_name_base in biosample_name_to_id:
                    biosample_name_prefix = potential_biosample_name_base
            
            if biosample_name_prefix:
                # If a biosample name prefix is successfully derived and found in the biosample list
                biosample_id = biosample_name_to_id.get(biosample_name_prefix)
                if biosample_id: # Should always be true if biosample_name_prefix was found
                    biosample_name = biosample_name_prefix # Set biosample_name
                    processedsample_placeholder = final_processed_sample_key
                    material_processing_protocol_id = protocol_name_yaml_key
                    match_confidence = 'high' # Set confidence for successfully mapped samples
                # else: This else block is theoretically unreachable if biosample_name_prefix was found in biosample_name_to_id
            # else: If no biosample_name_prefix was derived or it didn't match, biosample_id and other fields remain None, match_confidence remains None

        output_records.append({
            "raw_data_identifier": raw_data_identifier,
            "biosample_id": biosample_id,
            "biosample_name": biosample_name, # Added to output record
            "processedsample_placeholder": processedsample_placeholder,
            "material_processing_protocol_id": material_processing_protocol_id,
            "match_confidence": match_confidence
        })

    # Create DataFrame and save
    output_df = pd.DataFrame(output_records)
    output_df.to_csv(output_csv_path, index=False)
    print(f"Mapping saved to {output_csv_path}")

# Call the function to execute the script
map_raw_files_to_biosamples()