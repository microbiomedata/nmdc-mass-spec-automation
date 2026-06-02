import pandas as pd
import yaml
import re

def generate_mapping_script():
    biosample_file = "/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/example_di_nom/metadata/biosample_attributes.csv"
    raw_files_file = "/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/example_di_nom/metadata/downloaded_files.csv"
    yaml_file = "/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/example_di_nom/protocol_info/llm_generated_protocol_outline.yaml"
    output_file = "/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/example_di_nom/metadata/llm_biosample_raw_file_mapper.csv"

    # --- 1. Load Biosamples ---
    biosamples_df = pd.read_csv(biosample_file)
    biosample_id_to_name = {}
    biosample_fragment_to_id = {}
    biosample_fragment_to_name = {}

    # Regex to extract common identifier from biosample name, like "13_KONZ_A1_PRE"
    # This pattern targets biosample names that include 'EMSL <number> <SITE_ID>_<STATE>_PRE/POST' structure
    biosample_name_pattern = re.compile(r"EMSL (\d+) ([A-Z]+(?:_[A-Za-z0-9]+)*)_(PRE|POST)")

    for index, row in biosamples_df.iterrows():
        biosample_id = row['id']
        biosample_name = row['name']
        biosample_id_to_name[biosample_id] = biosample_name

        match = biosample_name_pattern.search(biosample_name)
        if match:
            # Construct a consistent fragment, e.g., "13_KONZ_A1_PRE"
            fragment = f"{match.group(1)}_{match.group(2)}_{match.group(3)}"
            biosample_fragment_to_id[fragment] = biosample_id
            biosample_fragment_to_name[fragment] = biosample_name

    # --- 2. Parse Material Processing YAML ---
    with open(yaml_file, 'r') as f:
        protocol_outline = yaml.safe_load(f)

    # Stores processed sample info. Key: (protocol_name, extract_type_key),
    # Value: {'processed_sample_placeholder': ps_id, 'processed_sample_name_template': ps_name, 'material_processing_protocol_id': protocol_name}
    processed_sample_map = {}

    for protocol_name, protocol_data in protocol_outline.items():
        if 'processedsamples' in protocol_data:
            for ps_entry in protocol_data['processedsamples']:
                ps_id = list(ps_entry.keys())[0] # e.g., 'ProcessedSample5_NOM'
                ps_info = ps_entry[ps_id]['ProcessedSample']
                ps_description = ps_info.get('description', '').lower()
                ps_name = ps_info.get('name', '')

                extract_type_key = None
                # Determine a canonical type key for the processed sample based on description
                # This needs to be precise to match raw file patterns later
                if "nom from water extract and solid phase extraction" in ps_description: # e.g., from 'sequential_extraction' protocol
                    extract_type_key = 'water_SPE'
                elif "nom from methanol extract and solid phase extraction" in ps_description: # e.g., from 'sequential_extraction' protocol
                    extract_type_key = 'methanol_SPE'
                elif "nom from cold water and solid phase extraction" in ps_description: # e.g., from 'NOM' protocol
                    extract_type_key = 'cold_water_SPE'
                elif "nom from hot water and solid phase extraction" in ps_description: # e.g., from 'NOM' protocol
                    extract_type_key = 'hot_water_SPE'
                # Add other extract types as needed

                if extract_type_key:
                    processed_sample_map[(protocol_name, extract_type_key)] = {
                        'processed_sample_placeholder': ps_id,
                        'processed_sample_name_template': ps_name, # Template with <Biosample>
                        'material_processing_protocol_id': protocol_name
                    }

    # --- 3. Process Raw Files ---
    raw_files_df = pd.read_csv(raw_files_file)
    mapping_results = []

    # Regex to extract biosample-identifying fragment from raw file name
    # e.g., "Miesel_60009_13_KONZ_A1_PRE_H2O_SPE_..." -> "13_KONZ_A1_PRE"
    raw_file_fragment_pattern = re.compile(r"_(\d+)_([A-Z]+(?:_[A-Za-z0-9]+)*)_(PRE|POST)")

    for index, row in raw_files_df.iterrows():
        raw_data_identifier = row['raw_data_file_short']
        biosample_id = None
        biosample_name = None
        processed_sample_placeholder_val = ""
        material_processing_protocol_id_val = ""
        match_confidence = "unmatched" # Default to unmatched

        # Handle calibrant files first
        if "SRFA" in raw_data_identifier:
            match_confidence = "calibrant"
            # Calibrants do not map to biosamples or processed samples
        else:
            match = raw_file_fragment_pattern.search(raw_data_identifier)
            if match:
                found_fragment = f"{match.group(1)}_{match.group(2)}_{match.group(3)}"
                
                if found_fragment in biosample_fragment_to_id:
                    biosample_id = biosample_fragment_to_id[found_fragment]
                    biosample_name = biosample_fragment_to_name[found_fragment]
                    
                    # Determine raw file extract type and implied protocol from filename
                    file_extract_type_key = None
                    current_protocol_id = None
                    
                    if "_H2O_SPE" in raw_data_identifier:
                        file_extract_type_key = 'water_SPE'
                        # Assuming H2O_SPE from Miesel files maps to sequential_extraction's general water SPE
                        current_protocol_id = 'sequential_extraction' 
                    elif "_Met_SPE" in raw_data_identifier:
                        file_extract_type_key = 'methanol_SPE'
                        # Assuming Met_SPE from Miesel files maps to sequential_extraction's methanol SPE
                        current_protocol_id = 'sequential_extraction'
                    # Add conditions for 'cold_water_SPE' or 'hot_water_SPE' if raw files contain these specific terms

                    if current_protocol_id and file_extract_type_key:
                        if (current_protocol_id, file_extract_type_key) in processed_sample_map:
                            ps_details = processed_sample_map[(current_protocol_id, file_extract_type_key)]
                            
                            processed_sample_placeholder_val = ps_details['processed_sample_placeholder']
                            material_processing_protocol_id_val = ps_details['material_processing_protocol_id']
                            
                            # Match confidence is high if both biosample and specific processed sample are found
                            match_confidence = "high"
                        else:
                            # Biosample matched, but specific processed sample type from filename not found in the resolved protocol
                            match_confidence = "medium" 
                    else:
                        # Biosample matched, but the raw file extract type or implied protocol could not be determined
                        match_confidence = "low"
                else:
                    match_confidence = "unmatched" # Biosample fragment from raw file not found in biosample list
            else:
                match_confidence = "unmatched" # Raw file name pattern not recognized

        mapping_results.append({
            'raw_data_identifier': raw_data_identifier,
            'biosample_id': biosample_id,
            'biosample_name': biosample_name,
            'processedsample_placeholder': processed_sample_placeholder_val,
            'material_processing_protocol_id': material_processing_protocol_id_val,
            'match_confidence': match_confidence
        })

    # --- 4. Write Output CSV ---
    output_df = pd.DataFrame(mapping_results)
    output_df.to_csv(output_file, index=False)

generate_mapping_script()