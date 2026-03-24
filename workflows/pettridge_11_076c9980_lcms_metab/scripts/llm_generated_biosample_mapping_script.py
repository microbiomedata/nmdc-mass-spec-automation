import pandas as pd
import re
import yaml

# --- File Paths (as specified in the prompt) ---
BIOSAMPLE_ATTRIBUTES_PATH = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/pettridge_11_076c9980_lcms_metab/metadata/biosample_attributes.csv"
DOWNLOADED_FILES_PATH = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/pettridge_11_076c9980_lcms_metab/metadata/downloaded_files.csv"
PROTOCOL_YAML_PATH = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/pettridge_11_076c9980_lcms_metab/protocol_info/llm_generated_protocol_outline.yaml"
OUTPUT_CSV_PATH = "/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/pettridge_11_076c9980_lcms_metab/metadata/llm_biosample_raw_file_mapper.csv"

# --- Load Data ---
biosamples_df = pd.read_csv(BIOSAMPLE_ATTRIBUTES_PATH)
raw_files_df = pd.read_csv(DOWNLOADED_FILES_PATH)

with open(PROTOCOL_YAML_PATH, 'r') as f:
    protocol_yaml = yaml.safe_load(f)

# Extract relevant protocol info
# Assuming there's only one top-level protocol in the YAML for this specific mapping task
protocol_name = list(protocol_yaml.keys())[0]

# The prompt's example YAML for LCMS_Metabolomics_Soil_Extraction indicates ProcessedSample3 as the DG input
processed_sample_placeholder = f"ProcessedSample3_{protocol_name}" 

# Prepare biosample data for easier lookup
biosample_lookup = {}
for _, row in biosamples_df.iterrows():
    biosample_id = row['id']
    biosample_name = row['name']
    
    # Exclude biosamples with "GRE.SIPMG." as per instructions
    if "GRE.SIPMG." in biosample_name:
        continue

    # Extract the unique identifier part after ' - '
    match_biosample_name_suffix = re.search(r'- (.*)$', biosample_name)
    if match_biosample_name_suffix:
        key_part_raw = match_biosample_name_suffix.group(1) # e.g., 'flux8day.12C.oxic.44.000.176'
        
        # The last number is the key ID
        id_suffix_match = re.search(r'\.(\d+)$', key_part_raw)
        if id_suffix_match:
            id_suffix = id_suffix_match.group(1) # e.g., '176'
            
            # The part before the ID suffix is for conditions comparison
            conditions_part_bs = key_part_raw[:-len(id_suffix)-1] # e.g., 'flux8day.12C.oxic.44.000'
            
            # Store with biosample_id, full name, and conditions part for comparison
            biosample_lookup[id_suffix] = {
                'id': biosample_id,
                'name': biosample_name,
                'conditions_part': conditions_part_bs
            }

# List to store results
results = []

# Process each raw file
for _, row in raw_files_df.iterrows():
    raw_file_name = row['file_name']
    
    current_match_confidence = "" # Default to empty for unmatched files
    current_biosample_id = ""
    current_biosample_name = ""
    current_processedsample_placeholder = ""
    current_material_processing_protocol_id = ""
    
    # Check for specific exclusion strings first
    if "13C-plant" in raw_file_name or "unk" in raw_file_name or "ExCtrl" in raw_file_name:
        results.append({
            'raw_data_identifier': raw_file_name,
            'biosample_id': '',
            'biosample_name': '',
            'match_confidence': '', # Empty for excluded files
            'processedsample_placeholder': '',
            'material_processing_protocol_id': ''
        })
        continue # Skip to the next file

    # Extract the GRE-MB-ID and conditions part for matching biosample
    file_match_pattern = r'_GRE-MB-(\d+)_([a-zA-Z0-9\-]+)'
    file_match_part = re.search(file_match_pattern, raw_file_name)
    
    if file_match_part:
        file_id = file_match_part.group(1) # e.g., '176'
        file_conditions_raw = file_match_part.group(2) # e.g., 'flux8day-12C-oxic-44-000'
        
        # Apply the explicit exclusion rule for "13C" not part of "13CFlux" within the conditions part
        # "Any raw data files with "13C" in the name (excluding the 13CFlux label that is more an indication of the study) should not map to a biosample."
        # This checks for "13C" in `file_conditions_raw` that is NOT immediately followed by "Flux".
        if re.search(r'13C(?!Flux)', file_conditions_raw):
             results.append({
                'raw_data_identifier': raw_file_name,
                'biosample_id': '',
                'biosample_name': '',
                'match_confidence': '', # Empty for excluded files
                'processedsample_placeholder': '',
                'material_processing_protocol_id': ''
            })
             continue # Skip to the next file

        # Perform the primary matching if not excluded
        if file_id in biosample_lookup:
            bs_info = biosample_lookup[file_id]
            bs_conditions_part = bs_info['conditions_part'] # e.g., 'flux8day.12C.oxic.44.000'

            # Convert file_conditions_raw to the biosample conditions format (replace '-' with '.')
            file_conditions_for_comparison = file_conditions_raw.replace('-', '.')
            
            # The prompt indicates a specific rule for matching:
            # "If the number after GRE-MB- does not match the number at the end of the biosample name, that is not a match."
            # My current logic already handles this by using `file_id` as a direct key into `biosample_lookup`.
            # "Only report high matches, there should only be a few, one positive and one negative per biosample that is mapped."
            # This implies if a match is found based on conditions, it should be 'high'.
            if file_conditions_for_comparison == bs_conditions_part:
                current_match_confidence = "high"
                current_biosample_id = bs_info['id']
                current_biosample_name = bs_info['name']
                current_processedsample_placeholder = processed_sample_placeholder
                current_material_processing_protocol_id = protocol_name
    
    results.append({
        'raw_data_identifier': raw_file_name,
        'biosample_id': current_biosample_id,
        'biosample_name': current_biosample_name,
        'match_confidence': current_match_confidence,
        'processedsample_placeholder': current_processedsample_placeholder,
        'material_processing_protocol_id': current_material_processing_protocol_id
    })

# Create DataFrame and save to CSV
output_df = pd.DataFrame(results)

# Define the exact required columns in the correct order
final_columns = [
    'raw_data_identifier',
    'biosample_id',
    'biosample_name',
    'match_confidence',
    'processedsample_placeholder',
    'material_processing_protocol_id'
]

output_df = output_df[final_columns]

output_df.to_csv(OUTPUT_CSV_PATH, index=False)

print(f"Mapping saved to {OUTPUT_CSV_PATH}")