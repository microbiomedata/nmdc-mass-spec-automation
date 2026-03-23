import pandas as pd
import yaml
import re

# File paths (as specified in the prompt)
BIOSAMPLE_ATTRIBUTES_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/example_lcms_lipids/metadata/biosample_attributes.csv'
DOWNLOADED_FILES_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/example_lcms_lipids/metadata/downloaded_files.csv'
PROTOCOL_YAML_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/example_lcms_lipids/protocol_info/llm_generated_protocol_outline.yaml'
OUTPUT_CSV_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/example_lcms_lipids/metadata/llm_biosample_raw_file_mapper.csv'

# --- 1. Load Data ---
biosamples_df = pd.read_csv(BIOSAMPLE_ATTRIBUTES_PATH)
raw_files_df = pd.read_csv(DOWNLOADED_FILES_PATH)

with open(PROTOCOL_YAML_PATH, 'r') as f:
    protocol_yaml = yaml.safe_load(f)

# --- 2. Prepare Biosample Lookup ---
# biosample_id_by_exact_s: Maps 'SXX' to (nmdc_id, original_name) for biosamples named exactly 'SXX'
# biosample_id_by_s_d: Maps 'SXX_DYY' to (nmdc_id, original_name) for biosamples containing 'SXX_DYY'
biosample_lookup = {} # Store nmdc_id and original_name for all permutations

for index, row in biosamples_df.iterrows():
    nmdc_id = row['id']
    name = row['name']
    
    # Store for direct matching
    biosample_lookup[name] = (nmdc_id, name)

    s_match = re.search(r'(S\d+)', name)
    d_match = re.search(r'(D\d+)', name)
    
    s_id_str = s_match.group(1) if s_match else None
    d_id_str = d_match.group(1) if d_match else None
    
    if s_id_str:
        # Key for SXX_DYY patterns
        if d_id_str:
            key_s_d = f"{s_id_str}_{d_id_str}"
            biosample_lookup[key_s_d] = (nmdc_id, name)
        
        # Key for SXX only patterns (especially for files where DYY is not explicitly in filename)
        # We prioritize the full name match, but this can serve as a fallback or a simpler match
        biosample_lookup[s_id_str] = (nmdc_id, name)

# --- 3. Prepare Protocol and Processed Sample Lookup from YAML ---
# This mapping assumes final DG input processed samples are 'ProcessedSample3' for both protocols
protocol_mapping = {
    'HILICZ': {
        'protocol_id': 'polar_metabolites_extraction',
        'processed_sample_placeholder': 'ProcessedSample3_polar_metabolites_extraction'
    },
    'C18': {
        'protocol_id': 'nonpolar_metabolites_extraction',
        'processed_sample_placeholder': 'ProcessedSample3_nonpolar_metabolites_extraction'
    }
}

# --- 4. Process Each Raw File ---
output_records = []
for index, row in raw_files_df.iterrows():
    file_name = row['file_name']
    
    record = {
        'biosample_id': '',
        'biosample_name': '', # Added this column
        'raw_data_identifier': file_name,
        'material_processing_protocol_id': '',
        'processedsample_placeholder': '',
        'match_confidence': ''
    }

    # Check for pilot files (unmatched)
    if "pilot" in file_name.lower():
        record['match_confidence'] = 'unmatched'
        output_records.append(record)
        continue

    # Check for calibrant files (QC)
    if "_QC_" in file_name:
        record['match_confidence'] = 'calibrant'
        output_records.append(record)
        continue

    # Regular sample mapping
    matched_biosample_info = None # (nmdc_id, original_name)
    
    # Extract SXX and optional DYY from filename pattern like _S17-D45_A_ or _S16_
    s_d_match_from_file = re.search(r'_S(\d+)(?:-D(\d+))?', file_name) 
    
    if s_d_match_from_file:
        s_id_from_file = f"S{s_d_match_from_file.group(1)}"
        d_id_from_file = f"D{s_d_match_from_file.group(2)}" if s_d_match_from_file.group(2) else None
        
        # Try to match the most specific pattern first (SXX_DYY)
        if d_id_from_file:
            key_specific = f"{s_id_from_file}_{d_id_from_file}"
            if key_specific in biosample_lookup:
                matched_biosample_info = biosample_lookup[key_specific]
        
        # If no specific SXX_DYY match, try SXX pattern
        if not matched_biosample_info and s_id_from_file in biosample_lookup:
            matched_biosample_info = biosample_lookup[s_id_from_file]
        
        # If a biosample ID was found for the file
        if matched_biosample_info:
            record['biosample_id'] = matched_biosample_info[0]
            record['biosample_name'] = matched_biosample_info[1] # Populate biosample_name
            record['match_confidence'] = 'high' # Assume high confidence if pattern matches an existing biosample

            # Determine protocol and processed sample based on extraction type (HILICZ/C18)
            if 'HILICZ' in file_name:
                record['material_processing_protocol_id'] = protocol_mapping['HILICZ']['protocol_id']
                record['processedsample_placeholder'] = protocol_mapping['HILICZ']['processed_sample_placeholder']
            elif 'C18' in file_name:
                record['material_processing_protocol_id'] = protocol_mapping['C18']['protocol_id']
                record['processedsample_placeholder'] = protocol_mapping['C18']['processed_sample_placeholder']
            # If no HILICZ or C18 and biosample was matched, protocol/processed_sample remain empty
            
    output_records.append(record)

# --- 5. Assemble Output DataFrame and Save ---
output_df = pd.DataFrame(output_records)

# Ensure correct column order as per validation requirements
final_columns = [
    'biosample_id', 
    'biosample_name', # Added to final columns
    'raw_data_identifier', 
    'material_processing_protocol_id', 
    'processedsample_placeholder', 
    'match_confidence'
]
output_df = output_df[final_columns]

output_df.to_csv(OUTPUT_CSV_PATH, index=False)