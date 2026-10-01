import pandas as pd
import yaml
import re

# File paths (MUST use these exact paths)
BIOSAMPLE_ATTRIBUTES_PATH = '/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom_metadata/metadata/biosample_attributes.csv'
RAW_FILES_PATH = '/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom_metadata/metadata/downloaded_files.csv'
PROTOCOL_YAML_PATH = '/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom_metadata/protocol_info/llm_generated_protocol_outline.yaml'
OUTPUT_CSV_PATH = '/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom_metadata/metadata/llm_biosample_raw_file_mapper.csv'

# Load biosample attributes
try:
    biosample_df = pd.read_csv(BIOSAMPLE_ATTRIBUTES_PATH)
except FileNotFoundError:
    print(f"Error: Biosample attributes file not found at {BIOSAMPLE_ATTRIBUTES_PATH}")
    exit(1)

biosample_lookup = {}
# Create a lookup for biosample_name (from biosample_attributes.csv) to its NMDC ID
# and include common variations from raw filenames for robust matching.
for _, row in biosample_df.iterrows():
    nmdc_id = row['id']
    name = row['name']
    biosample_lookup[name] = nmdc_id
    # Add variations for matching raw filenames (e.g., "_T" maps to "_TOP")
    if '_TOP' in name:
        biosample_lookup[name.replace('_TOP', '_T')] = nmdc_id
    if '_BTM' in name:
        biosample_lookup[name.replace('_BTM', '_B')] = nmdc_id

# Load raw file names
try:
    raw_files_df = pd.read_csv(RAW_FILES_PATH)
except FileNotFoundError:
    print(f"Error: Raw files CSV not found at {RAW_FILES_PATH}")
    exit(1)

# Load protocol YAML (not strictly needed for the mapping logic itself, 
# but confirms the YAML structure and protocol IDs exist for cross-referencing.)
try:
    with open(PROTOCOL_YAML_PATH, 'r') as f:
        protocol_yaml = yaml.safe_load(f)
except FileNotFoundError:
    print(f"Error: Protocol YAML file not found at {PROTOCOL_YAML_PATH}")
    exit(1)
except yaml.YAMLError as e:
    print(f"Error parsing YAML file: {e}")
    exit(1)

output_records = []

# Regex to capture biosample identifier and processed sample info from the filename.
# (.*?)   - Group 1: Non-greedy match for the biosample name part (e.g., "60881_6_T")
# _(\d+)   - Group 2: Matches "_" followed by one or more digits (the N in N_WEOM/MAOM, e.g., "1")
# _(WEOM|MAOM) - Group 3: Matches "_WEOM" or "_MAOM"
# _r.*$   - Matches "_r" followed by any characters to the end of the string.
#           This part is included based on the provided filename examples which consistently show "_r" followed by replicate info.
raw_file_pattern = re.compile(r'^(.*?)_(\d+)_(WEOM|MAOM)_r.*$', re.IGNORECASE)

for _, row in raw_files_df.iterrows():
    raw_filename = row['raw_data_file_short']
    
    # Initialize a record with empty strings for optional fields as per validation rules.
    # If a biosample_id cannot be determined, these fields should remain empty.
    record = {
        'raw_data_identifier': raw_filename,
        'biosample_id': '',
        'biosample_name': '',
        'processedsample_placeholder': '',
        'material_processing_protocol_id': '',
        'match_confidence': ''
    }

    # First, check for calibrant files based on keywords (case-insensitive)
    if "SRFA" in raw_filename.upper() or "SRFAII" in raw_filename.upper():
        record['match_confidence'] = 'calibrant'
        output_records.append(record)
        continue

    # Attempt to parse the filename using the regex pattern for non-calibrant files
    match = raw_file_pattern.match(raw_filename)
    if match:
        biosample_part_raw = match.group(1)
        processed_sample_code_number = int(match.group(2))
        processed_sample_type_suffix = match.group(3).lower()
        
        # Normalize the biosample part from the filename to match names in biosample_attributes.csv
        # E.g., "60881_6_T" -> "60881_6_TOP", "60933_17_B" -> "60933_17_BTM"
        biosample_part_normalized = biosample_part_raw.replace('_T', '_TOP').replace('_B', '_BTM')
        
        # Attempt to find the biosample ID using the normalized name
        biosample_id = biosample_lookup.get(biosample_part_normalized)

        if biosample_id:
            # High confidence match: biosample_id found and pattern matched
            record['biosample_id'] = biosample_id
            record['biosample_name'] = biosample_df[biosample_df['id'] == biosample_id]['name'].iloc[0]
            
            # Determine the material processing protocol ID (e.g., 'weom_extraction')
            protocol_id = f'{processed_sample_type_suffix}_extraction'
            
            # Determine the processed sample placeholder ID.
            # Based on YAML structure and examples provided, 'N_WEOM' or 'N_MAOM' refers to 
            # the Nth subsample, which after extraction and SPE becomes ProcessedSample(N+6).
            processed_sample_placeholder_num = processed_sample_code_number + 6
            processedsample_placeholder = f'ProcessedSample{processed_sample_placeholder_num}_{protocol_id}'

            record['processedsample_placeholder'] = processedsample_placeholder
            record['material_processing_protocol_id'] = protocol_id
            record['match_confidence'] = 'high' # High confidence match
        # else:
            # If the filename pattern matched, but the biosample name couldn't be resolved,
            # then biosample_id remains empty. According to validator, 'match_confidence'
            # cannot be 'low' without a biosample_id, so we leave it empty, as initialized.
            # The protocol and processed sample info also depend on a valid biosample,
            # so they should also remain empty.
            # record['biosample_name'] = biosample_part_normalized # Optional for debugging unmatched rows
    # else:
        # If the filename does not match the expected pattern at all,
        # all mapping fields remain empty, as initialized.
        # record['biosample_name'] = 'unidentified' # Optional for debugging unmatched rows

    output_records.append(record)

# Create final DataFrame from the collected records and save to CSV
output_df = pd.DataFrame(output_records)
output_df.to_csv(OUTPUT_CSV_PATH, index=False)