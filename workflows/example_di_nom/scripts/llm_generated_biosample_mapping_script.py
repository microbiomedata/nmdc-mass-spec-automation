import pandas as pd
import yaml
import re

# File paths - ensure these are the exact paths as specified
BIOSAMPLES_CSV_PATH = '/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/miesel_11_8bjf2432_nom/metadata/biosample_attributes.csv'
RAW_FILES_CSV_PATH = '/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/miesel_11_8bjf2432_nom/metadata/downloaded_files.csv'
PROTOCOL_YAML_PATH = '/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/miesel_11_8bjf2432_nom/protocol_info/llm_generated_protocol_outline.yaml'
OUTPUT_CSV_PATH = '/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/miesel_11_8bjf2432_nom/metadata/llm_biosample_raw_file_mapper.csv'

# Load biosample attributes
biosample_df = pd.read_csv(BIOSAMPLES_CSV_PATH)

# Prepare a mapping from a searchable tag (derived from biosample name) to full biosample info
biosample_tag_map = {}
for _id, name in biosample_df[['id', 'name']].values:
    search_tag = ""
    # Process biosample names to extract a searchable tag
    if name.startswith('EMSL'):
        # For names like 'EMSL 4 BONA_Oea_PRE_NotGr_NA_NA', extract 'BONA_Oea'
        match = re.search(r'EMSL \d+\s*([A-Z0-9_]+)(?:_PRE_NotGr_NA_NA)?(?:_POST_MST_33)?(?:_POST_MSTplus5_33)?', name)
        if match:
            search_tag = match.group(1)
    else:
        # For names like 'BLAN_032-M-31.5-18.5-20190702-ga1', use the part before the first underscore as a primary tag
        # This is a simplification; a more robust approach might be needed based on specific biosample naming conventions.
        # For this dataset, the full name seems to be a good search tag if not EMSL.
        search_tag = name.split('_')[0] if '_' in name and not name.startswith('EMSL') else name
        # Further refinement for biosamples like BLAN_032-M-... -> BLAN
        if re.match(r'[A-Z]{4}_\d{3}-M-.*', name):
            search_tag = name.split('_')[0]


    if search_tag:
        biosample_tag_map[search_tag] = {'id': _id, 'name': name}

# Add more specific tags from filenames directly if they appear in raw_data_file_short
# This handles cases where biosample names might be abbreviated or altered in filenames
# and ensures we prioritize specific filename patterns for matching.
# This loop should ideally run after initial biosample_tag_map population.
for _id, name in biosample_df[['id', 'name']].values:
    # Example: 'ABBY_A_PRE_NotGr_NA_NA' or 'ABBY_A_POST_NotGr_MST+5_33' -> 'ABBY_A'
    match = re.search(r'([A-Z]{4}_A)(?:_PRE_NotGr_NA_NA|_POST_NotGr_MST\+?5?_33)?', name)
    if match:
        biosample_tag_map[match.group(1)] = {'id': _id, 'name': name}

    # Extracting biosample parts like 'ABBY', 'BLAN', 'BONA', etc. from EMSL biosample names
    # This captures the 4-letter site code.
    emsl_match = re.search(r'EMSL \d+ ([A-Z]{4})(?:_A1|_Ap1|_Ak|_Oea|_Oaf|_A)?(?:_PRE_NotGr_NA_NA|_POST_NotGr_MST\+?5?_33)?', name)
    if emsl_match:
        biosample_tag_map[emsl_match.group(1)] = {'id': _id, 'name': name}


# Sort tags by length in descending order to prioritize longer, more specific matches
sorted_biosample_tags = sorted(biosample_tag_map.keys(), key=len, reverse=True)

# Load raw file names
raw_files_df = pd.read_csv(RAW_FILES_CSV_PATH)
raw_file_short_names = raw_files_df['raw_data_file_short'].tolist()

# Load material processing YAML
with open(PROTOCOL_YAML_PATH, 'r') as f:
    protocol_yaml = yaml.safe_load(f)

# The top-level protocol is 'ALL'
top_level_protocol_id = 'ALL'

# Define the processed sample placeholders based on the YAML structure in the prompt
# This assumes specific ProcessedSample IDs within the 'ALL' protocol structure
processed_sample_map = {
    'H2O_SPE': 'ProcessedSample6_NOM',
    'Met_SPE': 'ProcessedSample7_NOM',
    'GCMS_Combined': 'ProcessedSample2_GCMS' # For files ending in _MST_33 or _MSTplus5_33
}

# Initialize list to store results
output_records = []

for raw_file_name in raw_file_short_names:
    record = {
        'biosample_id': '',
        'biosample_name': '',
        'raw_data_identifier': raw_file_name,
        'material_processing_protocol_id': '',
        'processedsample_placeholder': '',
        'match_confidence': '' # Default to empty for unmapped
    }

    # Handle calibrant files first
    if 'QC_SRFAII' in raw_file_name:
        record['match_confidence'] = 'calibrant'
        # For calibrants, biosample_id, biosample_name, protocol_id, and processedsample_placeholder are left empty
        output_records.append(record)
        continue

    matched_biosample_info = None
    # Try to match the biosample using the sorted tags
    for tag in sorted_biosample_tags:
        # Match the tag as a whole word or significant part to avoid partial matches
        # For example, "BONA" should match "BONA_Oea" but not "Carbona"
        # Using regex word boundary \b or checking for delimiters like _
        if re.search(r'\b' + re.escape(tag) + r'($|_)', raw_file_name) or \
           re.search(r'(_|^)' + re.escape(tag) + r'($|_)', raw_file_name):
            matched_biosample_info = biosample_tag_map[tag]
            # Prioritize EMSL biosample matching if the raw file also contains EMSL string
            if "EMSL" in matched_biosample_info['name'] and "EMSL" in raw_file_name:
                break
            elif "EMSL" not in matched_biosample_info['name'] and "EMSL" not in raw_file_name:
                break
            # If there's a mix, let the longer tag match win if it comes first due to sorting
            if "EMSL" in matched_biosample_info['name'] and "EMSL" not in raw_file_name:
                # If biosample is EMSL but filename is not, it's a potential mismatch or a complex case.
                # Let's continue to see if a non-EMSL tag matches later.
                continue
            if "EMSL" not in matched_biosample_info['name'] and "EMSL" in raw_file_name:
                # If biosample is non-EMSL but filename is EMSL, similarly continue.
                continue

    if matched_biosample_info:
        current_protocol_id = top_level_protocol_id
        current_processedsample_placeholder = ''
        
        # Determine specific processed sample placeholder and set confidence
        if 'H2O_SPE' in raw_file_name:
            current_processedsample_placeholder = processed_sample_map['H2O_SPE']
            record['match_confidence'] = 'high'
        elif 'Met_SPE' in raw_file_name:
            current_processedsample_placeholder = processed_sample_map['Met_SPE']
            record['match_confidence'] = 'high'
        elif '_MST_33' in raw_file_name or '_MSTplus5_33' in raw_file_name:
            current_processedsample_placeholder = processed_sample_map['GCMS_Combined']
            record['match_confidence'] = 'high'
        
        if current_processedsample_placeholder: # If a specific placeholder was identified
            record['biosample_id'] = matched_biosample_info['id']
            record['biosample_name'] = matched_biosample_info['name']
            record['material_processing_protocol_id'] = current_protocol_id
            record['processedsample_placeholder'] = current_processedsample_placeholder
        # If biosample matched but no specific placeholder, match_confidence remains empty
        # This implicitly marks it as not fully mapped, adhering to the empty string requirement.

    # If no biosample was matched at all, match_confidence remains empty, as initialized.
        
    output_records.append(record)

# Create DataFrame and save to CSV
output_df = pd.DataFrame(output_records)
output_df.to_csv(OUTPUT_CSV_PATH, index=False)