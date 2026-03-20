import csv
import re

# File paths - IMPORTANT: Use these EXACT paths
BIOSAMPLE_ATTRIBUTES_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/studies/example_gcms_metab/metadata/biosample_attributes.csv'
DOWNLOADED_FILES_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/studies/example_gcms_metab/metadata/downloaded_files.csv'
PROTOCOL_YAML_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/studies/example_gcms_metab/protocol_info/llm_generated_protocol_outline.yaml'
OUTPUT_CSV_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/studies/example_gcms_metab/metadata/llm_biosample_raw_file_mapper.csv'

# Protocol and ProcessedSample details (hardcoded based on the provided YAML example)
MATERIAL_PROCESSING_PROTOCOL_ID = 'gcms_metabolomics'
# This is the output of the final step in the YAML for GCMS metabolomics pathway
PROCESSEDSAMPLE_PLACEHOLDER = 'ProcessedSample4_gcms_metabolomics' 

# 1. Load Biosample Attributes
biosamples = {} # Stores biosample name -> id mapping
biosample_name_to_id = {}
with open(BIOSAMPLE_ATTRIBUTES_PATH, mode='r', newline='') as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        biosample_name_to_id[row['name']] = row['id']

# Prepare biosample patterns for matching raw files
biosample_match_keys = {} # Stores cleaned_name -> {'id': bsm_id, 'name': name}
for name, bsm_id in biosample_name_to_id.items():
    # Remove prefixes 'BW-' and 'Inc-BW-'
    cleaned_name = name.replace('BW-', '').replace('Inc-BW-', '')
    # Convert hyphens to underscores for matching with raw file naming convention
    match_key = cleaned_name.replace('-', '_')
    biosample_match_keys[match_key] = {'id': bsm_id, 'name': name}

# 2. Load Raw File Names
raw_files = []
with open(DOWNLOADED_FILES_PATH, mode='r', newline='') as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        raw_files.append(row['raw_data_file_short'])

# 3. Perform Mapping
mapping_results = []
for raw_file in raw_files:
    biosample_id = ''
    biosample_name = ''
    match_confidence = '' # Default to empty string for unmapped
    processed_sample_placeholder = ''
    material_processing_protocol = ''

    # Calibrant/Control check based on provided example's study context
    if 'GCMS_FAMEs' in raw_file:
        match_confidence = 'calibrant'
        # For calibrants, assign the relevant protocol and processed sample placeholder
        processed_sample_placeholder = PROCESSEDSAMPLE_PLACEHOLDER
        material_processing_protocol = MATERIAL_PROCESSING_PROTOCOL_ID
    else:
        # Regex to extract key parts from raw file name like Blanch_Nat_Met_C_12_AB_M_17.cdf
        # It captures the type (C/H), number, and M/O indicator.
        match_obj = re.match(r'Blanch_Nat_Met_([CH])_(\d+)_AB_([MO])_\d+\.cdf', raw_file)
        if match_obj:
            extracted_key = f"{match_obj.group(1)}_{match_obj.group(2)}_{match_obj.group(3)}"
            
            if extracted_key in biosample_match_keys:
                biosample_info = biosample_match_keys[extracted_key]
                biosample_id = biosample_info['id']
                biosample_name = biosample_info['name']
                match_confidence = 'high' # Explicitly set to 'high' for mapped biosamples
                processed_sample_placeholder = PROCESSEDSAMPLE_PLACEHOLDER
                material_processing_protocol = MATERIAL_PROCESSING_PROTOCOL_ID
            # If match_obj exists but extracted_key is not in biosample_match_keys,
            # match_confidence remains empty (unmapped).

    mapping_results.append({
        'raw_data_identifier': raw_file,
        'biosample_id': biosample_id,
        'biosample_name': biosample_name,
        'material_processing_protocol_id': material_processing_protocol,
        'processedsample_placeholder': processed_sample_placeholder,
        'match_confidence': match_confidence
    })

# 4. Write Output CSV
fieldnames = [
    'raw_data_identifier',
    'biosample_id',
    'biosample_name',
    'material_processing_protocol_id',
    'processedsample_placeholder',
    'match_confidence'
]

with open(OUTPUT_CSV_PATH, mode='w', newline='') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(mapping_results)

print(f"Mapping results written to {OUTPUT_CSV_PATH}")