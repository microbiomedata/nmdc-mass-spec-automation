import pandas as pd
import re
import yaml

# Define file paths as per instructions
INPUT_BIOSAMPLE_PATH = 'nmdc_dp_utils/llm/examples/example_1/biosample_attributes.csv'
INPUT_FILES_PATH = 'nmdc_dp_utils/llm/examples/example_1/downloaded_files.csv'
INPUT_YAML_PATH = 'nmdc_dp_utils/llm/examples/example_1/combined_outline.yaml'
OUTPUT_PATH = 'nmdc_dp_utils/llm/examples/example_1/llm_generated_mapping.csv'

# Load input data
biosamples_df = pd.read_csv(INPUT_BIOSAMPLE_PATH)
raw_files_df = pd.read_csv(INPUT_FILES_PATH)

# Load material processing YAML (though for this specific study, we only need to know 'NOM' exists)
with open(INPUT_YAML_PATH, 'r') as f:
    material_processing_protocols = yaml.safe_load(f)

# Create a dictionary for efficient biosample lookup
# Biosample names in the CSV are like "1000 soils - ANZA_CoreB_TOP"
biosample_lookup = {
    row['name']: row['id']
    for _, row in biosamples_df.iterrows()
}

def map_file_to_biosample(filename):
    """
    Extract metadata from filename and map to biosample and processed sample
    for the specific study and protocols provided.
    """
    result = {
        'raw_data_identifier': filename,
        'biosample_id': None,
        'biosample_name': None,
        'match_confidence': 'low',  # Default to low, update if matched
        'processedsample_placeholder': None,
        'material_processing_protocol_id': None
    }

    # Pattern for file names observed in the provided raw_files_df:
    # E.g., 1000S_CFS1_FTMS_SPE_BTM_1_run1_Fir_22Apr22_300SA_p01_19_1_3376.zip
    # E.g., 1000S_FTA3_RR_FTMS_SPE_BTM_1_run1_Fir_22Apr22_300SA_p01_40_1_3397.zip (includes optional RR)
    # E.g., 1000s_ANZA_FTMS_SPE_BTM_1_29Oct22_Mag_300SA_p025_167_1_7117.zip (starts with 1000s)
    # Captures core ID, depth (BTM/TOP), and subsample ID (1, 2, or 3)
    file_pattern = re.compile(
        r"1000[Ss]_(?P<core_id>[A-Z0-9]+)(?:_RR)?_FTMS_SPE_(?P<depth>BTM|TOP)_(?P<subsample_id>[1-3])_.*\.zip"
    )
    match = file_pattern.match(filename)

    if match:
        core_id = match.group('core_id')
        depth = match.group('depth')
        subsample_id = match.group('subsample_id')

        # Construct the biosample name as it appears in the biosample_attributes.csv
        biosample_name_constructed = f"1000 soils - {core_id}_CoreB_{depth}"
        
        if biosample_name_constructed in biosample_lookup:
            result['biosample_id'] = biosample_lookup[biosample_name_constructed]
            result['biosample_name'] = biosample_name_constructed
            result['material_processing_protocol_id'] = 'NOM' # The only protocol defined in the provided YAML

            # Determine the correct processed sample placeholder based on subsample_id
            # as per the provided Material Processing YAML (NOM protocol, SPE outputs)
            if subsample_id == '1':
                result['processedsample_placeholder'] = 'ProcessedSample5_NOM'
            elif subsample_id == '2':
                result['processedsample_placeholder'] = 'ProcessedSample7_NOM'
            elif subsample_id == '3':
                result['processedsample_placeholder'] = 'ProcessedSample9_NOM'
            
            result['match_confidence'] = 'high'
        else:
            # Pattern matched, but specific biosample ID/name combination not found in the provided list
            result['match_confidence'] = 'medium' 

    return result

# Process all files
results = []
for filename in raw_files_df['raw_data_file_name']:
    mapping = map_file_to_biosample(filename)
    results.append(mapping)

# Save output
output_df = pd.DataFrame(results)
output_df.to_csv(OUTPUT_PATH, index=False)