import pandas as pd
import re
import yaml

# Load input data
biosamples = pd.read_csv('nmdc_dp_utils/llm/examples/example_1/biosample_attributes.csv')
files = pd.read_csv('nmdc_dp_utils/llm/examples/example_1/downloaded_files.csv')

# Load material processing protocols (YAML)
with open('nmdc_dp_utils/llm/examples/example_1/combined_outline.yaml', 'r') as f:
    material_processing_protocols = yaml.safe_load(f)

def map_file_to_biosample(filename, biosamples_df, protocols_yaml):
    """
    Extracts metadata from the filename and maps it to a biosample and
    processed sample based on predefined patterns and protocols.
    """
    # Initialize results
    biosample_id = None
    biosample_name = None
    match_confidence = "low"
    processedsample_placeholder = None
    material_processing_protocol_id = None

    # Pattern for 1000S files
    # Captures site (e.g., CFS1, FTA3_RR), layer (BTM/TOP), and replicate number (1, 2, 3 or 2b)
    pattern = re.compile(
        r"1000[Ss]_(?P<site>[A-Z0-9_]+)_FTMS_SPE_(?P<layer>(?:BTM|TOP))_(?P<replicate>\d|2b)_.+\.zip"
    )
    
    match = pattern.match(filename)

    if match:
        site_raw = match.group("site")
        layer = match.group("layer")
        replicate = match.group("replicate")

        # Clean site name: remove '_RR' if present
        site_cleaned = site_raw.replace("_RR", "")

        # Construct biosample name based on file metadata
        expected_biosample_name = f"1000 soils - {site_cleaned}_CoreB_{layer}"

        # Find matching biosample
        matched_biosample = biosamples_df[biosamples_df['name'] == expected_biosample_name]

        if not matched_biosample.empty:
            biosample_id = matched_biosample.iloc[0]['id']
            biosample_name = matched_biosample.iloc[0]['name']
            
            # All these files are related to the 'NOM' protocol
            material_processing_protocol_id = 'NOM'

            # Determine processed sample placeholder based on replicate number
            if replicate == '1':
                processedsample_placeholder = 'ProcessedSample5_NOM'
            elif replicate in ['2', '2b']:
                processedsample_placeholder = 'ProcessedSample7_NOM'
            elif replicate == '3':
                processedsample_placeholder = 'ProcessedSample9_NOM'
            
            if biosample_id and processedsample_placeholder and material_processing_protocol_id:
                match_confidence = "high"
        else:
            # Matched file pattern but couldn't find biosample
            match_confidence = "medium"

    return {
        "raw_data_identifier": filename,
        "biosample_id": biosample_id,
        "biosample_name": biosample_name,
        "match_confidence": match_confidence,
        "processedsample_placeholder": processedsample_placeholder,
        "material_processing_protocol_id": material_processing_protocol_id,
    }

# Process all files
results = []
for filename in files['raw_data_file_name']:
    mapping = map_file_to_biosample(filename, biosamples, material_processing_protocols)
    results.append(mapping)

# Save output
output = pd.DataFrame(results)
output.to_csv('nmdc_dp_utils/llm/examples/example_1/llm_generated_mapping_20260317.csv', index=False)