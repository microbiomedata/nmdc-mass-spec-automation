import pandas as pd
import re

# Load input data
biosamples = pd.read_csv('/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/studies/example_lcms_lipids/metadata/biosample_attributes.csv')
files = pd.read_csv('/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/studies/example_lcms_lipids/metadata/downloaded_files.csv')

def map_file_to_biosample(filename, biosamples_df):
    """
    Extract metadata from filename and match to biosample information and processing protocol.
    """
    result = {
        'biosample_id': '',
        'biosample_name': '',
        'match_confidence': '',
        'processedsample_placeholder': '',
        'material_processing_protocol_id': ''
    }

    # CRITICAL CONSTRAINT: ONLY map files with "MSMS" in the file name, as per additional context.
    if "MSMS" not in filename:
        return result

    # Extract sample identifier from filename.
    # The pattern observed in the provided raw files is SXX-DYY_Z (e.g., S17-D45_A).
    match = re.search(r'S(\d+)-D(\d+)_([A-Z])', filename)
    if not match:
        # If no specific sample ID pattern is found, return with empty mapping fields.
        # This handles any files that do not correspond to a biosample using this pattern.
        return result

    sample_number = match.group(1)       # e.g., '17'
    day_number = match.group(2)          # e.g., '45'
    replicate_letter = match.group(3)    # e.g., 'A'

    # Determine the material processing protocol and processed sample placeholder
    # based on keywords in the filename ("HILICZ" for polar, "C18" for nonpolar).
    protocol_id = ''
    processedsample_placeholder = ''
    if "HILICZ" in filename:
        protocol_id = 'polar_metabolites_extraction'
        # According to the YAML, ProcessedSample3 is the final resuspended extract.
        processedsample_placeholder = 'ProcessedSample3_polar_metabolites_extraction'
    elif "C18" in filename:
        protocol_id = 'nonpolar_metabolites_extraction'
        # According to the YAML, ProcessedSample3 is the final resuspended extract.
        processedsample_placeholder = 'ProcessedSample3_nonpolar_metabolites_extraction'
    else:
        # If no known protocol identifier found, return with empty mapping fields.
        return result

    # Construct a specific part of the biosample name for matching.
    # Biosample names are like 'S17_A_D45 hydrophilic' or 'S17_C_D45 hydrophilic'.
    # We construct 'S17_A_D45' to find the specific biosample.
    target_biosample_name_part = f"S{sample_number}_{replicate_letter}_D{day_number}"

    # Search for a matching biosample in the provided biosamples DataFrame.
    # Using regex=False for literal string matching in .str.contains.
    matched_biosamples = biosamples_df[biosamples_df['name'].str.contains(target_biosample_name_part, regex=False, na=False)]

    if not matched_biosamples.empty:
        # Assuming there is a unique best match for the specific target_biosample_name_part.
        # Taking the first match if multiple (should not happen with this specific pattern).
        biosample_row = matched_biosamples.iloc[0]
        result['biosample_id'] = biosample_row['id']
        result['biosample_name'] = biosample_row['name']
        result['match_confidence'] = "high" # High confidence due to specific part match
    # If no match, biosample_id, biosample_name, and match_confidence remain empty strings.

    result['processedsample_placeholder'] = processedsample_placeholder
    result['material_processing_protocol_id'] = protocol_id

    return result

# Process all files listed in the 'file_name' column of the input CSV.
results = []
for filename in files['file_name']:
    mapping = map_file_to_biosample(filename, biosamples)
    results.append({
        'raw_data_identifier': filename,
        'biosample_id': mapping['biosample_id'],
        'biosample_name': mapping['biosample_name'],
        'match_confidence': mapping['match_confidence'],
        'processedsample_placeholder': mapping['processedsample_placeholder'],
        'material_processing_protocol_id': mapping['material_processing_protocol_id']
    })

# Convert the results list to a DataFrame and save to CSV.
output = pd.DataFrame(results)
output.to_csv('/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/studies/example_lcms_lipids/metadata/llm_biosample_raw_file_mapper.csv', index=False)