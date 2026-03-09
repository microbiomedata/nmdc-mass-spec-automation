import pandas as pd
import re
import yaml

# Load input data
biosamples = pd.read_csv('/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/studies/example_lcms_lipids/metadata/biosample_attributes.csv')
files = pd.read_csv('/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/studies/example_lcms_lipids/metadata/downloaded_files.csv')

with open('/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/studies/example_lcms_lipids/protocol_info/llm_generated_protocol_outline.yaml', 'r') as f:
    protocols_data = yaml.safe_load(f)

def map_file_to_biosample(filename, biosamples_df, protocols_dict):
    """Extract metadata and match to biosample"""
    
    result = {
        'raw_data_identifier': filename,
        'biosample_id': "",
        'biosample_name': "",
        'match_confidence': "",
        'processedsample_placeholder': "",
        'material_processing_protocol_id': ""
    }

    # CRITICAL CONSTRAINT: Only map files with "MSMS"
    if "MSMS" not in filename:
        return result
    
    # Handle pilot samples - do not map to biosamples
    if "pilot" in filename.lower():
        return result

    # Determine protocol based on filename
    protocol_id = ""
    processed_sample_placeholder = ""
    if "HILICZ" in filename:
        protocol_id = "polar_metabolites_extraction"
        processed_sample_placeholder = "ProcessedSample3_polar_metabolites_extraction"
    elif "C18" in filename:
        protocol_id = "nonpolar_metabolites_extraction"
        processed_sample_placeholder = "ProcessedSample3_nonpolar_metabolites_extraction"
    
    if protocol_id and processed_sample_placeholder:
        result['material_processing_protocol_id'] = protocol_id
        result['processedsample_placeholder'] = processed_sample_placeholder
    else:
        # If no protocol is matched, it means we can't map this file to a processed sample
        return result

    # Handle QC files for match_confidence before biosample matching
    qc_keywords = ["qc", "blank", "control"]
    if any(qc_kw in filename.lower() for qc_kw in qc_keywords):
        result['biosample_id'] = ""
        result['biosample_name'] = ""
        result['match_confidence'] = "" # Empty for regular QC/blanks
        return result
    
    if "fames" in filename.lower() or "srfa" in filename.lower():
        result['biosample_id'] = ""
        result['biosample_name'] = ""
        result['match_confidence'] = "calibrant"
        return result

    # --- Biosample matching logic ---
    biosample_found = False
    
    # Attempt to extract detailed sample identifier: S<num>-D<num>_<Letter>
    # The sample identifier is present in the filename after _MS1_XX_ or _MSMS_XX_
    # in the format S<SampleNumber>-D<DayNumber>_<ReplicateLetter>
    detailed_sample_match = re.search(r'(S\d+)-D(\d+)_(A|B|C)', filename)
    if detailed_sample_match:
        s_num_from_file = detailed_sample_match.group(1) # e.g., S17
        d_num_from_file = detailed_sample_match.group(2) # e.g., 45
        letter_from_file = detailed_sample_match.group(3) # e.g., A

        # Construct the biosample name pattern for matching in the biosamples DataFrame
        # Biosample names in CSV are like 'S17_C_D45 hydrophilic'
        # So we construct 'S<num>_<Letter>_D<num>' from the filename parts
        biosample_name_search_pattern = f"{s_num_from_file}_{letter_from_file}_D{d_num_from_file}"

        matched_biosample_df = biosamples_df[
            biosamples_df['name'].str.contains(biosample_name_search_pattern, case=False, na=False)
        ]
        
        # FIX: ValueError: The truth value of a Series is ambiguous. Use .empty
        if not matched_biosample_df.empty:
            # Assuming there's only one relevant match or taking the first one
            matched_biosample = matched_biosample_df.iloc[0]
            result['biosample_id'] = matched_biosample['id']
            result['biosample_name'] = matched_biosample['name']
            result['match_confidence'] = "high"
            biosample_found = True
    
    # If no detailed match, try matching just S<num> as a fallback (medium confidence)
    if not biosample_found:
        s_only_match = re.search(r'(S\d+)', filename)
        if s_only_match:
            s_num_only_from_file = s_only_match.group(0) # e.g., S17 (includes 'S')
            
            # Filter biosamples that contain S<num> and DO NOT contain a D<num> pattern
            # This prioritizes more general biosample names (e.g., "S16") if the file
            # itself only specifies S<num> and lacks D<num>/Letter detail.
            # Use '\b' for word boundary to avoid matching 'S1' in 'S10'
            matched_biosample_df_simple = biosamples_df[
                biosamples_df['name'].str.contains(rf'\b{s_num_only_from_file}\b', case=False, na=False) &
                ~biosamples_df['name'].str.contains(r'_D\d+', case=False, na=False)
            ]
            
            if not matched_biosample_df_simple.empty:
                matched_biosample = matched_biosample_df_simple.iloc[0]
                result['biosample_id'] = matched_biosample['id']
                result['biosample_name'] = matched_biosample['name']
                result['match_confidence'] = "medium"
                biosample_found = True
        
    if not biosample_found:
        # If a protocol was found but no biosample, confidence is low.
        # If no protocol was found, it would have returned earlier.
        result['match_confidence'] = "low"

    return result

# Process all files
results = []
# The column name from downloaded_files.csv is 'file_name'
for filename in files['file_name']:
    mapping = map_file_to_biosample(filename, biosamples, protocols_data)
    results.append(mapping)

# Save output
output = pd.DataFrame(results)
output.to_csv('/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/studies/example_lcms_lipids/metadata/llm_biosample_raw_file_mapper.csv', index=False)