import pandas as pd
import re
import yaml

# Load input data
biosamples_df = pd.read_csv('/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/kroeger_11_dwsv7q78_lcms_metab/metadata/biosample_attributes.csv')
files_df = pd.read_csv('/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/kroeger_11_dwsv7q78_lcms_metab/metadata/downloaded_files.csv')

# Load material processing YAML
with open('/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/kroeger_11_dwsv7q78_lcms_metab/protocol_info/llm_generated_protocol_outline.yaml', 'r') as f:
    material_processing_protocols = yaml.safe_load(f)

# Create a dictionary for quick biosample lookup
biosample_name_to_id = {re.match(r'(S\d+_[ABC]_D\d+)', name).group(1): b_id for b_id, name in biosamples_df[['id', 'name']].values if re.match(r'(S\d+_[ABC]_D\d+)', name)}
biosample_name_to_full_name = {re.match(r'(S\d+_[ABC]_D\d+)', name).group(1): name for _, name in biosamples_df[['id', 'name']].values if re.match(r'(S\d+_[ABC]_D\d+)', name)}


def map_file_to_biosample(filename):
    """Extract metadata and match to biosample"""
    
    result = {
        'biosample_id': '',
        'biosample_name': '',
        'match_confidence': '',
        'processedsample_placeholder': '',
        'material_processing_protocol_id': ''
    }

    # Check for pilot files - should not map to biosamples
    if "pilot" in filename.lower():
        return result
    
    # Check for QC/control files
    if any(qc_keyword in filename for qc_keyword in ["QC", "ExCtrl", "Sterile-BGramaLit", "Sterile-sand", "Neg-D"]):
        # Specifically for "Neg-D89_A", etc. in file names, these are controls, not actual samples
        if "Neg-D" in filename and re.search(r'Neg-D\d+_[ABC]', filename):
             return result
        # Also check for "Neg-D" with other patterns like "Neg-D30_A_Rg70to1050" or "Neg-D45_B_Rg80to1200"
        if re.search(r'Neg-D\d+_[ABC]', filename) and "soil" not in filename: # Exclude samples like SXX-DYY_Z_Rg...-soil-S1
            return result
        return result # For other general QC/control files

    # Try to extract sample identifier
    # Pattern: SXX-DYY_Z (e.g., S32-D89_A, S16-D30_A)
    match = re.search(r'(S\d+)-(D\d+)_([ABC])', filename)
    if match:
        s_part = match.group(1) # e.g., S32
        d_part = match.group(2) # e.g., D89
        replicate_part = match.group(3) # e.g., A

        # Reconstruct biosample name part for matching: SXX_Z_DYY
        biosample_name_prefix = f"{s_part}_{replicate_part}_{d_part}"

        if biosample_name_prefix in biosample_name_to_id:
            result['biosample_id'] = biosample_name_to_id[biosample_name_prefix]
            result['biosample_name'] = biosample_name_to_full_name[biosample_name_prefix]
            result['match_confidence'] = 'high'

            # Determine protocol based on file naming convention (HILICZ for polar, C18 for nonpolar)
            if "HILICZ" in filename:
                result['material_processing_protocol_id'] = 'polar_metabolites'
                result['processedsample_placeholder'] = 'ProcessedSample3_polar_metabolites'
            elif "C18" in filename:
                result['material_processing_protocol_id'] = 'nonpolar_metabolites'
                result['processedsample_placeholder'] = 'ProcessedSample3_nonpolar_metabolites'
    
    return result

# Process all files
results = []
for filename in files_df['file_name']:
    mapping = map_file_to_biosample(filename)
    results.append({
        'raw_data_identifier': filename,
        'biosample_id': mapping['biosample_id'],
        'biosample_name': mapping['biosample_name'],
        'match_confidence': mapping['match_confidence'],
        'processedsample_placeholder': mapping['processedsample_placeholder'],
        'material_processing_protocol_id': mapping['material_processing_protocol_id']
    })

# Save output
output_df = pd.DataFrame(results)
output_df.to_csv('/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/kroeger_11_dwsv7q78_lcms_metab/metadata/llm_biosample_raw_file_mapper.csv', index=False)