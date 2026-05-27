import pandas as pd
import yaml
import re

# Define file paths
BIOSAMPLE_ATTRIBUTES_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/miesel_11_8bjf2432_gcms_metab/metadata/biosample_attributes.csv'
RAW_FILES_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/miesel_11_8bjf2432_gcms_metab/metadata/downloaded_files.csv'
PROTOCOL_YAML_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/miesel_11_8bjf2432_gcms_metab/protocol_info/llm_generated_protocol_outline.yaml'
OUTPUT_CSV_PATH = '/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/miesel_11_8bjf2432_gcms_metab/metadata/llm_biosample_raw_file_mapper.csv'

def clean_name_for_matching(name):
    """
    Cleans a name string for consistent matching across biosample names and raw file names.
    This function aims to extract the most stable, unique identifier for a biosample.
    """
    if not isinstance(name, str):
        return str(name)

    name = name.upper() # Standardize to uppercase

    # Remove file extensions
    name = name.replace('.CDF', '')

    # Replace common separators with underscores
    name = re.sub(r'[\s\-]+', '_', name) # Replace spaces and hyphens with single underscore

    # Remove parts that denote processing or additional info, common in both biosample names and raw file names
    # Examples: _PRE, _POST, _NOTGR, _NA, _MST_33, _MSTPLUS5_33, _M_31_5 (depth info)
    
    # Specific patterns for raw file suffixes / biosample modifiers
    # MST+5_33, MST_33, MST (should capture _MST, _MSTPLUS5)
    name = re.sub(r'_MST(?:_?PLUS\d+)?(?:_\d+)?$', '', name) # Covers _MST, _MST_33, _MSTPLUS5_33

    name = re.sub(r'_PRE$', '', name)
    name = re.sub(r'_POST$', '', name)
    name = re.sub(r'_NOTGR$', '', name)
    name = re.sub(r'_NA$', '', name)
    
    # Remove parts like '_31_5', '_18_5', '_20190702', '_GA1', '_GA2', '_GA3', '_GA4', '_GA5'
    # These often indicate depth, date, or replicate information that might not be in the core biosample name
    name = re.sub(r'_\d{1,2}(?:_\d{1,2}){0,2}(?:_GA\d+)?$', '', name) # Match _D_D or _D_D_D or _GA1 type suffixes
    name = re.sub(r'_\d{8}$', '', name) # YYYYMMDD
    name = re.sub(r'_[A-Z]\d+$', '', name) # Example: _GA1

    # Final cleanup of leading/trailing underscores
    name = name.strip('_')
    
    # Heuristics for specific biosample naming patterns observed in biosample_attributes.csv
    # e.g., "EMSL 1 ABBY_A_PRE_NotGr_NA_NA" -> "EMSL_1_ABBY_A"
    # e.g., "BLAN_032-M-31.5-18.5-20190702-ga1" -> "BLAN_032_M"
    match_em = re.match(r'(EMSL_\d+_[A-Z0-9]+_?[A-Z0-9]*)', name)
    if match_em:
        return match_em.group(1).strip('_')

    # General pattern for SITE_NUMBER_TYPE (e.g., BLAN_032_M)
    match_site_num_type = re.match(r'([A-Z]+_\d+_[A-Z])', name)
    if match_site_num_type:
        return match_site_num_type.group(1)

    return name

def map_raw_files_to_samples():
    """
    Maps raw data files to biosamples and processed samples based on provided CSVs and YAML.
    """
    # Load biosample attributes
    biosample_df = pd.read_csv(BIOSAMPLE_ATTRIBUTES_PATH)
    # Create a mapping from cleaned biosample name to biosample ID and original name
    biosample_name_to_id = {}
    biosample_name_to_original_name = {}
    for index, row in biosample_df.iterrows():
        cleaned = clean_name_for_matching(row['name'])
        biosample_name_to_id[cleaned] = row['id']
        biosample_name_to_original_name[cleaned] = row['name'] # Store the original name before cleaning

    # Load raw files
    raw_files_df = pd.read_csv(RAW_FILES_PATH)

    # Load material processing YAML
    with open(PROTOCOL_YAML_PATH, 'r') as f:
        protocol_yaml = yaml.safe_load(f)

    results = []

    # Identify the GCMS processed sample identifier and template from the YAML
    gcms_processed_samples_info = {} # protocol_name -> {'processed_sample_id_template', 'processed_sample_name_template'}

    for proto_name, proto_details in protocol_yaml.items():
        if 'processedsamples' in proto_details:
            for ps_entry in proto_details['processedsamples']:
                ps_key = list(ps_entry.keys())[0] # e.g., ProcessedSample5_bulk_feces
                ps_data = ps_entry[ps_key]
                ps_name = ps_data.get('ProcessedSample', {}).get('name')
                ps_description = ps_data.get('ProcessedSample', {}).get('description')

                # Ensure values are strings for 'in' operator checks
                if ps_name is None: ps_name = ""
                if ps_description is None: ps_description = ""
                
                # Updated logic to identify GCMS derivatized samples more robustly
                # Based on the example, `name: <Biosample>_derivatized_GCMS` and `description: Derivatized <Biosample> for GCMS metabolomics analysis`
                # The "#input to DG" is a comment, not part of the description string.
                if (re.search(r'DERIVATIZED_GCMS', ps_name.upper()) and 
                    re.search(r'GCMS METABOLOMICS ANALYSIS', ps_description.upper())):
                    
                    # If multiple protocols contain such a sample, prioritize 'bulk_soil'
                    # If not 'bulk_soil', take the first one found.
                    if 'bulk_soil' not in gcms_processed_samples_info or proto_name == 'bulk_soil':
                        gcms_processed_samples_info[proto_name] = {
                            'processed_sample_id_template': ps_key,
                            'processed_sample_name_template': ps_name
                        }
    
    # Determine the default GCMS protocol to use
    default_gcms_protocol_name = None
    if 'bulk_soil' in gcms_processed_samples_info:
        default_gcms_protocol_name = 'bulk_soil'
    elif gcms_processed_samples_info: # If 'bulk_soil' not found, pick the first one identified
        default_gcms_protocol_name = list(gcms_processed_samples_info.keys())[0]
    
    if not default_gcms_protocol_name:
        raise ValueError("Could not find any GCMS processed sample definitions in the YAML protocols that match the expected pattern (e.g., '_derivatized_GCMS' in name and 'GCMS metabolomics analysis' in description).")

    gcms_ps_id_template = gcms_processed_samples_info[default_gcms_protocol_name]['processed_sample_id_template']
    gcms_ps_name_template = gcms_processed_samples_info[default_gcms_protocol_name]['processed_sample_name_template']

    for index, row in raw_files_df.iterrows():
        raw_file_short = row['raw_data_file_short']
        
        # Exclude blank and FAMEs files from mapping as they are not tied to biosamples
        if "Blank" in raw_file_short or "FAMEs" in raw_file_short:
            continue

        # Clean raw file name for matching
        cleaned_file_biosample_name = clean_name_for_matching(raw_file_short)
        
        # Look up biosample_id and original biosample_name using the cleaned name
        biosample_id = biosample_name_to_id.get(cleaned_file_biosample_name)
        original_biosample_name = biosample_name_to_original_name.get(cleaned_file_biosample_name)
        
        if biosample_id and original_biosample_name:
            # Construct processed_sample_name by replacing <Biosample> placeholder with the original biosample name
            processed_sample_name = gcms_ps_name_template.replace('<Biosample>', original_biosample_name)
            
            # Use the YAML's ProcessedSample key as a placeholder ID.
            processed_sample_id_from_yaml_key = gcms_ps_id_template
            
            results.append({
                'raw_data_file': raw_file_short,
                'biosample_id': biosample_id,
                'biosample_name': original_biosample_name,
                'processed_sample_id': processed_sample_id_from_yaml_key,
                'processed_sample_name': processed_sample_name,
                'protocol_name': default_gcms_protocol_name
            })
        else:
            print(f"Warning: Could not find biosample for raw file: {raw_file_short} (cleaned name: '{cleaned_file_biosample_name}')")

    # Create output DataFrame and save to CSV
    output_df = pd.DataFrame(results)
    output_df.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"Mapping complete. Output saved to {OUTPUT_CSV_PATH}")

if __name__ == '__main__':
    map_raw_files_to_samples()