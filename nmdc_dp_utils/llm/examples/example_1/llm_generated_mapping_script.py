import pandas as pd
import re
import yaml # Not strictly used for mapping logic, but present as per initial thought for parsing YAML

# Hardcoded paths for input/output files
INPUT_BIOSAMPLE_PATH = 'nmdc_dp_utils/llm/examples/example_1/biosample_attributes.csv'
INPUT_FILES_PATH = 'nmdc_dp_utils/llm/examples/example_1/downloaded_files.csv'
OUTPUT_PATH = 'nmdc_dp_utils/llm/examples/example_1/llm_generated_mapping.csv'

# Material processing YAML is provided for context to derive mapping rules,
# but the specific processed sample placeholders and protocol IDs are
# hardcoded directly into the mapping function based on these rules.
MATERIAL_PROCESSING_YAML = """
NOM:
  steps:
  - Step 1_NOM:
      SubSamplingProcess:
        description: A portion of soil being taken from <Biosample> for extraction
        has_input:
        - Biosample
        has_output:
        - ProcessedSample1_NOM
  - Step 2_NOM:
      Extraction:
        description: Water extraction of <ProcessedSample1_NOM>
        has_input:
        - ProcessedSample1_NOM
        has_output:
        - ProcessedSample2_NOM
        - ProcessedSample3_NOM
  - Step 3_NOM:
      Extraction:
        description: Methanol extraction of <ProcessedSample1_NOM>
        has_input:
        - ProcessedSample3_NOM
        has_output:
        - ProcessedSample4_NOM
        - ProcessedSample5_NOM
  - Step 4_NOM:
      Extraction:
        description: Chloroform extraction of <ProcessedSample1_NOM>
        has_input:
        - ProcessedSample5_NOM
        has_output:
        - ProcessedSample6_NOM
  processedsamples:
  - ProcessedSample1_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_subsample
        description: A portion of soil from <Biosample> for extraction
  - ProcessedSample2_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_NOM_water
        description: NOM from water extraction of <Biosample>
        sampled_portion:
        - aqueous_layer
  - ProcessedSample3_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_water_extracted_soil
        description: Water-extracted soil for further extraction of <Biosample>
        sampled_portion:
        - pellet
  - ProcessedSample4_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_NOM_methanol
        description: NOM from methanol extraction of <Biosample>
        sampled_portion:
        - methanol_layer
  - ProcessedSample5_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_methanol_extracted_soil
        description: Methanol-extracted soil for further extraction of <Biosample>
        sampled_portion:
        - pellet
  - ProcessedSample6_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_NOM_chloroform
        description: NOM from chloroform extraction of <Biosample>
        sampled_portion:
        - chloroform_layer
---
lipid:
  steps:
  - Step 1_lipid:
      SubSamplingProcess:
        description: A portion of soil being taken from <Biosample> soil sample for
          extraction
        has_input:
        - Biosample
        has_output:
        - ProcessedSample1_lipid
  - Step 2_lipid:
      Extraction:
        description: Water extraction of <ProcessedSample1_lipid> preceding Folch
          extraction
        has_input:
        - ProcessedSample1_lipid
        has_output:
        - ProcessedSample2_lipid
  - Step 3_lipid:
      Extraction:
        description: Folch extraction of <ProcessedSample2_lipid> to separate NOM
          by methanol-chloroform layer
        has_input:
        - ProcessedSample2_lipid
        has_output:
        - ProcessedSample3_lipid
  processedsamples:
  - ProcessedSample1_lipid:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_Folch-subsample
        description: The output from subsampling <Biosample> for extraction
  - ProcessedSample2_lipid:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_water_extracted_soil
        description: Water-extracted soil for Folch extraction of <Biosample>
  - ProcessedSample3_lipid:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_chloroform_lipids
        description: Chloroform layer of the Folch extract of <Biosample>
        sampled_portion:
        - chloroform_layer
---
NOM_1000S_variant: 
  steps:
  - Step 1_NOM:
      SubSamplingProcess:
        description: Portions of soil being taken from <Biosample>
        has_input:
        - Biosample
        has_output:
        - ProcessedSample1_NOM
        - ProcessedSample2_NOM
        - ProcessedSample3_NOM
  - Step 2_NOM:
      Extraction:
        description: Water extraction of <ProcessedSample1_NOM>
        has_input:
        - ProcessedSample1_NOM
        has_output:
        - ProcessedSample4_NOM
  - Step 3_NOM:
      ChromatographicSeparationProcess:
        description: Solid phase extraction of dissolved organic matter from <ProcessedSample4_NOM>
        has_input:
        - ProcessedSample4_NOM
        has_output:
        - ProcessedSample5_NOM
  - Step 4_NOM:
      Extraction:
        description: Water extraction of <ProcessedSample2_NOM>
        has_input:
        - ProcessedSample2_NOM
        has_output:
        - ProcessedSample6_NOM
  - Step 5_NOM:
      ChromatographicSeparationProcess:
        description: Solid phase extraction of dissolved organic matter from <ProcessedSample6_NOM>
        has_input:
        - ProcessedSample6_NOM
        has_output:
        - ProcessedSample7_NOM
  - Step 6_NOM:
      Extraction:
        description: Water extraction of <ProcessedSample3_NOM>
        has_input:
        - ProcessedSample3_NOM
        has_output:
        - ProcessedSample8_NOM
  - Step 7_NOM:
      ChromatographicSeparationProcess:
        description: Solid phase extraction of dissolved organic matter from <ProcessedSample8_NOM>
        has_input:
        - ProcessedSample8_NOM
        has_output:
        - ProcessedSample9_NOM
  processedsamples:
  - ProcessedSample1_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_subsample
        description: Subsample of <Biosample>, corresponding to subsampled portion
          1 of 3 for <Biosample>
  - ProcessedSample2_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_subsample
        description: Subsample of <Biosample>, corresponding to subsampled portion
          2 of 3 for <Biosample>
  - ProcessedSample3_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_subsample
        description: Subsample of <Biosample>, corresponding to subsampled portion
          3 of 3 for <Biosample>
  - ProcessedSample4_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <ProcessedSample1_NOM>_water_extract
        description: Water-extracted NOM from <ProcessedSample1_NOM>, corresponding
          to subsampled portion 1 of 3 for <Biosample>
        sampled_portion: aqueous_layer
  - ProcessedSample5_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <ProcessedSample1_NOM>_NOM_water_SPE
        description: NOM from water and solid phase extraction of <ProcessedSample1_NOM>,
          corresponding to subsampled portion 1 of 3 for <Biosample>
        sampled_portion: aqueous_layer
  - ProcessedSample6_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <ProcessedSample2_NOM>_water_extract
        description: Water-extracted NOM from <ProcessedSample2_NOM>, corresponding
          to subsampled portion 2 of 3 for <Biosample>
        sampled_portion: aqueous_layer
  - ProcessedSample7_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <ProcessedSample2_NOM>_NOM_water_SPE
        description: NOM from water and solid phase extraction of <ProcessedSample2_NOM>,
          corresponding to subsampled portion 2 of 3 for <Biosample>
        sampled_portion: aqueous_layer
  - ProcessedSample8_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <ProcessedSample3_NOM>_water_extract
        description: Water-extracted NOM from <ProcessedSample3_NOM>, corresponding
          to subsampled portion 3 of 3 for <Biosample>
        sampled_portion: aqueous_layer
  - ProcessedSample9_NOM:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <ProcessedSample3_NOM>_NOM_water_SPE
        description: NOM from water and solid phase extraction of <ProcessedSample3_NOM>,
          corresponding to subsampled portion 3 of 3 for <Biosample>
        sampled_portion: aqueous_layer
---
metab:
  steps:
  - Step 1_metab:
      separation_method: {}
  processedsamples:
  - ProcessedSample1_metab:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_exudate
        description: Exudates of of <Biosample> for metabolomics analysis
"""

def find_biosample(biosamples_df, name_pattern, is_regex=True):
    """
    Searches the biosamples DataFrame for a biosample matching the given name pattern.
    Returns biosample_id, biosample_name, confidence.
    
    Args:
        biosamples_df (pd.DataFrame): DataFrame containing biosample information.
        name_pattern (str or dict): If `is_regex` is True, this is a regex string.
                                    If `is_regex` is False, this is a dict like
                                    {"start": "LabX_", "end": "_Type_Y"}.
        is_regex (bool): Flag to indicate if name_pattern is a regex.
    """
    for index, row in biosamples_df.iterrows():
        if is_regex:
            if re.search(name_pattern, row['name']):
                return row['id'], row['name'], 'high'
        else: # Specific logic for metab samples matching start and end patterns
            if row['name'].startswith(name_pattern["start"]) and row['name'].endswith(name_pattern["end"]):
                return row['id'], row['name'], 'high'
    return None, None, 'low'

def map_file_to_biosample(filename, all_biosamples_df):
    """
    Extract metadata from filename and match to biosample and processed sample.
    """
    # Initialize default values
    biosample_id = None
    biosample_name = None
    processed_sample_placeholder = None
    material_processing_protocol_id = None
    match_confidence = 'low'

    # --- Brodie Lipids Pattern ---
    # Example: 769701_EMSL_49991_Brodie_369_Lipids_POS_09Aug19_Lola-WCSH417820.raw
    match_lipid = re.search(r'Brodie_(\d+)_Lipids', filename, re.IGNORECASE)
    if match_lipid:
        sample_num = match_lipid.group(1)
        # Biosample name typically ends with 'ER_XXX' for Brodie samples
        # E.g., "Bulk soil microbial communities ... - ER_369"
        biosample_name_regex = r"ER_" + re.escape(sample_num) + r"$"
        
        biosample_id, biosample_name, match_confidence = find_biosample(all_biosamples_df, biosample_name_regex, is_regex=True)
        
        if biosample_id:
            material_processing_protocol_id = 'lipid'
            processed_sample_placeholder = 'ProcessedSample3_lipid'
            return {
                'raw_data_identifier': filename,
                'biosample_id': biosample_id,
                'biosample_name': biosample_name,
                'match_confidence': match_confidence,
                'processedsample_placeholder': processed_sample_placeholder,
                'material_processing_protocol_id': material_processing_protocol_id
            }

    # --- Brodie NOM Pattern (H2O, MeOH, CHCl3) ---
    # Example: Brodie_134A_CHCl3_15Oct18_IAT_p1_1_01_35922.zip
    match_brodie_nom = re.search(r'Brodie_(\d+)([A-Z])?_(H2O_SPE|H2O|MeOH|CHCl3)', filename, re.IGNORECASE)
    if match_brodie_nom:
        sample_num = match_brodie_nom.group(1)
        extract_type = match_brodie_nom.group(3).upper() # H2O_SPE, H2O, MEOH, CHCL3

        # Biosample name typically ends with 'ER_XXX' for Brodie samples
        biosample_name_regex = r"ER_" + re.escape(sample_num) + r"$"
        
        biosample_id, biosample_name, match_confidence = find_biosample(all_biosamples_df, biosample_name_regex, is_regex=True)
        
        if biosample_id:
            material_processing_protocol_id = 'NOM'
            if extract_type in ['H2O', 'H2O_SPE']:
                processed_sample_placeholder = 'ProcessedSample2_NOM'
            elif extract_type == 'MEOH':
                processed_sample_placeholder = 'ProcessedSample4_NOM'
            elif extract_type == 'CHCL3':
                processed_sample_placeholder = 'ProcessedSample6_NOM'
            
            return {
                'raw_data_identifier': filename,
                'biosample_id': biosample_id,
                'biosample_name': biosample_name,
                'match_confidence': match_confidence,
                'processedsample_placeholder': processed_sample_placeholder,
                'material_processing_protocol_id': material_processing_protocol_id
            }

    # --- 1000 Soils NOM Pattern (FTMS_SPE) ---
    # Example: 1000S_CFS1_FTMS_SPE_BTM_1_run1_Fir_22Apr22_300SA_p01_19_1_3376.zip
    match_1000s_nom = re.search(r'1000S_([A-Z0-9]+)_(?:RR_)?FTMS_SPE_(BTM|TOP)_(\d+)', filename, re.IGNORECASE)
    if match_1000s_nom:
        site_id = match_1000s_nom.group(1)
        depth = match_1000s_nom.group(2).upper() # BTM or TOP
        subsample_num = int(match_1000s_nom.group(3)) # 1, 2, or 3
        
        # Biosample name: "1000 soils - SITEID_CoreB_DEPTH"
        biosample_name_regex = r"1000 soils - " + re.escape(site_id) + r"_CoreB_" + re.escape(depth) + r"$"
        
        biosample_id, biosample_name, match_confidence = find_biosample(all_biosamples_df, biosample_name_regex, is_regex=True)
        
        if biosample_id:
            material_processing_protocol_id = 'NOM' # Map to generic NOM protocol
            if subsample_num == 1:
                processed_sample_placeholder = 'ProcessedSample5_NOM'
            elif subsample_num == 2:
                processed_sample_placeholder = 'ProcessedSample7_NOM'
            elif subsample_num == 3:
                processed_sample_placeholder = 'ProcessedSample9_NOM'
            
            return {
                'raw_data_identifier': filename,
                'biosample_id': biosample_id,
                'biosample_name': biosample_name,
                'match_confidence': match_confidence,
                'processedsample_placeholder': processed_sample_placeholder,
                'material_processing_protocol_id': material_processing_protocol_id
            }
            
    # --- Metab EcoFAB Pattern ---
    # Example: 20240225_EB_VN_..._MS2_100_RtExu-D-EcoFAB-Brachy-Axenic_4_Reinj_444.raw
    # Example: 20240225_EB_VN_..._MS2_22_TxCtrl-A-EcoFAB-Medium-Sterile_1__048.raw
    
    # Pattern for experimental samples (RtExu-X-EcoFAB-Brachy-Axenic_Y or RtExu-X-EcoFAB-Brachy-SynComZZ_Y)
    match_metab_exp = re.search(r'RtExu-([A-Z])-EcoFAB-Brachy-(Axenic|SynCom(\d+))_(\d+)', filename, re.IGNORECASE)
    if match_metab_exp:
        lab_letter = match_metab_exp.group(1)
        sample_type_long = match_metab_exp.group(2)
        syncom_id = match_metab_exp.group(3)
        sample_num_suffix = match_metab_exp.group(4) # The last digit (e.g., '4' in Axenic_4)
        
        short_type = ''
        if sample_type_long.lower() == 'axenic':
            short_type = 'Axe'
        elif sample_type_long.lower().startswith('syncom'):
            short_type = f'Syn{syncom_id}'
        
        # Construct parts of the biosample name for matching: starts with 'LabX_' and ends with '_Type_Y'
        biosample_name_match_parts = {
            "start": f"Lab{lab_letter}_",
            "end": f"{short_type}_{sample_num_suffix}"
        }

        biosample_id, biosample_name, match_confidence = find_biosample(all_biosamples_df, biosample_name_match_parts, is_regex=False)
        
        if biosample_id:
            material_processing_protocol_id = 'metab'
            processed_sample_placeholder = 'ProcessedSample1_metab'
            return {
                'raw_data_identifier': filename,
                'biosample_id': biosample_id,
                'biosample_name': biosample_name,
                'match_confidence': match_confidence,
                'processedsample_placeholder': processed_sample_placeholder,
                'material_processing_protocol_id': material_processing_protocol_id
            }

    # Pattern for control samples (TxCtrl-X-EcoFAB-Medium-Sterile_Y)
    match_metab_ctrl = re.search(r'TxCtrl-([A-Z])-EcoFAB-Medium-Sterile_(\d+)', filename, re.IGNORECASE)
    if match_metab_ctrl:
        lab_letter = match_metab_ctrl.group(1)
        sample_num_suffix = match_metab_ctrl.group(2) # The last digit (e.g., '1' in Sterile_1)

        # Construct parts of the biosample name for matching
        biosample_name_match_parts = {
            "start": f"Lab{lab_letter}_",
            "end": f"TxCtrl_{sample_num_suffix}"
        }

        biosample_id, biosample_name, match_confidence = find_biosample(all_biosamples_df, biosample_name_match_parts, is_regex=False)
        
        if biosample_id:
            material_processing_protocol_id = 'metab'
            processed_sample_placeholder = 'ProcessedSample1_metab'
            return {
                'raw_data_identifier': filename,
                'biosample_id': biosample_id,
                'biosample_name': biosample_name,
                'match_confidence': match_confidence,
                'processedsample_placeholder': processed_sample_placeholder,
                'material_processing_protocol_id': material_processing_protocol_id
            }

    # If no pattern matches
    return {
        'raw_data_identifier': filename,
        'biosample_id': None,
        'biosample_name': None,
        'match_confidence': 'none',
        'processedsample_placeholder': None,
        'material_processing_protocol_id': None
    }

# Main script logic
if __name__ == "__main__":
    # Load input data
    try:
        biosamples_df = pd.read_csv(INPUT_BIOSAMPLE_PATH)
        if not all(col in biosamples_df.columns for col in ['id', 'name']):
            print(f"Error: '{INPUT_BIOSAMPLE_PATH}' must contain 'id' and 'name' columns.")
            exit()
    except FileNotFoundError:
        print(f"Error: Biosample file not found at {INPUT_BIOSAMPLE_PATH}")
        exit()
    
    try:
        files_df = pd.read_csv(INPUT_FILES_PATH)
        if 'raw_data_file_name' not in files_df.columns:
            print(f"Error: '{INPUT_FILES_PATH}' must contain a 'raw_data_file_name' column.")
            exit()
    except FileNotFoundError:
        print(f"Error: Raw files list not found at {INPUT_FILES_PATH}")
        exit()

    # Process all files
    results = []
    for filename in files_df['raw_data_file_name']:
        mapping = map_file_to_biosample(filename, biosamples_df)
        results.append(mapping)

    # Save output
    output_df = pd.DataFrame(results)
    # Define the desired column order for the output CSV
    desired_columns = [
        'raw_data_identifier',
        'biosample_id',
        'biosample_name',
        'match_confidence',
        'processedsample_placeholder',
        'material_processing_protocol_id'
    ]
    # Reindex the DataFrame to match the desired order, filling missing columns with NaN if any
    output_df = output_df.reindex(columns=desired_columns)
    output_df.to_csv(OUTPUT_PATH, index=False)