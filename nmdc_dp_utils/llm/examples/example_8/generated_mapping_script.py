import pandas as pd
import re
import yaml

# --- Configuration Paths ---
INPUT_BIOSAMPLE_PATH = 'nmdc_dp_utils/llm/examples/example_8/biosample_attributes.csv'
INPUT_FILES_PATH = 'nmdc_dp_utils/llm/examples/example_8/downloaded_files.csv'
OUTPUT_PATH = 'nmdc_dp_utils/llm/examples/example_8/llm_generated_mapping_codegen.csv'
RAW_FILE_NAME_COLUMN = 'raw_data_file_name'

# --- Material Processing Protocols (YAML data provided in task description) ---
material_processing_protocols_yaml_str = """
polar_metabolites:
  steps:
  - Step 1_polar_metabolites:
      SubSamplingProcess:
        description: A 2 g portion taken from <Biosample> for polar metabolite extraction.
        has_input:
        - Biosample
        has_output:
        - ProcessedSample1_polar_metabolites
  - Step 2_polar_metabolites:
      Extraction:
        description: Polar metabolites extracted from <ProcessedSample1_polar_metabolites>
          using LC-MS water, including vortexing, sonication in an iced water bath
          (10 min), centrifugation (5 min at 5000 rpm), re-vortexing, re-sonication
          (4 min in iced water bath), and re-centrifugation (10 min at 8000 rpm).
          The supernatant was collected and then lyophilized.
        has_input:
        - ProcessedSample1_polar_metabolites
        has_output:
        - ProcessedSample2_polar_metabolites
  - Step 3_polar_metabolites:
      DissolvingProcess:
        description: Dried polar metabolite extracts from <ProcessedSample2_polar_metabolites>
          resuspended in 100% methanol containing isotopically labeled internal standards
          for LC-MS analysis.
        has_input:
        - ProcessedSample2_polar_metabolites
        has_output:
        - ProcessedSample3_polar_metabolites
  processedsamples:
  - ProcessedSample1_polar_metabolites:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_polar_subsample
        description: A 2 g subsample of <Biosample> prepared for polar metabolite
          extraction.
  - ProcessedSample2_polar_metabolites:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_dried_polar_extract
        description: Dried polar metabolite extract obtained from <ProcessedSample1_polar_metabolites>
          after water extraction and lyophilization.
        sampled_portion: aqueous_layer
  - ProcessedSample3_polar_metabolites:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_resuspended_polar_metabolites
        description: Resuspended dried polar metabolite extracts from <ProcessedSample2_polar_metabolites>
          for LC-MS analysis.
        sampled_portion: methanol_layer
nonpolar_metabolites:
  steps:
  - Step 1_nonpolar_metabolites:
      SubSamplingProcess:
        description: A 2 g portion taken from <Biosample> for nonpolar metabolite
          extraction.
        has_input:
        - Biosample
        has_output:
        - ProcessedSample1_nonpolar_metabolites
  - Step 2_nonpolar_metabolites:
      Extraction:
        description: Nonpolar metabolites extracted from <ProcessedSample1_nonpolar_metabolites>
          using 100% methanol, including vortexing, sonication, and centrifugation.
          The supernatant was collected and dried in a SpeedVac.
        has_input:
        - ProcessedSample1_nonpolar_metabolites
        has_output:
        - ProcessedSample2_nonpolar_metabolites
  - Step 3_nonpolar_metabolites:
      DissolvingProcess:
        description: Dried nonpolar metabolite extracts from <ProcessedSample2_nonpolar_metabolites>
          resuspended in 100% methanol containing isotopically labeled internal standards
          for LC-MS analysis.
        has_input:
        - ProcessedSample2_nonpolar_metabolites
        has_output:
        - ProcessedSample3_nonpolar_metabolites
  processedsamples:
  - ProcessedSample1_nonpolar_metabolites:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_nonpolar_subsample
        description: A 2 g subsample of <Biosample> prepared for nonpolar metabolite
          extraction.
  - ProcessedSample2_nonpolar_metabolites:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_dried_nonpolar_extract
        description: Dried nonpolar metabolite extract obtained from <ProcessedSample1_nonpolar_metabolites>
          after methanol extraction and SpeedVac drying.
        sampled_portion: methanol_layer
  - ProcessedSample3_nonpolar_metabolites:
      ProcessedSample:
        id: null
        type: nmdc:ProcessedSample
        name: <Biosample>_resuspended_nonpolar_metabolites
        description: Resuspended dried nonpolar metabolite extracts from <ProcessedSample2_nonpolar_metabolites>
          for LC-MS analysis.
        sampled_portion: methanol_layer
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
"""
material_processing_protocols = yaml.safe_load(material_processing_protocols_yaml_str)

def parse_filename(filename):
    """
    Extracts metadata from a raw mass spectrometry filename based on defined patterns.

    Args:
        filename (str): The raw data file name.

    Returns:
        dict: A dictionary containing extracted fields:
              'raw_data_identifier', 'sample_id_parsed', 'timepoint_parsed',
              'replicate_parsed', 'method_parsed', 'extraction_type_parsed',
              'is_control'.
    """
    parsed_data = {
        'raw_data_identifier': filename,
        'sample_id_parsed': None,
        'timepoint_parsed': None,
        'replicate_parsed': None,
        'method_parsed': None,  # Chromatography method like HILICZ, C18
        'extraction_type_parsed': None, # Extraction solvent like H2O, MeOH, CHCl3, Lipids
        'is_control': False
    }

    # 1. Check for control samples first
    control_patterns = [
        r'QC', r'Blank', r'ExCtrl', r'Neg-D\d+-Ctrl', r'Sterile-sand', r'Sterile-BGramaLit'
    ]
    if any(re.search(p, filename, re.IGNORECASE) for p in control_patterns):
        parsed_data['is_control'] = True

    # 2. Extract Sample ID, Timepoint, Replicate
    # Pattern A: SXX-DYY_A/B/C (e.g., S32-D89_A)
    match_sdt = re.search(r'S(\d+)-D(\d+)_([ABC])', filename)
    if match_sdt:
        parsed_data['sample_id_parsed'] = f'S{match_sdt.group(1)}'
        parsed_data['timepoint_parsed'] = f'D{match_sdt.group(2)}'
        parsed_data['replicate_parsed'] = match_sdt.group(3)
    else:
        # Pattern B: Brodie_XXX_A/B/C (e.g., Brodie_134A)
        match_brodie_abc = re.search(r'Brodie_(\d+)([ABC])', filename, re.IGNORECASE)
        if match_brodie_abc:
            parsed_data['sample_id_parsed'] = f'ER_{match_brodie_abc.group(1)}' # Standardize to ER_XXX
            parsed_data['replicate_parsed'] = match_brodie_abc.group(2)
        else:
            # Pattern C: Brodie_XXX_rX (e.g., Brodie_115_r2)
            match_brodie_rx = re.search(r'Brodie_(\d+)_r(\d+)', filename, re.IGNORECASE)
            if match_brodie_rx:
                parsed_data['sample_id_parsed'] = f'ER_{match_brodie_rx.group(1)}' # Standardize to ER_XXX
                parsed_data['replicate_parsed'] = f'r{match_brodie_rx.group(2)}'
            else:
                # Pattern D: SXX (e.g. S16 from biosample list could be a standalone match)
                match_s = re.search(r'S(\d+)', filename)
                if match_s:
                    parsed_data['sample_id_parsed'] = f'S{match_s.group(1)}'
                
                # Pattern E: ER_XXX, often for Brodie samples (e.g. ER_369)
                match_er = re.search(r'ER_(\d+)', filename)
                if match_er:
                    parsed_data['sample_id_parsed'] = f'ER_{match_er.group(1)}'
                
                # Pattern F: Brodie_XXX_Lipids (e.g. Brodie_369_Lipids)
                match_brodie_lipids = re.search(r'Brodie_(\d+)_Lipids', filename, re.IGNORECASE)
                if match_brodie_lipids and not parsed_data['sample_id_parsed']: # If not already set by ER_XXX
                    parsed_data['sample_id_parsed'] = f'ER_{match_brodie_lipids.group(1)}'
    
    # Extract method (chromatography or extraction type)
    if re.search(r'HILICZ|HILIC', filename, re.IGNORECASE):
        parsed_data['method_parsed'] = 'HILICZ' # Standardize to HILICZ
    elif re.search(r'C18|RP', filename, re.IGNORECASE):
        parsed_data['method_parsed'] = 'C18' # Standardize to C18
    
    # Check for specific extraction types for NOM/Lipid protocols (Brodie files)
    if re.search(r'H2O|w_', filename, re.IGNORECASE): # 'w_' also indicates water extract
        parsed_data['extraction_type_parsed'] = 'H2O'
    elif re.search(r'MeOH', filename, re.IGNORECASE):
        parsed_data['extraction_type_parsed'] = 'MeOH'
    elif re.search(r'CHCl3', filename, re.IGNORECASE):
        parsed_data['extraction_type_parsed'] = 'CHCl3'
    elif re.search(r'Lipids', filename, re.IGNORECASE):
        parsed_data['extraction_type_parsed'] = 'Lipids'

    return parsed_data

def match_to_biosample(parsed_data, biosamples_df):
    """
    Matches parsed filename data to a biosample entry.

    Args:
        parsed_data (dict): Dictionary from parse_filename.
        biosamples_df (pd.DataFrame): DataFrame of biosamples with 'id' and 'name'.

    Returns:
        tuple: (biosample_id, biosample_name, match_confidence)
    """
    biosample_id = None
    biosample_name = None
    match_confidence = 'no_match'

    if parsed_data['is_control']:
        return None, None, 'control_file'

    sample_id = parsed_data['sample_id_parsed']
    timepoint = parsed_data['timepoint_parsed']
    replicate = parsed_data['replicate_parsed']
    method = parsed_data['method_parsed'] # HILICZ or C18

    if not sample_id:
        return biosample_id, biosample_name, 'no_sample_id_in_filename'

    # Determine potential polarity word from method for SXX biosamples
    potential_polarity_word = None
    if method == 'HILICZ':
        potential_polarity_word = 'hydrophilic'
    elif method == 'C18':
        potential_polarity_word = 'hydrophobic'

    # --- Matching Logic for SXX type biosamples ---
    if sample_id.startswith('S'):
        # Attempt high confidence match: SXX_REP_DYY hydrophilic/hydrophobic
        if replicate and timepoint and potential_polarity_word:
            full_pattern = rf'^{sample_id}_{replicate}_{timepoint} {re.escape(potential_polarity_word)}$'
            match = biosamples_df[biosamples_df['name'].str.contains(full_pattern, regex=True, case=False)]
            if not match.empty:
                return match.iloc[0]['id'], match.iloc[0]['name'], 'high'

        # Attempt medium confidence match: SXX_REP_DYY (ignoring polarity word, or if polarity not determined)
        if replicate and timepoint:
            medium_pattern = rf'^{sample_id}_{replicate}_{timepoint}'
            match = biosamples_df[biosamples_df['name'].str.contains(medium_pattern, regex=True, case=False)]
            if not match.empty:
                # Check for conflicting polarity words in biosample name if current method implies one
                bs_name = match.iloc[0]['name'].lower()
                if (potential_polarity_word == 'hydrophobic' and 'hydrophilic' in bs_name) or \
                   (potential_polarity_word == 'hydrophilic' and 'hydrophobic' in bs_name):
                    # Conflict, so this isn't a medium match, move to low
                    pass 
                else:
                    return match.iloc[0]['id'], match.iloc[0]['name'], 'medium'

        # Attempt low confidence match: SXX (match only sample ID, if no more specific info matches)
        low_pattern = rf'^{sample_id}$' # Exact match for SXX biosample name (e.g., "S16")
        match_low = biosamples_df[biosamples_df['name'].str.contains(low_pattern, regex=True, case=False)]
        if not match_low.empty:
            return match_low.iloc[0]['id'], match_low.iloc[0]['name'], 'low'
        
        # Fallback for SXX within longer names without specific DXX_ABC (e.g., from ER_ samples which also have SXX in description)
        # We assume the above explicit patterns catch the intended SXX biosamples.
        # This part handles cases where SXX might be found in a broader context but without the structured suffix.
        # Example: if a biosample name was "My sample S16 from D30" and the file only had S16.
        # For this specific dataset, biosample SXX are either SXX or SXX_A_DXX Hydrophilic/Hydrophobic
        # so direct regex matching is robust.

    # --- Matching Logic for ER_XXX type biosamples ---
    elif sample_id.startswith('ER_'):
        # For ER_XXX samples, the biosample name is typically "Description - ER_XXX"
        # So we look for "ER_XXX" within the biosample name.
        er_pattern = rf'{re.escape(sample_id)}$' # Match 'ER_XXX' at the end of the name
        match = biosamples_df[biosamples_df['name'].str.contains(er_pattern, regex=True, case=False)]
        if not match.empty:
            # Assuming any match for ER_XXX in biosample name is high confidence for these types
            return match.iloc[0]['id'], match.iloc[0]['name'], 'high'
        
        # Also try matching ER_XXX not necessarily at the end
        er_pattern_anywhere = rf'\b{re.escape(sample_id)}\b'
        match_anywhere = biosamples_df[biosamples_df['name'].str.contains(er_pattern_anywhere, regex=True, case=False)]
        if not match_anywhere.empty:
            return match_anywhere.iloc[0]['id'], match_anywhere.iloc[0]['name'], 'high'


    return biosample_id, biosample_name, match_confidence


def determine_protocol(parsed_data, material_processing_protocols_dict):
    """
    Determines the material processing protocol and processed sample placeholder
    based on parsed filename data.

    Args:
        parsed_data (dict): Dictionary from parse_filename.
        material_processing_protocols_dict (dict): Parsed YAML of material processing protocols.

    Returns:
        tuple: (protocol_id, processed_sample_placeholder)
    """
    protocol_id = None
    processed_sample_placeholder = None

    method = parsed_data['method_parsed']
    extraction_type = parsed_data['extraction_type_parsed']

    if method == 'HILICZ':
        protocol_id = 'polar_metabolites'
        processed_sample_placeholder = 'ProcessedSample3_polar_metabolites'
    elif method == 'C18':
        protocol_id = 'nonpolar_metabolites'
        processed_sample_placeholder = 'ProcessedSample3_nonpolar_metabolites'
    elif extraction_type == 'H2O':
        protocol_id = 'NOM'
        processed_sample_placeholder = 'ProcessedSample2_NOM'
    elif extraction_type == 'MeOH':
        protocol_id = 'NOM'
        processed_sample_placeholder = 'ProcessedSample4_NOM'
    elif extraction_type == 'CHCl3':
        protocol_id = 'NOM'
        processed_sample_placeholder = 'ProcessedSample6_NOM'
    elif extraction_type == 'Lipids':
        protocol_id = 'lipid'
        processed_sample_placeholder = 'ProcessedSample3_lipid'
    
    # Validate if the determined processed_sample_placeholder exists in the protocol
    if protocol_id and processed_sample_placeholder:
        if protocol_id in material_processing_protocols_dict:
            # The structure for processedsamples in the YAML is a list of dicts.
            # Each dict has one key which is the processed sample placeholder name.
            for ps_entry in material_processing_protocols_dict[protocol_id]['processedsamples']:
                if list(ps_entry.keys())[0] == processed_sample_placeholder:
                    return protocol_id, processed_sample_placeholder
        # If we reach here, the determined placeholder was not found in the YAML,
        # so return None for both.
        return None, None
            
    return None, None

# --- Main processing logic ---
def main():
    try:
        # Load input data
        biosamples_df = pd.read_csv(INPUT_BIOSAMPLE_PATH)
        files_df = pd.read_csv(INPUT_FILES_PATH)
    except FileNotFoundError as e:
        print(f"Error: Input file not found: {e}")
        return
    except KeyError:
        print(f"Error: Column '{RAW_FILE_NAME_COLUMN}' not found in {INPUT_FILES_PATH}")
        return

    results = []

    for index, row in files_df.iterrows():
        filename = row[RAW_FILE_NAME_COLUMN]
        
        parsed_data = parse_filename(filename)
        biosample_id, biosample_name, match_confidence = match_to_biosample(parsed_data, biosamples_df)
        protocol_id, processed_sample_placeholder = determine_protocol(parsed_data, material_processing_protocols)

        results.append({
            'raw_data_identifier': filename,
            'biosample_id': biosample_id,
            'biosample_name': biosample_name,
            'match_confidence': match_confidence,
            'processedsample_placeholder': processed_sample_placeholder,
            'material_processing_protocol_id': protocol_id
        })

    # Save output
    output_df = pd.DataFrame(results, columns=[
        'raw_data_identifier', 'biosample_id', 'biosample_name', 
        'match_confidence', 'processedsample_placeholder', 
        'material_processing_protocol_id'
    ])
    output_df.to_csv(OUTPUT_PATH, index=False)
    print(f"Mapping successfully generated and saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()