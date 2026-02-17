import pandas as pd
import re
import yaml

# --- Configuration ---
INPUT_BIOSAMPLE_PATH = 'nmdc_dp_utils/llm/examples/example_1/biosample_attributes.csv'
INPUT_FILES_PATH = 'nmdc_dp_utils/llm/examples/example_1/downloaded_files.csv'
OUTPUT_PATH = 'nmdc_dp_utils/llm/examples/example_1/llm_generated_mapping_codegen.csv'
RAW_FILE_NAME_COLUMN = 'raw_data_file_name'

# --- Material Processing YAML (for reference and logic derivation) ---
# This YAML is provided in the problem description and is used to derive
# the processed sample mapping logic within the determine_protocol function.
# It is not loaded programmatically from a file in this script, but its
# content directly informs the conditional logic.
MATERIAL_PROCESSING_YAML_STR = """
NOM:
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
# Load YAML content for protocol structure details, although specific processed sample
# IDs are mostly hardcoded based on example output for Brodie files.
material_processing_config = yaml.safe_load(MATERIAL_PROCESSING_YAML_STR)

# --- Functions ---

def parse_filename(filename):
    """
    Extracts metadata from a given filename based on predefined patterns.
    Distinguishes between "Brodie" and "1000S" file naming conventions.
    
    Args:
        filename (str): The raw data file name.
        
    Returns:
        dict: A dictionary containing extracted metadata such as sample_id_full,
              sample_id_num, location, replicate, method, ionization, and file_type.
    """
    data = {
        'raw_data_identifier': filename,
        'sample_id_full': None,
        'sample_id_num': None,
        'location': None,
        'replicate': None,
        'method': None,
        'ionization': None,
        'file_type': None
    }

    # Pattern for Brodie files (e.g., Brodie_134A_CHCl3..., Brodie_369_Lipids..., Brodie_115_w_r2...)
    # Captures "Brodie_NUMBER" and optional "LETTER" (replicate A, B, C)
    brodie_match = re.search(r'Brodie_(\d+)([ABC]?)', filename, re.IGNORECASE)
    
    if brodie_match:
        data['file_type'] = 'Brodie'
        data['sample_id_num'] = brodie_match.group(1)
        
        # Biosamples for Brodie are named ER_XXX
        data['sample_id_full'] = f"ER_{data['sample_id_num']}"

        # Extract replicate if present (e.g., 134A -> A)
        if brodie_match.group(2):
            data['replicate'] = brodie_match.group(2).upper()
        
        # Try to extract replicate in the format _rX or _RX
        replicate_r_match = re.search(r'[_-][rR](\d+)', filename)
        if replicate_r_match:
            data['replicate'] = replicate_r_match.group(1) # e.g. '2' from 'r2'

        # Determine method for Brodie files
        if 'Lipids' in filename:
            data['method'] = 'Lipids'
        elif 'H2O_SPE' in filename:
            data['method'] = 'H2O_SPE'
        elif '_w_' in filename or '_H2O' in filename: # Brodie_XXX_w_rX or Brodie_XXX_H2O_
            data['method'] = 'H2O'
        elif 'MeOH' in filename:
            data['method'] = 'MeOH'
        elif 'CHCl3' in filename:
            data['method'] = 'CHCl3'
        
        # Determine ionization for Brodie files
        if 'POS' in filename:
            data['ionization'] = 'POS'
        elif 'Neg' in filename or 'neg' in filename:
            data['ionization'] = 'NEG'

    # Pattern for 1000S files (e.g., 1000S_CFS1_FTMS_SPE_BTM_1_run1...)
    # Captures 4-letter sample code, TOP/BTM location, and replicate number (1, 2, or 3)
    if not data['file_type']: # Only attempt if not already matched as Brodie
        thousands_match = re.search(r'1000S_([A-Z]{3,4})_.*?_(TOP|BTM)_(?:(\d+))?_run(\d+)', filename, re.IGNORECASE)
        if thousands_match:
            data['file_type'] = '1000S'
            data['sample_id_num'] = thousands_match.group(1) # e.g., CFS1
            data['location'] = thousands_match.group(2).upper()
            
            # Biosamples for 1000S are named ABC_CoreB_TOP/BTM
            data['sample_id_full'] = f"{data['sample_id_num']}_CoreB_{data['location']}"

            # Replicate is captured as the digit before '_run'
            if thousands_match.group(3):
                data['replicate'] = thousands_match.group(3) # e.g. '1' from '_1_run1'
            else: # Fallback using run number if specific replicate number isn't found
                data['replicate'] = thousands_match.group(4) # e.g. '1' from '_run1'
                
            # Method for 1000S files (consistent based on examples)
            data['method'] = 'FTMS_SPE'

    return data

def match_to_biosample(parsed_data, biosamples_df):
    """
    Matches parsed filename data to biosample attributes.
    
    Args:
        parsed_data (dict): Dictionary of metadata extracted from filename.
        biosamples_df (pd.DataFrame): DataFrame containing biosample ID and name.
        
    Returns:
        tuple: (biosample_id, biosample_name, confidence)
               Returns (None, None, 'low') if no match or for QC/Blank files.
    """
    biosample_id = None
    biosample_name = None
    confidence = 'low'

    # Edge case: QC/Blank/Control files
    qc_patterns = [r'QC', r'Blank', r'ExCtrl', r'Control', r'std']
    if any(re.search(p, parsed_data['raw_data_identifier'], re.IGNORECASE) for p in qc_patterns):
        return None, None, 'low'

    if parsed_data['sample_id_full']:
        # Construct search pattern. Biosample names are typically "Description - ID"
        search_pattern = f" - {parsed_data['sample_id_full']}"
        
        matches = biosamples_df[biosamples_df['name'].str.contains(search_pattern, regex=False, na=False)]
        
        if not matches.empty:
            biosample_id = matches['id'].iloc[0]
            biosample_name = matches['name'].iloc[0]
            confidence = 'high'
            
    return biosample_id, biosample_name, confidence

def determine_protocol(parsed_data):
    """
    Determines the material processing protocol and processed sample placeholder
    based on the extracted analytical method and file type.
    
    This function implements logic based on the provided YAML structure for
    '1000S' files, and inferred logic from example output for 'Brodie' files
    due to observed discrepancies in processed sample IDs.
    
    Args:
        parsed_data (dict): Dictionary of metadata extracted from filename.
        
    Returns:
        tuple: (protocol_id, processed_sample_placeholder)
    """
    protocol = None
    processed_sample = None
    
    method = parsed_data.get('method')
    replicate = parsed_data.get('replicate')
    file_type = parsed_data.get('file_type')

    if file_type == 'Brodie':
        # Logic for Brodie files derived from example output, as their processed_sample_placeholder
        # IDs do not directly align with the provided 'NOM' YAML for the 1000S data.
        if method == 'Lipids':
            protocol = 'lipid'
            processed_sample = 'ProcessedSample3_lipid'
        elif method == 'H2O' or method == 'H2O_SPE': # Covers _w_, H2O_, and H2O_SPE based on example mapping
            protocol = 'NOM'
            processed_sample = 'ProcessedSample2_NOM'
        elif method == 'MeOH':
            protocol = 'NOM'
            processed_sample = 'ProcessedSample4_NOM'
        elif method == 'CHCl3':
            protocol = 'NOM'
            processed_sample = 'ProcessedSample6_NOM'
        
    elif file_type == '1000S':
        # Logic for 1000S files strictly follows the provided NOM YAML
        if method == 'FTMS_SPE':
            protocol = 'NOM'
            # Map replicate number to specific ProcessedSample_NOM from the YAML
            if replicate == '1':
                processed_sample = 'ProcessedSample5_NOM' # SPE product from Subsample 1
            elif replicate == '2':
                processed_sample = 'ProcessedSample7_NOM' # SPE product from Subsample 2
            elif replicate == '3':
                processed_sample = 'ProcessedSample9_NOM' # SPE product from Subsample 3
            else:
                # Default for cases where replicate is not 1, 2, or 3 (e.g., 'run1' but no explicit subsample #)
                processed_sample = 'ProcessedSample5_NOM' 
    
    return protocol, processed_sample

# --- Main Processing ---

def main():
    try:
        biosamples = pd.read_csv(INPUT_BIOSAMPLE_PATH)
    except FileNotFoundError:
        print(f"Error: Biosample file not found at {INPUT_BIOSAMPLE_PATH}")
        return
    except Exception as e:
        print(f"Error loading biosample file: {e}")
        return

    try:
        files = pd.read_csv(INPUT_FILES_PATH)
    except FileNotFoundError:
        print(f"Error: Raw files list not found at {INPUT_FILES_PATH}")
        return
    except Exception as e:
        print(f"Error loading raw files list: {e}")
        return
    
    if RAW_FILE_NAME_COLUMN not in files.columns:
        print(f"Error: Column '{RAW_FILE_NAME_COLUMN}' not found in {INPUT_FILES_PATH}")
        return

    results = []
    for filename in files[RAW_FILE_NAME_COLUMN]:
        parsed_data = parse_filename(filename)
        
        biosample_id, biosample_name, match_confidence = match_to_biosample(parsed_data, biosamples)
        
        protocol_id, processed_sample_placeholder = determine_protocol(parsed_data)
        
        results.append({
            'raw_data_identifier': filename,
            'biosample_id': biosample_id if biosample_id else '',
            'biosample_name': biosample_name if biosample_name else '',
            'match_confidence': match_confidence,
            'processedsample_placeholder': processed_sample_placeholder if processed_sample_placeholder else '',
            'material_processing_protocol_id': protocol_id if protocol_id else ''
        })

    output_df = pd.DataFrame(results, columns=[
        'raw_data_identifier', 'biosample_id', 'biosample_name', 
        'match_confidence', 'processedsample_placeholder', 
        'material_processing_protocol_id'
    ])

    try:
        output_df.to_csv(OUTPUT_PATH, index=False)
        print(f"Mapping successfully generated and saved to {OUTPUT_PATH}")
    except Exception as e:
        print(f"Error saving output file: {e}")

if __name__ == "__main__":
    main()