"""
Validation functions for biosample mapping CSV output.
"""
import csv
import io
import re
import yaml


def validate_biosample_mapping_csv(
    csv_content: str,
    biosample_attributes_csv: str,
    material_processing_yaml: str,
    raw_files_csv: str
) -> dict:
    """
    Validate the biosample mapping CSV against the input data.
    
    Parameters
    ----------
    csv_content : str
        The generated CSV mapping content
    biosample_attributes_csv : str
        The biosample attributes CSV content
    material_processing_yaml : str
        The material processing YAML content
    raw_files_csv : str
        The raw files CSV content
    
    Returns
    -------
    dict
        Validation result with:
        - 'valid' (bool): True if no errors found
        - 'errors' (list of str): Critical issues that must be fixed
        - 'warnings' (list of str): Non-critical issues for review
        - 'unmapped_files' (list of str): Raw files not mapped to biosamples
    """
    errors = []
    
    # Parse the generated CSV
    try:
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        generated_rows = list(csv_reader)
    except Exception as e:
        return {
            'valid': False,
            'errors': [f"Failed to parse generated CSV: {str(e)}"]
        }
    
    # Check required columns
    required_columns = {
        'raw_data_identifier',
        'biosample_id',
        'biosample_name',
        'match_confidence',
        'processedsample_placeholder',
        'material_processing_protocol_id'
    }
    
    if not generated_rows:
        errors.append("Generated CSV is empty (no data rows)")
    else:
        actual_columns = set(generated_rows[0].keys())
        missing_columns = required_columns - actual_columns
        if missing_columns:
            errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        
        extra_columns = actual_columns - required_columns
        if extra_columns:
            errors.append(f"Unexpected columns: {', '.join(extra_columns)}")
    
    # Parse biosample attributes
    try:
        biosample_reader = csv.DictReader(io.StringIO(biosample_attributes_csv))
        biosample_rows = list(biosample_reader)
        biosample_map = {row['id']: row for row in biosample_rows}
    except Exception as e:
        return {
            'valid': False,
            'errors': [f"Failed to parse biosample attributes CSV: {str(e)}"]
        }
    
    # Parse raw files
    try:
        raw_files_reader = csv.DictReader(io.StringIO(raw_files_csv))
        raw_files_rows = list(raw_files_reader)
        # Handle both 'file_name' and 'raw_data_file_name' column names
        if raw_files_rows:
            if 'file_name' in raw_files_rows[0]:
                raw_file_names = {row['file_name'] for row in raw_files_rows}
            elif 'raw_data_file_name' in raw_files_rows[0]:
                raw_file_names = {row['raw_data_file_name'] for row in raw_files_rows}
            else:
                # Use first column if neither name is found
                first_col = list(raw_files_rows[0].keys())[0]
                raw_file_names = {row[first_col] for row in raw_files_rows}
        else:
            raw_file_names = set()
    except Exception as e:
        return {
            'valid': False,
            'errors': [f"Failed to parse raw files CSV: {str(e)}"]
        }
    
    # Parse YAML
    try:
        yaml_data = yaml.safe_load(material_processing_yaml)
        protocol_names = set(yaml_data.keys())
        
        # Extract all ProcessedSample IDs from the YAML
        processed_samples = set()
        for protocol_name, protocol_data in yaml_data.items():
            if 'processedsamples' in protocol_data:
                for ps_item in protocol_data['processedsamples']:
                    processed_samples.update(ps_item.keys())
    except Exception as e:
        return {
            'valid': False,
            'errors': [f"Failed to parse material processing YAML: {str(e)}"]
        }
    
    # Validate each row
    mapped_raw_files = set()
    nmdc_biosample_id_pattern = re.compile(r'^nmdc:bsm-\d+-[a-z0-9]+$')
    
    for i, row in enumerate(generated_rows, start=2):  # Start at 2 to account for header
        row_num = f"Row {i}"
        
        # Check raw_data_identifier
        raw_id = row.get('raw_data_identifier', '').strip()
        if not raw_id:
            errors.append(f"{row_num}: Missing raw_data_identifier")
        else:
            mapped_raw_files.add(raw_id)
            if raw_id not in raw_file_names:
                errors.append(f"{row_num}: raw_data_identifier '{raw_id}' not found in raw files CSV")
        
        # Check biosample_id
        biosample_id = row.get('biosample_id', '').strip()
        if biosample_id:  # Allow empty for QC/control samples
            if not nmdc_biosample_id_pattern.match(biosample_id):
                errors.append(f"{row_num}: biosample_id '{biosample_id}' does not match NMDC format (nmdc:bsm-XX-XXXXXXXX)")
            elif biosample_id not in biosample_map:
                errors.append(f"{row_num}: biosample_id '{biosample_id}' not found in biosample attributes CSV")
            else:
                # Check biosample_name matches
                expected_name = biosample_map[biosample_id].get('name', '')
                actual_name = row.get('biosample_name', '').strip()
                if expected_name and actual_name and expected_name != actual_name:
                    errors.append(f"{row_num}: biosample_name '{actual_name}' does not match expected '{expected_name}' for biosample_id '{biosample_id}'")
        
        # Check match_confidence
        match_confidence = row.get('match_confidence', '').strip()
        if match_confidence not in ['high', 'medium', 'low', '']:
            errors.append(f"{row_num}: match_confidence must be 'high', 'medium', 'low', or empty (got '{match_confidence}')")
        
        # Check processedsample_placeholder
        ps_placeholder = row.get('processedsample_placeholder', '').strip()
        if ps_placeholder and ps_placeholder not in processed_samples:
            errors.append(f"{row_num}: processedsample_placeholder '{ps_placeholder}' not found in material processing YAML")
        
        # Check material_processing_protocol_id
        protocol_id = row.get('material_processing_protocol_id', '').strip()
        if protocol_id and protocol_id not in protocol_names:
            errors.append(f"{row_num}: material_processing_protocol_id '{protocol_id}' not found in material processing YAML (available: {', '.join(protocol_names)})")
    
    # Check that all raw files are mapped (warning only, not an error)
    unmapped_files = raw_file_names - mapped_raw_files
    warnings = []
    if unmapped_files:
        warnings.append(f"Unmapped raw files ({len(unmapped_files)}): These files could not be mapped to biosamples (may be QC, blanks, standards, etc.)")
    
    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings,
        'unmapped_files': sorted(list(unmapped_files))
    }
