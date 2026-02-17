#!/usr/bin/env python3
"""
Generate standardized input files for LLM-based material processing extraction.

For each example_* directory containing combined_input.csv and study_id.txt, generates:
  - biosample_attributes.csv: Biosamples from NMDC API (by study ID)
  - downloaded_files.csv: Raw data file list
  - nmdc_id_to_filename_mapping.csv: NMDC ID to filename mapping (if needed)
  - combined_inputs_v2.csv: Standardized format with biosample names and metadata

Uses nmdc_api_utilities to fetch data from NMDC database.
"""

import csv
import pandas as pd
from pathlib import Path
from typing import Set, Dict
from nmdc_api_utilities.biosample_search import BiosampleSearch
from nmdc_api_utilities.data_generation_search import DataGenerationSearch
from nmdc_api_utilities.data_object_search import DataObjectSearch


def is_filename(raw_data_id: str) -> bool:
    """
    Check if raw_data_identifier looks like a filename (not an NMDC ID).
    
    Returns True if it contains file extensions like .raw, .mzML, or doesn't have nmdc: prefix.
    """
    raw_data_id = raw_data_id.lower()
    return (
        '.raw' in raw_data_id or 
        '.mzml' in raw_data_id or 
        '.d' in raw_data_id or
        not raw_data_id.startswith('nmdc:')
    )


def generate_biosample_attributes(
    combined_input_path: Path,
    study_id: str,
    output_path: Path
) -> int:
    """
    Generate biosample_attributes.csv from NMDC API using study ID.
    
    Returns:
        Number of biosamples retrieved, or 0 if failed
    """
    
    biosample_search = BiosampleSearch()
    
    try:
        # Query by associated_studies field (same as NMDCWorkflowBiosampleManager)
        filter_str = f'{{"associated_studies":"{study_id}"}}'
        
        biosamples = biosample_search.get_record_by_filter(
            filter=filter_str,
            max_page_size=1000,
            fields="id,name,samp_name,description,gold_biosample_identifiers,insdc_biosample_identifiers,submitter_id,analysis_type",
            all_pages=True,
        )
        
        if not biosamples:
            print(f"  ❌ No biosample data retrieved from NMDC API for study {study_id}")
            return 0
        
    except Exception as e:
        print(f"  ❌ Error fetching biosamples: {type(e).__name__}: {e}")
        return 0
    
    # Convert to DataFrame and save
    biosample_df = pd.DataFrame(biosamples)   
    biosample_df.to_csv(output_path, index=False)
    return len(biosample_df)


def generate_downloaded_files(
    combined_input_path: Path,
    output_path: Path
) -> Dict[str, str]:
    """
    Generate downloaded_files.csv from combined_input.csv.
    Resolves NMDC IDs to filenames via API if needed.
    
    Returns:
        Dictionary mapping NMDC IDs to filenames
    """
    import json
    
    files: Set[str] = set()
    nmdc_id_to_filename: Dict[str, str] = {}  # Track mapping for combined_input update
    data_gen_search = DataGenerationSearch()
    data_obj_search = DataObjectSearch()
    
    # Track NMDC IDs that need API resolution
    nmdc_ids_to_resolve = []
    
    with open(combined_input_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_data_id = row['raw_data_identifier']
            
            # Check if it's already a filename
            if is_filename(raw_data_id):
                files.add(raw_data_id)
            else:
                # It's an NMDC ID - need to resolve via API
                nmdc_ids_to_resolve.append(raw_data_id)
    
    # Resolve NMDC IDs to filenames via API
    if nmdc_ids_to_resolve:
        try:
            # Step 1: Query DataGeneration for all NMDC IDs at once using $in operator
            nmdc_ids_list = list(set(nmdc_ids_to_resolve))  # Deduplicate
            id_list_json = json.dumps(nmdc_ids_list)
            filter_str = f'{{"id":{{"$in":{id_list_json}}}}}'
            
            data_gens = data_gen_search.get_record_by_filter(
                filter=filter_str,
                max_page_size=1000,
                fields="id,has_output",
                all_pages=True,
            )
            
            if not data_gens:
                print("  ⚠ No data generation records found for NMDC IDs")
            else:
                # Build mapping of data_obj_id -> nmdc_id (for reverse lookup later)
                data_obj_to_nmdc: Dict[str, str] = {}
                data_obj_ids = []
                
                for data_gen in data_gens:
                    nmdc_id = data_gen.get('id')
                    has_output = data_gen.get('has_output', [])
                    if has_output:
                        # has_output can be a list of IDs
                        if isinstance(has_output, list):
                            for obj_id in has_output:
                                data_obj_ids.append(obj_id)
                                data_obj_to_nmdc[obj_id] = nmdc_id
                        else:
                            data_obj_ids.append(has_output)
                            data_obj_to_nmdc[has_output] = nmdc_id
                
                if data_obj_ids:
                    # Step 2: Query DataObject for all object IDs at once
                    data_obj_ids_list = list(set(data_obj_ids))  # Deduplicate
                    obj_id_list_json = json.dumps(data_obj_ids_list)
                    filter_str = f'{{"id":{{"$in":{obj_id_list_json}}}}}'
                    
                    data_objs = data_obj_search.get_record_by_filter(
                        filter=filter_str,
                        max_page_size=1000,
                        fields="id,name",
                        all_pages=True,
                    )
                    
                    if data_objs:
                        for data_obj in data_objs:
                            obj_id = data_obj.get('id')
                            filename = data_obj.get('name')
                            if filename and obj_id in data_obj_to_nmdc:
                                files.add(filename)
                                # Map the original NMDC ID to this filename
                                nmdc_id = data_obj_to_nmdc[obj_id]
                                nmdc_id_to_filename[nmdc_id] = filename
                    else:
                        print("  ⚠ No data objects found")
                else:
                    print("  ⚠ No data object IDs in has_output fields")
                    
        except Exception as e:
            print(f"  ⚠ Error resolving NMDC IDs: {type(e).__name__}: {e}")
    
    # Write downloaded_files.csv with just raw file names (no path)
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['raw_data_file_name'])  # Single column header
        for filename in sorted(files):
            # Extract actual file name after last '/' if present
            if '/' in filename:
                writer.writerow([filename.split('/')[-1]])
            else:
                writer.writerow([filename])
    return nmdc_id_to_filename


def process_example_directory(example_dir: Path) -> None:
    """
    Process example directory to generate standardized input files.
    Prints single summary line per example.
    """
    combined_input = example_dir / 'combined_input.csv'
    study_id_file = example_dir / 'study_id.txt'
    biosample_output = example_dir / 'biosample_attributes.csv'
    
    if not combined_input.exists():
        print(f"⚠ Skipping {example_dir.name}: combined_input.csv not found")
        return
    
    if not study_id_file.exists():
        print(f"⚠ Skipping {example_dir.name}: study_id.txt not found")
        print(f"  Create {study_id_file} with NMDC study ID (e.g., 'nmdc:sty-11-34xj1150')")
        return
    
    # Read study ID
    with open(study_id_file, 'r') as f:
        study_id = f.read().strip()
    
    if not study_id:
        print(f"⚠ Skipping {example_dir.name}: study_id.txt is empty")
        return
    
    # Track what was generated for summary
    generated = []
    
    # Generate biosample_attributes.csv using study ID (skip if already exists)
    biosample_count = 0
    if not biosample_output.exists():
        biosample_count = generate_biosample_attributes(combined_input, study_id, biosample_output)
        if biosample_count > 0:
            generated.append(f"{biosample_count} biosamples")
    
    # Generate downloaded_files.csv and get NMDC ID to filename mapping
    files_output = example_dir / 'downloaded_files.csv'
    nmdc_id_to_filename = generate_downloaded_files(combined_input, files_output)
    
    # Count files for summary
    with open(files_output, 'r') as f:
        file_count = sum(1 for _ in f) - 1  # Subtract header
    generated.append(f"{file_count} files")
    
    # Write out the NMDC ID to filename mapping
    if nmdc_id_to_filename:
        mapping_output = example_dir / 'nmdc_id_to_filename_mapping.csv'
        mapping_df = pd.DataFrame([
            {'nmdc_id': nmdc_id, 'raw_file_name': filename}
            for nmdc_id, filename in sorted(nmdc_id_to_filename.items())
        ])
        mapping_df.to_csv(mapping_output, index=False)
    
    # Always create combined_inputs_v2.csv with standardized structure
    df = pd.read_csv(combined_input)
    
    # Replace NMDC IDs with filenames if we have mappings
    if nmdc_id_to_filename:
        df['raw_data_identifier'] = df['raw_data_identifier'].apply(
            lambda x: nmdc_id_to_filename.get(x, x).split('/')[-1] if '/' in nmdc_id_to_filename.get(x, x) else nmdc_id_to_filename.get(x, x)
        )
    else:
        # Already filenames, just strip paths if present
        df['raw_data_identifier'] = df['raw_data_identifier'].apply(
            lambda x: x.split('/')[-1] if '/' in x else x
        )

    biosample_name_map = {}
    biosample_attr_path = example_dir / 'biosample_attributes.csv'
    if biosample_attr_path.exists():
        try:
            biosample_df = pd.read_csv(biosample_attr_path)
            if 'id' in biosample_df.columns and 'name' in biosample_df.columns:
                biosample_name_map = dict(zip(biosample_df['id'], biosample_df['name']))
        except Exception as e:
            print(f"  ⚠ Could not read biosample_attributes.csv for biosample_name mapping: {e}")

    df['biosample_name'] = df['biosample_id'].map(biosample_name_map).fillna('')
    df['match_confidence'] = 'high'
    columns = [
        'raw_data_identifier',
        'biosample_id',
        'biosample_name',
        'match_confidence',
        'processedsample_placeholder',
        'material_processing_protocol_id'
    ]
    for col in columns:
        if col not in df.columns:
            df[col] = ''
    df = df[columns]
    combined_input_v2 = example_dir / 'combined_inputs_v2.csv'
    df.to_csv(combined_input_v2, index=False)
    
    # Print single summary line
    print(f"✓ {example_dir.name}: {', '.join(generated)}")


def main():
    """Process all example directories to generate standardized input files."""
    script_dir = Path(__file__).parent
    
    # Find all example directories
    example_dirs = sorted([
        d for d in script_dir.iterdir() 
        if d.is_dir() and d.name.startswith('example_')
    ])
    
    if not example_dirs:
        print("❌ No example directories found!")
        return
    
    for example_dir in example_dirs:
        process_example_directory(example_dir)
    
    print(f"\n✅ Processed {len(example_dirs)} examples")


if __name__ == '__main__':
    main()
