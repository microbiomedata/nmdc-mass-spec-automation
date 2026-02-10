#!/usr/bin/env python3
"""
Script to generate biosample_attributes.csv and downloaded_files.csv 
from combined_input.csv for each example in llm_protocol_context.

This prepares the standardized input structure for LLM-based material processing extraction:
1. biosample_attributes.csv - ALL biosamples for the study from NMDC API (by study ID)
2. downloaded_files.csv - list of raw data files
3. nmdc_id_to_filename_mapping.csv - mapping of NMDC IDs to resolved filenames
4. combined_input.csv - UPDATED to replace NMDC IDs with raw file names
5. combined_outline.yaml - material processing protocol (already exists)

Requirements for each example directory:
- combined_input.csv: Raw data mapping (biosample_id, raw_data_identifier, etc.)
- study_id.txt: NMDC study ID (e.g., 'nmdc:sty-11-34xj1150')

Uses nmdc_api_utilities to fetch real data from NMDC database:
- BiosampleSearch: Fetch ALL biosamples for study (by associated_studies field)
- DataGenerationSearch: Get data object IDs from NMDC workflow IDs
- DataObjectSearch: Get filenames from data object IDs
"""

import csv
import os
import pandas as pd
from pathlib import Path
from typing import Set, List, Dict
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
) -> None:
    """
    Generate biosample_attributes.csv using NMDC API by querying for study ID.
    
    Queries the NMDC API for all biosamples associated with the study,
    matching the behavior of NMDCWorkflowBiosampleManager.get_biosample_attributes().
    This returns ALL biosamples for the study, not just ones with data files.
    
    Args:
        combined_input_path: Path to combined_input.csv (not used, kept for consistency)
        study_id: NMDC study ID (e.g., 'nmdc:sty-11-34xj1150')
        output_path: Path to write biosample_attributes.csv
    
    Fields returned by NMDC API:
    - id: NMDC biosample identifier
    - name: Short name for the biosample
    - samp_name: Sample name
    - analysis_type: List of analysis types
    - gold_biosample_identifiers: External GOLD identifiers
    """
    import json
    
    print(f"  Fetching biosample attributes for study: {study_id}")
    
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
            return
            
        print(f"  Retrieved {len(biosamples)} biosamples from NMDC API")
        
    except Exception as e:
        print(f"  ❌ Error fetching biosamples from NMDC API: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Convert to DataFrame and save
    biosample_df = pd.DataFrame(biosamples)   
    biosample_df.to_csv(output_path, index=False)
    
    print(f"  ✓ Generated {output_path.name} with {len(biosample_df)} biosamples from NMDC API")


def generate_downloaded_files(
    combined_input_path: Path,
    output_path: Path
) -> Dict[str, str]:
    """
    Generate downloaded_files.csv from combined_input.csv.
    
    Extracts filenames from raw_data_identifier column:
    - If raw_data_identifier is already a filename, uses it directly
    - If it's an NMDC ID (e.g., nmdc:omprc-11-...), queries NMDC API:
      1. Uses DataGenerationSearch to get has_output field (data object IDs)
      2. Uses DataObjectSearch to get name field (filename) for each data object
    
    Returns:
        Dictionary mapping NMDC IDs to filenames for updating combined_input.csv
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
        print(f"  Resolving {len(nmdc_ids_to_resolve)} NMDC IDs to filenames via API...")
        
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
                print(f"  ⚠ Warning: No data generation records found for NMDC IDs")
            else:
                print(f"  Retrieved {len(data_gens)} data generation records")
                
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
                    print(f"  Found {len(data_obj_ids)} data object IDs to resolve")
                    
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
                        print(f"  Retrieved {len(data_objs)} data objects")
                        for data_obj in data_objs:
                            obj_id = data_obj.get('id')
                            filename = data_obj.get('name')
                            if filename and obj_id in data_obj_to_nmdc:
                                files.add(filename)
                                # Map the original NMDC ID to this filename
                                nmdc_id = data_obj_to_nmdc[obj_id]
                                nmdc_id_to_filename[nmdc_id] = filename
                    else:
                        print(f"  ⚠ Warning: No data objects found")
                else:
                    print(f"  ⚠ Warning: No data object IDs in has_output fields")
                    
        except Exception as e:
            print(f"  ⚠ Error resolving NMDC IDs: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
    
    # Write downloaded_files.csv with just raw file names
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['raw_data_file_name'])  # Single column header
        
        for filename in sorted(files):
            writer.writerow([filename])
    
    print(f"  ✓ Generated {output_path.name} with {len(files)} files")
    
    return nmdc_id_to_filename


def process_example_directory(example_dir: Path) -> None:
    """
    Process a single example directory to generate input files.
    
    Requires:
    - combined_input.csv: Raw data mapping
    - study_id.txt: NMDC study ID (e.g., 'nmdc:sty-11-34xj1150')
    
    Skips if biosample_attributes.csv already exists.
    """
    combined_input = example_dir / 'combined_input.csv'
    study_id_file = example_dir / 'study_id.txt'
    biosample_output = example_dir / 'biosample_attributes.csv'
    
    # Skip if biosample_attributes.csv already exists
    if biosample_output.exists():
        print(f"⏭  Skipping {example_dir.name}: biosample_attributes.csv already exists")
        return
    
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
    
    print(f"\n📁 Processing {example_dir.name}...")
    print(f"  Study ID: {study_id}")
    
    # Generate biosample_attributes.csv using study ID
    generate_biosample_attributes(combined_input, study_id, biosample_output)
    
    # Generate downloaded_files.csv and get NMDC ID to filename mapping
    files_output = example_dir / 'downloaded_files.csv'
    nmdc_id_to_filename = generate_downloaded_files(combined_input, files_output)
    
    # Write out the NMDC ID to filename mapping
    if nmdc_id_to_filename:
        mapping_output = example_dir / 'nmdc_id_to_filename_mapping.csv'
        mapping_df = pd.DataFrame([
            {'nmdc_id': nmdc_id, 'raw_file_name': filename}
            for nmdc_id, filename in sorted(nmdc_id_to_filename.items())
        ])
        mapping_df.to_csv(mapping_output, index=False)
        print(f"  ✓ Generated {mapping_output.name} with {len(mapping_df)} mappings")
    
    # Update combined_input.csv to replace NMDC IDs with raw file names
    if nmdc_id_to_filename:
        print(f"  Updating combined_input.csv to use raw file names...")
        
        # Read the original combined_input
        df = pd.read_csv(combined_input)
        
        # Replace NMDC IDs with filenames in raw_data_identifier column
        df['raw_data_identifier'] = df['raw_data_identifier'].apply(
            lambda x: nmdc_id_to_filename.get(x, x)  # Replace if in mapping, else keep original
        )
        
        # Write updated combined_input back
        df.to_csv(combined_input, index=False)
        print(f"  ✓ Updated {combined_input.name} with raw file names")


def main():
    """
    Process all example directories to generate standardized input files.
    
    For each example_* directory:
    Requires:
    - combined_input.csv: Raw data mapping
    - study_id.txt: NMDC study ID
    
    Generates:
    1. biosample_attributes.csv - ALL biosamples for study from NMDC API
       (queries by study ID using associated_studies field, same as NMDCWorkflowBiosampleManager)
    2. downloaded_files.csv - Raw file names
       (resolves NMDC IDs to filenames via DataGenerationSearch and DataObjectSearch)
    3. nmdc_id_to_filename_mapping.csv - NMDC ID to filename mapping
    4. Updates combined_input.csv to replace NMDC IDs with raw file names
    
    These examples use real NMDC data from the database, matching the exact
    structure used by the NMDCWorkflowBiosampleManager class in workflow_manager_mixins.py
    """
    script_dir = Path(__file__).parent
    
    print("=" * 70)
    print("Generating biosample_attributes.csv and downloaded_files.csv")
    print("from combined_input.csv for all examples")
    print("=" * 70)
    print("\nFetching real biosample data from NMDC API...")
    print("\nNOTE: Each example directory must contain:")
    print("  - combined_input.csv")
    print("  - study_id.txt (NMDC study ID, e.g., 'nmdc:sty-11-34xj1150')")
    
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
    
    print("\n" + "=" * 70)
    print(f"✅ Complete! Processed {len(example_dirs)} examples")
    print("=" * 70)
    print("\nGenerated files structure for each example:")
    print("  - biosample_attributes.csv: ALL biosamples for study from NMDC API")
    print("  - downloaded_files.csv: Raw data file list")
    print("  - nmdc_id_to_filename_mapping.csv: NMDC ID to filename mapping")
    print("  - combined_input.csv: UPDATED with raw file names (replaced NMDC IDs)")
    print("  - combined_outline.yaml: Material processing protocol (already exists)")
    print("\nThese inputs are used for LLM-based material processing extraction.")


if __name__ == '__main__':
    main()
