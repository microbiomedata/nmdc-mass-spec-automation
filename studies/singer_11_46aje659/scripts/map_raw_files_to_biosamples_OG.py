#!/usr/bin/env python3
"""
singer_11_46aje659 - Map raw data files to NMDC biosamples

This script maps raw LC-MS data files to NMDC biosamples for the study:
"Panicgrass rhizosphere soil microbial communities from growth chamber in LBNL, Berkeley, California, USA"

The mapping strategy is study-specific and may need to be customized based on:
- File naming conventions
- Sample metadata available
- Biosample attributes from NMDC

Run from the root data_processing directory:
python singer_11_46aje659/scripts/map_raw_files_to_biosamples_TEMPLATE.py
"""

import sys
import pandas as pd
from pathlib import Path

# Add the utils directory to path
sys.path.append(str(Path.cwd() / "nmdc_dp_utils"))

from study_manager import NMDCWorkflowManager


def extract_sample_info_from_filename(filename):
    """
    Extract sample information from raw data filename for Singer study.
    
    Filenames contain patterns like: 
    - Plot ID: ...root-F2-S1... or ...root-I14-S1...
    - Treatment info: ...17_A-C-2_3... which maps to biosample pattern like G2_A_2_control
    
    Example: 20220203_JGI-AK-TH_ES_504603_EcoPod_final_IDX_C18_USDAY59443_NEG_MSMS_17_A-C-2_3_Rg80to1200-CE102040-root-G2-S1_Run173.raw
    Should extract: plot_id=G2, treatment_letter=A, treatment_number=2, treatment_type=control
    
    Args:
        filename: Raw data filename 
        
    Returns:
        Dictionary with extracted sample information
    """
    # Remove file extension
    base_name = Path(filename).stem
    
    # Initialize sample info dictionary
    sample_info = {
        'raw_filename': filename,
        'plot_id': None,
        'treatment_letter': None,
        'treatment_number': None,
        'treatment_type': None,
        'ionization_mode': None,
        'column_type': None,
        'is_qc': False,
        'is_control': False
    }
    
    # Look for ionization mode
    if '_POS_' in base_name:
        sample_info['ionization_mode'] = 'positive'
    elif '_NEG_' in base_name:
        sample_info['ionization_mode'] = 'negative'
    
    # Look for column type
    if 'HILICZ' in base_name or 'HILIC' in base_name:
        sample_info['column_type'] = 'hilic'
    elif 'C18' in base_name:
        sample_info['column_type'] = 'c18'
    
    # Check for QC samples
    if 'QC' in base_name or '_Post_' in base_name:
        sample_info['is_qc'] = True
        return sample_info
    
    # Check for control samples
    if 'ExCtrl' in base_name:
        sample_info['is_control'] = True
        return sample_info
    
    import re
    
    # Extract plot ID from the root- pattern
    # Look for pattern like "root-F2-S1" or "root-I14-S1"
    root_match = re.search(r'root-([A-Z]\d+)-S\d+', base_name)
    if root_match:
        sample_info['plot_id'] = root_match.group(1)
    
    # Extract treatment information from patterns like "17_A-C-2_3"
    # This should map to biosample names like "G2_A_2_control"
    treatment_match = re.search(r'_(\d+)_([A-Z])-([A-Z])-(\d+)_(\d+)', base_name)
    if treatment_match:
        sample_num = treatment_match.group(1)      # 17
        treatment_letter = treatment_match.group(2)  # A or B
        treatment_code = treatment_match.group(3)    # C or D (control/drought)
        treatment_number = treatment_match.group(4)  # 0-7 
        replicate = treatment_match.group(5)         # 1-9
        
        sample_info['treatment_letter'] = treatment_letter
        sample_info['treatment_number'] = treatment_number
        
        # Map treatment codes to biosample naming convention
        if treatment_code == 'C':
            sample_info['treatment_type'] = 'control'
        elif treatment_code == 'D':
            sample_info['treatment_type'] = 'drought'
        else:
            sample_info['treatment_type'] = 'unknown'
    
    return sample_info


def validate_treatment_consistency(treatment_type, biosample_name):
    """
    Validate that the extracted treatment type is consistent with the biosample name.
    
    Prevents mapping control samples (C) to drought biosamples and vice versa.
    
    Args:
        treatment_type: Extracted treatment type ('control', 'drought', 'unknown')
        biosample_name: NMDC biosample name (e.g., 'G2_A_2_control')
        
    Returns:
        bool: True if treatment is consistent, False otherwise
    """
    if treatment_type == 'control' and 'drought' in biosample_name:
        return False
    elif treatment_type == 'drought' and 'control' in biosample_name:
        return False
    elif treatment_type == 'unknown':
        # For unknown treatments, don't make assumptions - allow mapping
        return True
    else:
        # Treatment type matches biosample name or is compatible
        return True


def match_to_biosamples(raw_files_info, biosample_df):
    """
    Match raw file information to NMDC biosamples for Singer study.
    
    Strategy: Extract plot ID from filename (e.g., F2, I14) and match to biosample names
    which follow pattern like F7_A_BS, G13_A_BS, etc.
    
    Args:
        raw_files_info: List of dictionaries with raw file information
        biosample_df: DataFrame with NMDC biosample attributes
        
    Returns:
        DataFrame with mapping between raw files and biosamples
    """
    print(f"🔍 Attempting to match {len(raw_files_info)} raw files to biosamples...")
    print(f"📊 Available biosample columns: {list(biosample_df.columns)}")
    
    # Show some example biosample data
    if 'name' in biosample_df.columns:
        print("📝 Example biosample names:")
        for i, name in enumerate(biosample_df['name'].head(5)):
            print(f"  {i+1}. {name}")
    
    # Create mapping list
    mappings = []
    
    for raw_info in raw_files_info:
        mapping = {
            'raw_file_name': Path(raw_info['raw_filename']).name,
            'raw_file_path': raw_info['raw_filename'],  # Keep full path for debugging
            'biosample_id': None,
            'biosample_name': None,
            'match_confidence': 'no_match',
            'match_method': 'no_match'
        }
        
        # Skip QC and control samples - they don't map to biosamples
        if raw_info.get('is_qc', False):
            mapping['match_confidence'] = 'qc_sample'
            mappings.append(mapping)
            continue
            
        if raw_info.get('is_control', False):
            mapping['match_confidence'] = 'control_sample'
            mappings.append(mapping)
            continue
        
        # Enhanced matching strategy using plot ID and treatment information
        plot_id = raw_info.get('plot_id')
        treatment_letter = raw_info.get('treatment_letter')
        treatment_number = raw_info.get('treatment_number')
        treatment_type = raw_info.get('treatment_type')
        
        if plot_id and 'name' in biosample_df.columns:
            # ONLY exact matches allowed - no partial matching to ensure perfect replicate alignment
            if treatment_letter and treatment_number and treatment_type:
                # Expected biosample name pattern: "G2_A_2_control"
                expected_name = f"{plot_id}_{treatment_letter}_{treatment_number}_{treatment_type}"
                exact_matches = biosample_df[biosample_df['name'] == expected_name]
                
                if len(exact_matches) == 1:
                    # Validate treatment consistency before accepting match
                    biosample_name = exact_matches.iloc[0]['name']
                    if validate_treatment_consistency(treatment_type, biosample_name):
                        mapping['biosample_id'] = exact_matches.iloc[0]['id']
                        mapping['biosample_name'] = biosample_name
                        mapping['match_confidence'] = 'high'
                        mapping['match_method'] = 'exact_treatment_match'
                    else:
                        # Treatment mismatch - don't map
                        mapping['match_confidence'] = 'treatment_mismatch'
                        mapping['match_method'] = 'treatment_validation_failed'
                elif len(exact_matches) > 1:
                    # Multiple exact matches - this shouldn't happen in NMDC but handle it
                    mapping['match_confidence'] = 'multiple_exact_matches'
                    mapping['match_method'] = 'exact_treatment_multiple_error'
                else:
                    # No exact match found - do not map at all
                    mapping['match_confidence'] = 'no_exact_match'
                    mapping['match_method'] = 'exact_match_required'
            else:
                # Missing treatment information - cannot do exact matching
                mapping['match_confidence'] = 'incomplete_treatment_info'
                mapping['match_method'] = 'missing_treatment_data'
        
        mappings.append(mapping)
    
    mapping_df = pd.DataFrame(mappings)
    
    # Report matching statistics
    match_stats = mapping_df['match_confidence'].value_counts()
    print("\\n📈 Matching Results:")
    for confidence, count in match_stats.items():
        print(f"  {confidence}: {count} files")
    
    # Show some example matches for validation
    high_confidence = mapping_df[mapping_df['match_confidence'] == 'high'].head(5)
    if len(high_confidence) > 0:
        print("\\n✅ Example high-confidence matches:")
        for _, row in high_confidence.iterrows():
            method = row.get('match_method', 'unknown')
            print(f"  📄 {row['raw_file_name'][:60]}...")
            print(f"  🧬 → {row['biosample_name']} ({row['biosample_id']}) [{method}]")
    
    # Show method distribution
    if 'match_method' in mapping_df.columns:
        method_stats = mapping_df['match_method'].value_counts()
        print("\\n📊 Match method distribution:")
        for method, count in method_stats.items():
            print(f"  {method}: {count} files")
    
    return mapping_df


def main():
    """Main function to map raw files to biosamples."""
    
    print("=== SINGER_11_46AJE659 - RAW FILE TO BIOSAMPLE MAPPING ===")
    
    # Initialize study manager
    config_path = Path.cwd() / "studies" / "singer_11_46aje659" / "singer_config.json"
    if not config_path.exists():
        print(f"❌ Config file not found: {config_path}")
        print("Please run this script from the data_processing root directory")
        return 1
    
    study = NMDCWorkflowManager(str(config_path))
    
    # Step 1: Check if biosample attributes are available
    print("\\n1. Checking biosample attributes...")
    biosample_csv = study.study_path / "metadata" / "biosample_attributes.csv"
    if not biosample_csv.exists():
        print("❌ Biosample attributes not found. Run get_biosample_attributes() first.")
        return 1
    
    try:
        biosample_df = pd.read_csv(biosample_csv)
        print(f"✅ Loaded {len(biosample_df)} biosamples")
    except Exception as e:
        print(f"❌ Error loading biosample attributes: {e}")
        return 1
    
    # Step 2: Get list of downloaded raw data files
    print("\\n2. Loading downloaded files list...")
    downloaded_files_csv = study.study_path / "metadata" / "downloaded_files.csv"
    if not downloaded_files_csv.exists():
        print(f"❌ Downloaded files list not found: {downloaded_files_csv}")
        print("Run the download_from_massive() method first to generate this file.")
        return 1
    
    try:
        downloaded_df = pd.read_csv(downloaded_files_csv)
        raw_files = [Path(row['file_path']) for _, row in downloaded_df.iterrows() 
                    if Path(row['file_path']).exists()]
        print(f"✅ Found {len(raw_files)} downloaded raw data files")
    except Exception as e:
        print(f"❌ Error loading downloaded files list: {e}")
        return 1
    
    if not raw_files:
        print("❌ No valid raw data files found in downloaded files list")
        return 1
    
    # Step 3: Extract sample information from filenames
    print("\\n3. Extracting sample information from filenames...")
    raw_files_info = []
    for raw_file in raw_files:
        file_info = extract_sample_info_from_filename(str(raw_file))
        raw_files_info.append(file_info)
    
    # Show some examples
    print("📝 Example parsed filenames:")
    for i, info in enumerate(raw_files_info[:3]):
        print(f"  {i+1}. {Path(info['raw_filename']).name}")
        print(f"     Plot ID: {info.get('plot_id', 'Not found')}")
        print(f"     Treatment: {info.get('treatment_letter', 'N/A')}-{info.get('treatment_number', 'N/A')} ({info.get('treatment_type', 'N/A')})")
        print(f"     Ion mode: {info.get('ionization_mode', 'Not found')}")
        print(f"     Column: {info.get('column_type', 'Not found')}")
    
    # Step 4: Match raw files to biosamples
    print("\\n4. Matching raw files to NMDC biosamples...")
    mapping_df = match_to_biosamples(raw_files_info, biosample_df)
    
    # Step 5: Sort by confidence and save mapping file
    print("\\n5. Saving mapping file...")
    
    # Define confidence order (high to low)
    confidence_order = ['high', 'medium', 'low', 'multiple_matches', 'no_match']
    mapping_df['confidence_rank'] = mapping_df['match_confidence'].map(
        {conf: i for i, conf in enumerate(confidence_order)}
    )
    mapping_df = mapping_df.sort_values('confidence_rank').drop('confidence_rank', axis=1)
    
    # Save mapping file with correct study name
    mapping_file = study.study_path / "singer_11_46aje659_raw_file_biosample_mapping.csv"
    mapping_df.to_csv(mapping_file, index=False)
    print(f"💾 Saved mapping to: {mapping_file}")
    
    # Only create filtered file with perfect matches for WDL processing
    mapped_only = mapping_df[mapping_df['match_confidence'] == 'high'].copy()
    if len(mapped_only) > 0:
        mapped_files_csv = study.study_path / "metadata" / "mapped_raw_files.csv"
        mapped_only.to_csv(mapped_files_csv, index=False)
        print(f"💾 Saved mapped files only to: {mapped_files_csv}")
        print("   (Only perfect matches - this file will be used by generate_wdl_jsons())")
    else:
        print("⚠️  No perfect matches found - no mapped_raw_files.csv created")
    
    # Step 6: Report results and next steps
    print("\\n=== RESULTS AND NEXT STEPS ===")
    
    total_files = len(mapping_df)
    total_biosamples = len(biosample_df)
    matched_files = len(mapping_df[mapping_df['match_confidence'] == 'high'])  # Only perfect matches
    unmatched_files = len(mapping_df[mapping_df['match_confidence'].isin(['no_match', 'no_exact_match', 'incomplete_treatment_info'])])
    multiple_matches = len(mapping_df[mapping_df['match_confidence'] == 'multiple_exact_matches'])
    treatment_mismatches = len(mapping_df[mapping_df['match_confidence'] == 'treatment_mismatch'])
    
    # Calculate biosample coverage
    mapped_biosamples = mapping_df[mapping_df['biosample_id'].notna()]['biosample_id'].nunique()
    biosample_coverage_pct = (mapped_biosamples / total_biosamples) * 100 if total_biosamples > 0 else 0
    
    print(f"📊 Total files processed: {total_files}")
    print(f"✅ Successfully matched: {matched_files}")
    print(f"⚠️  Multiple matches: {multiple_matches}")
    print(f"❌ Unmatched files: {unmatched_files}")
    print(f"🚫 Treatment mismatches: {treatment_mismatches}")
    print(f"\\n🧬 Biosample Coverage:")
    print(f"   Total biosamples available: {total_biosamples}")
    print(f"   Biosamples with raw data: {mapped_biosamples}")
    print(f"   Coverage: {biosample_coverage_pct:.1f}%")
    
    if unmatched_files > 0 or multiple_matches > 0 or treatment_mismatches > 0:
        print("\\n⚠️  MANUAL REVIEW NEEDED:")
        print("1. Review the mapping file for unmatched files")
        if treatment_mismatches > 0:
            print(f"2. Check {treatment_mismatches} files with treatment mismatches (control→drought conflicts)")
        print("3. Customize the matching logic in this script")
        print("4. Check biosample attributes for additional matching fields")
        print("5. Verify raw file naming conventions")
        print("6. Re-run this script after making changes")
    
    if matched_files > 0:
        print("\\n✅ READY FOR NEXT STEPS:")
        print("1. Review and validate the perfect matches")
        print(f"2. Process {matched_files} perfectly matched files with WDL JSON generation")
        print("3. Consider whether to investigate unmatched files or accept current matches")
    else:
        print("\\n❌ NO PERFECT MATCHES FOUND:")
        print("1. Review biosample naming conventions")
        print("2. Check raw file naming patterns")
        print("3. Consider relaxing matching requirements if needed")
    
    print(f"\\nMapping file: {mapping_file}")
    print(f"Biosample attributes: {biosample_csv}")
    print(f"\\n📊 SUMMARY: {matched_files} perfect matches out of {total_files} files ({matched_files/total_files*100:.1f}%)")
    
    return 0 if (matched_files > 0 and multiple_matches == 0 and treatment_mismatches == 0) else 1


if __name__ == "__main__":
    sys.exit(main())