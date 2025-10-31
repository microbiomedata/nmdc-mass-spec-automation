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

from study_manager import NMDCStudyManager


def extract_sample_info_from_filename(filename):
    """
    Extract sample information from raw data filename.
    
    CUSTOMIZE THIS FUNCTION based on your study's file naming convention.
    
    Args:
        filename: Raw data filename (e.g., "sample_123_pos.raw")
        
    Returns:
        Dictionary with extracted sample information
    """
    # Remove file extension
    base_name = Path(filename).stem
    
    # Initialize sample info dictionary
    sample_info = {
        'raw_filename': filename,
        'sample_id': None,
        'treatment': None,
        'replicate': None,
        'ionization_mode': None,
        'column_type': None,
        'time_point': None
    }
    
    # CUSTOMIZE: Parse filename based on your naming convention
    # Example patterns to look for in singer_11_46aje659:
    parts = base_name.split('_')
    
    # Look for ionization mode
    for part in parts:
        if 'pos' in part.lower():
            sample_info['ionization_mode'] = 'positive'
        elif 'neg' in part.lower():
            sample_info['ionization_mode'] = 'negative'
    
    # Look for column type
    for part in parts:
        if 'hilic' in part.lower():
            sample_info['column_type'] = 'hilic'
        elif 'c18' in part.lower() or 'rp' in part.lower():
            sample_info['column_type'] = 'rp'
    
    # Extract sample ID (customize this logic)
    # Look for numeric parts that might be sample IDs
    for part in parts:
        if part.isdigit():
            sample_info['sample_id'] = part
            break
    
    # Add more parsing logic here based on your file naming patterns
    
    return sample_info


def match_to_biosamples(raw_files_info, biosample_df):
    """
    Match raw file information to NMDC biosamples.
    
    CUSTOMIZE THIS FUNCTION to implement study-specific matching logic.
    
    Args:
        raw_files_info: List of dictionaries with raw file information
        biosample_df: DataFrame with NMDC biosample attributes
        
    Returns:
        DataFrame with mapping between raw files and biosamples
    """
    print(f"🔍 Attempting to match {len(raw_files_info)} raw files to biosamples...")
    print(f"📊 Available biosample columns: {list(biosample_df.columns)}")
    
    # Show some example biosample data to help with matching
    if 'name' in biosample_df.columns:
        print("📝 Example biosample names:")
        for i, name in enumerate(biosample_df['name'].head(5)):
            print(f"  {i+1}. {name}")
    
    if 'id' in biosample_df.columns:
        print("🆔 Example biosample IDs:")
        for i, biosample_id in enumerate(biosample_df['id'].head(5)):
            print(f"  {i+1}. {biosample_id}")
    
    # Create mapping list
    mappings = []
    
    for raw_info in raw_files_info:
        mapping = {
            'raw_file_name': Path(raw_info['raw_filename']).name,
            'biosample_id': None,
            'biosample_name': None,
            'match_confidence': 'no_match'
        }
        
        # CUSTOMIZE: Implement your matching logic here
        
        # Strategy 1: Match by sample ID in biosample name
        if raw_info['sample_id'] and 'name' in biosample_df.columns:
            matches = biosample_df[biosample_df['name'].str.contains(
                raw_info['sample_id'], case=False, na=False)]
            if len(matches) == 1:
                mapping['biosample_id'] = matches.iloc[0]['id']
                mapping['biosample_name'] = matches.iloc[0]['name']
                mapping['match_confidence'] = 'high'
            elif len(matches) > 1:
                mapping['match_confidence'] = 'multiple_matches'
        
        # Strategy 2: Add more matching strategies here
        # - Match by treatment codes
        # - Match by plot numbers
        # - Match by other metadata fields
        
        mappings.append(mapping)
    
    mapping_df = pd.DataFrame(mappings)
    
    # Report matching statistics
    match_stats = mapping_df['match_confidence'].value_counts()
    print("\\n📈 Matching Results:")
    for confidence, count in match_stats.items():
        print(f"  {confidence}: {count} files")
    
    return mapping_df


def main():
    """Main function to map raw files to biosamples."""
    
    print("=== {study_name.upper()} - RAW FILE TO BIOSAMPLE MAPPING ===")
    
    # Initialize study manager
    config_path = Path.cwd() / "singer_11_46aje659" / "config.json"
    if not config_path.exists():
        print(f"❌ Config file not found: {config_path}")
        print("Please run this script from the data_processing root directory")
        return 1
    
    study = NMDCStudyManager(str(config_path))
    
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
        print(f"     Sample ID: {info.get('sample_id', 'Not found')}")
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
    
    # Save simplified mapping file
    mapping_file = study.study_path / "{study_name}_raw_file_biosample_mapping.csv"
    mapping_df.to_csv(mapping_file, index=False)
    print(f"💾 Saved mapping to: {mapping_file}")
    
    # Step 6: Report results and next steps
    print("\\n=== RESULTS AND NEXT STEPS ===")
    
    total_files = len(mapping_df)
    total_biosamples = len(biosample_df)
    matched_files = len(mapping_df[mapping_df['match_confidence'].isin(['high', 'medium', 'low'])])
    unmatched_files = len(mapping_df[mapping_df['match_confidence'] == 'no_match'])
    multiple_matches = len(mapping_df[mapping_df['match_confidence'] == 'multiple_matches'])
    
    # Calculate biosample coverage
    mapped_biosamples = mapping_df[mapping_df['biosample_id'].notna()]['biosample_id'].nunique()
    biosample_coverage_pct = (mapped_biosamples / total_biosamples) * 100 if total_biosamples > 0 else 0
    
    print(f"📊 Total files processed: {total_files}")
    print(f"✅ Successfully matched: {matched_files}")
    print(f"⚠️  Multiple matches: {multiple_matches}")
    print(f"❌ Unmatched files: {unmatched_files}")
    print(f"\\n🧬 Biosample Coverage:")
    print(f"   Total biosamples available: {total_biosamples}")
    print(f"   Biosamples with raw data: {mapped_biosamples}")
    print(f"   Coverage: {biosample_coverage_pct:.1f}%")
    
    if unmatched_files > 0 or multiple_matches > 0:
        print("\\n⚠️  MANUAL REVIEW NEEDED:")
        print("1. Review the mapping file for unmatched files")
        print("2. Customize the matching logic in this script")
        print("3. Check biosample attributes for additional matching fields")
        print("4. Verify raw file naming conventions")
        print("5. Re-run this script after making changes")
    
    if matched_files > 0:
        print("\\n✅ READY FOR NEXT STEPS:")
        print("1. Review and validate the mapping file")
        print("2. Filter raw files to only include matched samples")
        print("3. Proceed with WDL JSON generation")
    
    print(f"\\nMapping file: {mapping_file}")
    print(f"Biosample attributes: {biosample_csv}")
    
    return 0 if (unmatched_files == 0 and multiple_matches == 0) else 1


if __name__ == "__main__":
    sys.exit(main())