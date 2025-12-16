#!/usr/bin/env python3
"""
blanchard_11_8ws97026 - Map raw data files to NMDC biosamples

This script maps raw LC-MS data files to NMDC biosamples for the study:
"Molecular mechanisms underlying changes in the temperature sensitive respiration response of forest soils to long-term experimental warming"

The mapping strategy is study-specific and may need to be customized based on:
- File naming conventions
- Sample metadata available
- Biosample attributes from NMDC

Run from the root data_processing directory:
python blanchard_11_8ws97026/scripts/map_raw_files_to_biosamples_TEMPLATE.py
"""

import sys
import pandas as pd
from pathlib import Path

# Add the utils directory to path
sys.path.append(str(Path.cwd()))

from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager


def extract_sample_info_from_filename(filename):
    """
    Extract sample information from raw data filename.
    
    For Blanchard study, filenames follow pattern:
    Blanch_Nat_Met_{treatment}_{plot}_{extra}_{layer}_{num}.cdf
    
    Example: Blanch_Nat_Met_C_12_AB_M_17.cdf
    - Treatment: C (Control) or H (Heated)
    - Plot: 12
    - Layer: M (Mineral) or O (Organic)
    
    Maps to biosample names like: BW-C-12-M
    
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
        'sample_id': None,
        'treatment': None,
        'replicate': None,
        'ionization_mode': None,
        'column_type': None,
        'time_point': None
    }
    
    # Parse Blanchard filename: Blanch_Nat_Met_C_12_AB_M_17
    parts = base_name.split('_')
    
    if len(parts) >= 8 and parts[0] == 'Blanch' and parts[1] == 'Nat' and parts[2] == 'Met':
        # Extract treatment (C or H)
        treatment = parts[3]
        sample_info['treatment'] = treatment
        
        # Extract plot number - handle special case like "2B" -> "2"
        plot = parts[4]
        # Remove any letter suffix (e.g., 2B -> 2)
        plot_num = ''.join(c for c in plot if c.isdigit())
        sample_info['sample_id'] = plot_num
        
        # Extract layer (M or O) - it's at position 6
        layer = parts[6]
        sample_info['replicate'] = layer
        
        # Construct the biosample name pattern: BW-{treatment}-{plot}-{layer}
        # This will be used to match against biosample names
        sample_info['biosample_pattern'] = f"BW-{treatment}-{plot_num}-{layer}"
    
    return sample_info


def match_to_biosamples(raw_files_info, biosample_df):
    """
    Match raw file information to NMDC biosamples.
    
    For Blanchard study, matches files to biosamples using the pattern:
    Filename: Blanch_Nat_Met_C_12_AB_M_17.cdf -> Pattern: BW-C-12-M
    Biosample name: BW-C-12-M -> Exact match
    
    Args:
        raw_files_info: List of dictionaries with raw file information
        biosample_df: DataFrame with NMDC biosample attributes
        
    Returns:
        DataFrame with mapping between raw files and biosamples
    """
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
        filename = Path(raw_info['raw_filename']).name
        
        # Determine file type - check if it's a calibration/standard file
        if 'FAMEs' in filename or 'GCMS01' in filename:
            raw_file_type = 'calibration'
        else:
            raw_file_type = 'sample'
        
        mapping = {
            'raw_file_name': filename,
            'raw_file_type': raw_file_type,
            'biosample_id': None,
            'biosample_name': None,
            'match_confidence': 'no_match'
        }
        
        # Only try to match sample files (not calibration files)
        if raw_file_type == 'sample' and 'biosample_pattern' in raw_info and raw_info['biosample_pattern']:
            pattern = raw_info['biosample_pattern']
            
            # Look for exact match on biosample name
            if 'name' in biosample_df.columns:
                matches = biosample_df[biosample_df['name'] == pattern]
                
                if len(matches) == 1:
                    mapping['biosample_id'] = matches.iloc[0]['id']
                    mapping['biosample_name'] = matches.iloc[0]['name']
                    mapping['match_confidence'] = 'high'
                elif len(matches) > 1:
                    mapping['match_confidence'] = 'multiple_matches'
                    print(f"⚠️  Multiple matches for pattern: {pattern}")
                else:
                    # Try without the BW prefix - maybe it's just C-12-M
                    pattern_parts = pattern.split('-')[1:]  # Skip BW
                    simple_pattern = '-'.join(pattern_parts)
                    matches = biosample_df[biosample_df['name'].str.contains(
                        simple_pattern, case=False, na=False, regex=False)]
                    
                    if len(matches) == 1:
                        mapping['biosample_id'] = matches.iloc[0]['id']
                        mapping['biosample_name'] = matches.iloc[0]['name']
                        mapping['match_confidence'] = 'medium'
                    elif len(matches) > 1:
                        mapping['match_confidence'] = 'multiple_matches'
        elif raw_file_type == 'calibration':
            # Calibration files don't need biosample mapping but should be included
            mapping['match_confidence'] = 'high'  # Calibration files are always valid
        
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
    config_path = Path("studies/example_gcms_metab/example_gcms_metab_config.json")
    if not config_path.exists():
        print(f"❌ Config file not found: {config_path}")
        print("Please run this script from the data_processing root directory")
        return 1
    
    study = NMDCWorkflowManager(str(config_path))
    
    # Step 1: Check if biosample attributes are available
    print("\\n1. Checking biosample attributes...")
    biosample_csv = study.workflow_path / "metadata" / "biosample_attributes.csv"
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
    downloaded_files_csv = study.workflow_path / "metadata" / "downloaded_files.csv"
    if not downloaded_files_csv.exists():
        print(f"❌ Downloaded files list not found: {downloaded_files_csv}")
        print("Run the download_from_massive() method first to generate this file.")
        return 1
    
    try:
        downloaded_df = pd.read_csv(downloaded_files_csv)
        # Check which column name is present
        if 'file_path' in downloaded_df.columns:
            raw_files = [Path(row['file_path']) for _, row in downloaded_df.iterrows() 
                        if Path(row['file_path']).exists()]
        elif 'raw_data_file_short' in downloaded_df.columns:
            # Just filenames, need to construct full paths using manager's raw_data_directory
            raw_data_dir = Path(study.raw_data_directory)
            raw_files = [raw_data_dir / row['raw_data_file_short'] for _, row in downloaded_df.iterrows()]
            # Filter to only existing files
            raw_files = [f for f in raw_files if f.exists()]
        else:
            print(f"❌ Unknown column format in downloaded files CSV")
            return 1
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
    mapping_file = study.workflow_path / "metadata" / "mapped_raw_file_biosample_mapping.csv"
    mapping_df.to_csv(mapping_file, index=False)
    print(f"💾 Saved mapping to: {mapping_file}")
    
    # Step 6: Report results and next steps
    print("\\n=== RESULTS AND NEXT STEPS ===")
    
    total_files = len(mapping_df)
    total_biosamples = len(biosample_df)
    matched_files = len(mapping_df[mapping_df['match_confidence'].isin(['high', 'medium', 'low'])])
    unmatched_files = len(mapping_df[mapping_df['match_confidence'] == 'no_match'])
    multiple_matches = len(mapping_df[mapping_df['match_confidence'] == 'multiple_matches'])
    
    # Calculate sample-type statistics
    sample_files = mapping_df[mapping_df['raw_file_type'] == 'sample']
    total_sample_files = len(sample_files)
    sample_file_pct = (total_sample_files / total_files) * 100 if total_files > 0 else 0
    
    # Of the sample files, how many mapped to biosamples?
    sample_matched = len(sample_files[sample_files['match_confidence'].isin(['high', 'medium', 'low'])])
    sample_match_pct = (sample_matched / total_sample_files) * 100 if total_sample_files > 0 else 0
    
    # Calculate biosample coverage
    mapped_biosamples = mapping_df[mapping_df['biosample_id'].notna()]['biosample_id'].nunique()
    biosample_coverage_pct = (mapped_biosamples / total_biosamples) * 100 if total_biosamples > 0 else 0
    
    print(f"📊 Total files processed: {total_files}")
    print(f"✅ Successfully matched: {matched_files}")
    print(f"⚠️  Multiple matches: {multiple_matches}")
    print(f"❌ Unmatched files: {unmatched_files}")
    print(f"\\n🧪 Sample-type Analysis:")
    print(f"   Sample files: {total_sample_files} ({sample_file_pct:.1f}% of all files)")
    print(f"   Samples mapped to biosamples: {sample_matched}/{total_sample_files} ({sample_match_pct:.1f}%)")
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
    main()