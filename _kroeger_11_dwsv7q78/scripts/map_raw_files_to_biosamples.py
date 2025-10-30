#!/usr/bin/env python3
"""
kroeger_11_dwsv7q78 - Map raw data files to NMDC biosamples

This script maps raw LC-MS data files to NMDC biosamples for the study:
"Microbial regulation of soil water repellency to control soil degradation"

The mapping strategy is study-specific and may need to be customized based on:
- File naming conventions
- Sample metadata available
- Biosample attributes from NMDC

Run from the root data_processing directory:
python kroeger_11_dwsv7q78/scripts/map_raw_files_to_biosamples.py
"""

import sys
import pandas as pd
import re
from pathlib import Path

# Add the utils directory to path
sys.path.append(str(Path.cwd() / "nmdc_dp_utils"))

from study_manager import NMDCStudyManager


def extract_sample_info_from_filename(filename):
    """
    Extract sample information from Kroeger study filename.
    
    Args:
        filename: Raw data filename
        
    Returns:
        Dictionary with extracted sample information
    """
    import re
    
    # Remove file extension and get base name
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
    
    # Look for ionization mode
    if 'POS' in base_name:
        sample_info['ionization_mode'] = 'positive'
    elif 'NEG' in base_name:
        sample_info['ionization_mode'] = 'negative'
    
    # Look for column type
    if 'HILICZ' in base_name:
        sample_info['column_type'] = 'hilic'
    elif 'C18' in base_name:
        sample_info['column_type'] = 'rp'
    
    # Extract sample information using regex patterns
    # Pattern 1: S##-D##_[A-C] (e.g., S32-D30_A, S40-D89_B)
    complex_pattern = r'(\w+)-D(\d+)_([ABC])'
    complex_match = re.search(complex_pattern, base_name)
    
    if complex_match:
        sample_info['sample_id'] = complex_match.group(1)  # e.g., 'S32'
        sample_info['treatment'] = f"D{complex_match.group(2)}"  # e.g., 'D30'
        sample_info['replicate'] = complex_match.group(3)  # e.g., 'A'
        return sample_info
    
    # Pattern 2: Control samples (ExCtrl, Neg, Sterile-*, QC)
    control_patterns = [
        r'ExCtrl',
        r'Neg-D\d+',
        r'Sterile-\w+',
        r'QC'
    ]
    
    for pattern in control_patterns:
        if re.search(pattern, base_name):
            match = re.search(pattern, base_name)
            sample_info['sample_id'] = match.group(0)
            sample_info['treatment'] = 'control'
            return sample_info
    
    # Pattern 3: Simple sample ID (S##)
    simple_pattern = r'(S\d+)(?!-)'
    simple_match = re.search(simple_pattern, base_name)
    
    if simple_match:
        sample_info['sample_id'] = simple_match.group(1)
        return sample_info
    
    # Pattern 4: Pilot study samples
    pilot_pattern = r'S\d+-D\d+-\w+'
    if re.search(pilot_pattern, base_name):
        pilot_match = re.search(r'(S\d+)', base_name)
        if pilot_match:
            sample_info['sample_id'] = pilot_match.group(1)
            sample_info['treatment'] = 'pilot'
    
    return sample_info


def match_to_biosamples(raw_files_info, biosample_df):
    """
    Match raw file information to NMDC biosamples for Kroeger study.
    Based on the working mapping logic from the previous detailed analysis.
    """
    print(f"🔍 Attempting to match {len(raw_files_info)} raw files to biosamples...")
    print(f"📊 Available biosample columns: {list(biosample_df.columns)}")
    
    mappings = []
    
    for raw_info in raw_files_info:
        mapping = {
            'raw_file_name': Path(raw_info['raw_filename']).name,
            'biosample_id': None,
            'biosample_name': None,
            'match_confidence': 'no_match'
        }
        
        filename = Path(raw_info['raw_filename']).name
        sample_id = raw_info.get('sample_id')
        treatment = raw_info.get('treatment')
        replicate = raw_info.get('replicate')
        ionization_mode = raw_info.get('ionization_mode')
        column_type = raw_info.get('column_type')
        
        # Strategy 1: Control identification (highest priority)
        control_patterns = ['ExCtrl', 'Neg-', 'Sterile-', 'QC']
        if any(pattern in filename for pattern in control_patterns):
            mapping['match_confidence'] = 'control_sample'  
            mappings.append(mapping)
            continue
        
        # Strategy 2: Pilot study identification
        if 'pilot' in filename:
            mapping['match_confidence'] = 'control_sample'
            mappings.append(mapping)
            continue
        
        # Strategy 3: Extract sample information using complex regex patterns
        # Pattern for S##-D##_[A-C] (e.g., S32-D30_A, S40-D89_B)
        complex_pattern = r'(\w\d+)-D(\d+)_([ABC])'
        complex_match = re.search(complex_pattern, filename)
        
        if complex_match:
            extracted_sample = complex_match.group(1)  # e.g., 'S32'
            day = complex_match.group(2)  # e.g., '30'
            rep = complex_match.group(3)  # e.g., 'A'
            
            # Build the expected biosample name pattern (ignoring analytical method)
            # The hydrophobic/hydrophilic refers to soil properties, not analytical column
            base_pattern = f"{extracted_sample}_{rep}_D{day}"
            
            # Look for any biosample name that starts with this pattern
            pattern_matches = biosample_df[biosample_df['name'].str.contains(
                f"^{re.escape(base_pattern)}", case=False, na=False)]
            
            if len(pattern_matches) == 1:
                mapping['biosample_id'] = pattern_matches.iloc[0]['id']
                mapping['biosample_name'] = pattern_matches.iloc[0]['name']
                mapping['match_confidence'] = 'high'
                mappings.append(mapping)
                continue
            elif len(pattern_matches) > 1:
                # Multiple matches - this shouldn't happen with proper biosample naming
                mapping['match_confidence'] = 'multiple_matches'
                mappings.append(mapping)
                continue
        
        # Strategy 4: Simple sample ID matching (S##)
        simple_pattern = r'(S\d+)(?=[-_\s]|$)'
        simple_match = re.search(simple_pattern, filename)
        
        if simple_match:
            extracted_sample = simple_match.group(1)
            
            # Try exact name match first
            exact_matches = biosample_df[biosample_df['name'] == extracted_sample]
            if len(exact_matches) == 1:
                mapping['biosample_id'] = exact_matches.iloc[0]['id']
                mapping['biosample_name'] = exact_matches.iloc[0]['name']
                mapping['match_confidence'] = 'medium'
                mappings.append(mapping)
                continue
            
            # Try contains match
            contains_matches = biosample_df[biosample_df['name'].str.contains(
                extracted_sample, case=False, na=False)]
            if len(contains_matches) == 1:
                mapping['biosample_id'] = contains_matches.iloc[0]['id']
                mapping['biosample_name'] = contains_matches.iloc[0]['name']
                mapping['match_confidence'] = 'medium'
                mappings.append(mapping)
                continue
        
        # If no match found
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
    
    print("=== KROEGER_11_DWSV7Q78 - RAW FILE TO BIOSAMPLE MAPPING ===")
    
    # Initialize study manager
    config_path = Path.cwd() / "_kroeger_11_dwsv7q78" / "config.json"
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
    mapping_file = study.study_path / "kroeger_11_dwsv7q78_raw_file_biosample_mapping.csv"
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