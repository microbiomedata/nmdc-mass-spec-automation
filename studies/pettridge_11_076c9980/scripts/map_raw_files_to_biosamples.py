#!/usr/bin/env python3
"""
pettridge_11_076c9980 - Map raw data files to NMDC biosamples

This script maps raw LC-MS data files to NMDC biosamples for the study:
"Microbial Carbon Transformations in Wet Tropical Soils: Effects of Redox Fluctuation"

The mapping strategy is study-specific and may need to be customized based on:
- File naming conventions
- Sample metadata available
- Biosample attributes from NMDC

Run from the root data_processing directory:
python pettridge_11_076c9980/scripts/map_raw_files_to_biosamples_TEMPLATE.py
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
    
    Pettridge study filenames follow this pattern:
    20190103_KBL_JP_502924_Soil_13CFlux_QE-139_HILIC_USHGX01163_POS_MSMS_GRE-MB-103_statanox-12C-anox-23-000_2_Rg70to1050-CE102040-76-S1_Run203.mzML
    
    Key components:
    - GRE-MB-XXX: sample identifier
    - Treatment code (e.g., statanox-12C-anox-23-000, flux8day-12C-oxic-44-000)
    - POS/NEG: ionization mode
    - HILIC: column type
    - Replicate number (underscore before Rg)
    
    Args:
        filename: Raw data filename
        
    Returns:
        Dictionary with extracted sample information
    """
    base_name = Path(filename).stem
    
    sample_info = {
        'raw_filename': filename,
        'sample_id': None,
        'treatment_code': None,
        'replicate': None,
        'ionization_mode': None,
        'column_type': None,
    }
    
    parts = base_name.split('_')
    
    # Extract ionization mode
    if 'POS' in parts:
        sample_info['ionization_mode'] = 'POS'
    elif 'NEG' in parts:
        sample_info['ionization_mode'] = 'NEG'
    
    # Extract column type
    if 'HILIC' in parts:
        sample_info['column_type'] = 'HILIC'
    
    # Extract treatment code - look for patterns like "statanox-12C-anox-23-000", "flux8day-12C-oxic-44-000", etc.
    # These typically come after the GRE-MB-XXX identifier
    for i, part in enumerate(parts):
        if part.startswith('GRE-MB-'):
            sample_info['sample_id'] = part
            # Treatment code is the next part
            if i + 1 < len(parts):
                sample_info['treatment_code'] = parts[i + 1]
            # Replicate is the part after treatment code
            if i + 2 < len(parts) and parts[i + 2].isdigit():
                sample_info['replicate'] = parts[i + 2]
            break
        # Handle special controls like ExCtrl, plant samples
        elif part.startswith('ExCtrl-') or part.endswith('-plant'):
            sample_info['sample_id'] = part
            sample_info['treatment_code'] = 'control'
            if i + 1 < len(parts):
                # Next part might be additional descriptor or replicate
                if parts[i + 1].isdigit():
                    sample_info['replicate'] = parts[i + 1]
                elif not parts[i + 1].startswith('Rg'):
                    sample_info['treatment_code'] = part + '_' + parts[i + 1]
            break
    
    return sample_info


def match_to_biosamples(raw_files_info, biosample_df):
    """
    Match raw file information to NMDC biosamples.
    
    For Pettridge study, biosamples have a 'samp_name' field that contains treatment codes
    matching the treatment codes in the raw filenames.
    
    Matching strategy:
    1. Extract treatment code from filename (e.g., "statanox-12C-anox-23-000" becomes "statanox.12C.anox.23.000")
    2. Match against biosample 'samp_name' field (uses dots instead of dashes)
    3. Handle special cases (pretreat, timezero, controls)
    
    Args:
        raw_files_info: List of dictionaries with raw file information
        biosample_df: DataFrame with NMDC biosample attributes
        
    Returns:
        DataFrame with mapping between raw files and biosamples
    """
    print(f"🔍 Attempting to match {len(raw_files_info)} raw files to biosamples...")
    print(f"📊 Available biosample columns: {list(biosample_df.columns)}")
    
    # Show example biosample data
    if 'samp_name' in biosample_df.columns:
        print("📝 Example biosample samp_names:")
        for i, name in enumerate(biosample_df['samp_name'].head(10)):
            print(f"  {i+1}. {name}")
    
    # Create mapping list
    mappings = []
    unmatched_treatments = set()
    
    for raw_info in raw_files_info:
        mapping = {
            'raw_file_name': Path(raw_info['raw_filename']).name,
            'biosample_id': None,
            'biosample_name': None,
            'match_confidence': 'no_match',
            'treatment_code': raw_info.get('treatment_code', ''),
            'ionization_mode': raw_info.get('ionization_mode', '')
        }
        
        # Convert treatment code from filename format (with dashes) to biosample format (with dots)
        # Example: "statanox-12C-anox-23-000" -> "statanox.12C.anox.23.000"
        treatment = raw_info.get('treatment_code', '')
        if treatment and treatment != 'control':
            # Replace dashes with dots for matching
            treatment_pattern = treatment.replace('-', '.')
            
            # Find matching biosamples
            # Biosample samp_name patterns: "treatment.12C.condition.days.hours.replicate"
            # Filename treatment patterns: "treatment-12C-condition-days-hours"
            if 'samp_name' in biosample_df.columns:
                # First try exact prefix match (treatment code matches start of samp_name)
                matches = biosample_df[biosample_df['samp_name'].str.startswith(
                    treatment_pattern, na=False)]
                
                if len(matches) > 0:
                    # Found matches by prefix - all are likely valid, pick first
                    mapping['biosample_id'] = matches.iloc[0]['id']
                    mapping['biosample_name'] = matches.iloc[0]['name']
                    mapping['match_confidence'] = 'high'
                else:
                    # Try substring match as fallback
                    matches = biosample_df[biosample_df['samp_name'].str.contains(
                        treatment_pattern, case=False, na=False, regex=False)]
                    
                    if len(matches) == 1:
                        mapping['biosample_id'] = matches.iloc[0]['id']
                        mapping['biosample_name'] = matches.iloc[0]['name']
                        mapping['match_confidence'] = 'high'
                    elif len(matches) > 1:
                        # Multiple matches - use first as medium confidence
                        mapping['biosample_id'] = matches.iloc[0]['id']
                        mapping['biosample_name'] = matches.iloc[0]['name']
                        mapping['match_confidence'] = 'medium'
                    else:
                        unmatched_treatments.add(treatment)
        
        # Handle control samples
        elif treatment == 'control' or not treatment:
            sample_id = raw_info.get('sample_id', '') or ''
            if 'ExCtrl' in sample_id:
                # External control - may not have biosample
                mapping['match_confidence'] = 'no_match'
            elif 'plant' in sample_id:
                # Plant control - may not have biosample
                mapping['match_confidence'] = 'no_match'
        
        mappings.append(mapping)
    
    mapping_df = pd.DataFrame(mappings)
    
    # Report matching statistics
    match_stats = mapping_df['match_confidence'].value_counts()
    print("\n📈 Matching Results:")
    for confidence, count in match_stats.items():
        print(f"  {confidence}: {count} files")
    
    if unmatched_treatments:
        print("\n⚠️  Unmatched treatment codes:")
        for treatment in sorted(unmatched_treatments):
            print(f"  - {treatment}")
    
    return mapping_df


def main():
    """Main function to map raw files to biosamples."""
    
    study_name = "pettridge_11_076c9980"
    print(f"=== {study_name.upper()} - RAW FILE TO BIOSAMPLE MAPPING ===")
    
    # Initialize study manager  
    config_path = Path.cwd() / "studies" / study_name / "pettridge_config.json"
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
    print("\n5. Saving mapping file...")
    
    # Define confidence order (high to low)
    confidence_order = ['high', 'medium', 'low', 'multiple_matches', 'no_match']
    mapping_df['confidence_rank'] = mapping_df['match_confidence'].map(
        {conf: i for i, conf in enumerate(confidence_order)}
    )
    mapping_df = mapping_df.sort_values('confidence_rank').drop('confidence_rank', axis=1)
    
    # Save simplified mapping file (same naming pattern as kroeger script)
    mapping_file = study.study_path / "pettridge_11_076c9980_raw_file_biosample_mapping.csv"
    mapping_df.to_csv(mapping_file, index=False)
    print(f"💾 Saved mapping to: {mapping_file}")
    
    # Export unmapped files that aren't controls/QC/standards
    print("\n6. Exporting unmapped non-control files...")
    
    # Identify control/QC/standard patterns
    control_patterns = ['ExCtrl', 'QC', 'Blank', 'Standard', 'Std', 'plant', 'unk']
    
    # Filter for unmapped files only
    unmapped_df = mapping_df[mapping_df['match_confidence'] == 'no_match'].copy()
    
    # Identify potential sample files (exclude controls)
    def is_likely_control(filename):
        """Check if filename matches common control/QC/standard patterns."""
        filename_lower = filename.lower()
        return any(pattern.lower() in filename_lower for pattern in control_patterns)
    
    unmapped_df['is_control'] = unmapped_df['raw_file_name'].apply(is_likely_control)
    unmapped_samples = unmapped_df[~unmapped_df['is_control']].copy()
    
    if len(unmapped_samples) > 0:
        # Export unmapped sample files
        unmapped_file = study.study_path / "pettridge_11_076c9980_unmapped_samples.csv"
        unmapped_samples[['raw_file_name', 'treatment_code', 'ionization_mode']].to_csv(
            unmapped_file, index=False)
        print(f"💾 Saved {len(unmapped_samples)} unmapped sample files to: {unmapped_file}")
    else:
        print("✅ No unmapped sample files (all unmapped files are controls/QC)")
    
    # Step 7: Report results and next steps
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
    if len(unmapped_samples) > 0:
        print(f"   └─ Unmapped sample files: {len(unmapped_samples)} (exported for review)")
        print(f"   └─ Unmapped controls/QC: {unmatched_files - len(unmapped_samples)}")
    print("\n🧬 Biosample Coverage:")
    print(f"   Total biosamples available: {total_biosamples}")
    print(f"   Biosamples with raw data: {mapped_biosamples}")
    print(f"   Coverage: {biosample_coverage_pct:.1f}%")
    
    if unmatched_files > 0 or multiple_matches > 0:
        print("\n⚠️  MANUAL REVIEW NEEDED:")
        print("1. Review the mapping file for unmatched files")
        print("2. Customize the matching logic in this script")
        print("3. Check biosample attributes for additional matching fields")
        print("4. Verify raw file naming conventions")
        print("5. Re-run this script after making changes")
    
    if matched_files > 0:
        print("\n✅ READY FOR NEXT STEPS:")
        print("1. Review and validate the mapping file")
        if len(unmapped_samples) > 0:
            print("2. Review unmapped sample files for potential biosample issues")
            print("3. Filter raw files to only include matched samples")
            print("4. Proceed with WDL JSON generation")
        else:
            print("2. Filter raw files to only include matched samples")
            print("3. Proceed with WDL JSON generation")
    
    print(f"\n📁 Output Files:")
    print(f"   Mapping file: {mapping_file}")
    if len(unmapped_samples) > 0:
        print(f"   Unmapped samples: {unmapped_file}")
    print(f"   Biosample attributes: {biosample_csv}")
    
    # Exit with code 0 if we have at least some matches and no multiple matches
    # Exit with code 1 only if we have zero matches or multiple matches (serious errors)
    if matched_files == 0:
        print("\n❌ FATAL: No files matched to biosamples. Cannot proceed.")
        return 1
    elif multiple_matches > 0:
        print("\n❌ FATAL: Multiple biosample matches detected. Manual review required.")
        return 1
    else:
        print(f"\n✅ SUCCESS: {matched_files} files successfully mapped to biosamples.")
        return 0


if __name__ == "__main__":
    sys.exit(main())