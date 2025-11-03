"""
This script will use the NMDC API utilities package to get all the biosamples from
the EMP 500 project pull out the biosample id, gold id, and biosample name

Then it will create a mapping from biosample id to raw data file name.
"""
import pandas as pd
import re
from nmdc_api_utilities.biosample_search import BiosampleSearch
import urllib.parse



def prep_biosample_df ():
    # Fetch all biosamples associated with the study "nmdc:sty-11-r2h77870" in the NMDC database
    biosample_search = BiosampleSearch()
    biosamples = biosample_search.get_record_by_filter(
        filter='{"associated_studies":"nmdc:sty-11-r2h77870"}',
        max_page_size=1000,
        fields="id,name,samp_name",
        all_pages=True,
    )
    biosample_df = pd.DataFrame(biosamples)
    
    # From biosample_df, label each row as "rhizo", "root", "soil", or "leaf" 
    # based on samp_name 
    # if "endosphere" in samp_name, "root"; if "leaf" in samp_name, "leaf"; if "rhizo" in samp_name, "rhizo"; if "soil" in samp_name, "soil";
    biosample_df["tissue_type"] = pd.NA
    biosample_df.loc[biosample_df["samp_name"].str.contains("endosphere", case=False), "tissue_type"] = "root"
    biosample_df.loc[biosample_df["samp_name"].str.contains("leaf", case=False), "tissue_type"] = "leaf"
    biosample_df.loc[biosample_df["samp_name"].str.contains("rhizo", case=False), "tissue_type"] = "rhizo"
    biosample_df.loc[biosample_df["samp_name"].str.contains("soil", case=False), "tissue_type"] = "soil"
    assert biosample_df["tissue_type"].notna().all(), "Some biosamples do not have a tissue type assigned."

    # Remove the row with "GW-4579-CL1_25_23_RD_R2 endosphere" as biosample, this is indistinguishable from "GW-4579-CL1_25_23 endosphere" and causing issues in our mappings
    biosample_df = biosample_df[biosample_df["samp_name"] != "GW-4579-CL1_25_23_RD_R2 endosphere"]

    return biosample_df

def extract_sample_info_from_filename(filename):
    """
    Extract sample information from the filename.
    
    Pattern analysis from your data:
    - Files contain patterns like: GW-9591-Corv-LM_2, SKWA-24-3-Clat-LM_2, etc.
    - The key parts seem to be: [SAMPLE_ID]-[LOCATION]-[TISSUE_TYPE]_[REPLICATE]
    - Location codes: Corv (Corvallis), Clat (Clatskanie) 
    - Tissue types: LM (leaf?), RM (root?), RZM (rhizo?)
    - Replicate number: _1, _2, _3, etc.
    """
    
    # Extract the core sample identifier pattern
    # Look for patterns like: GW-9591-Corv-LM_2, SKWA-24-3-Clat-RM_1, etc.
    pattern = r'([A-Z]+-[0-9]+-[0-9]*-?[A-Za-z]+-[A-Z]+_[0-9]+)'
    match = re.search(pattern, filename)
    
    if match:
        sample_part = match.group(1)
        
        # Extract location (Corv/Clat)
        location = None
        if 'Corv' in sample_part:
            location = 'Co'  # Convert to Co format used in biosample names
        elif 'Clat' in sample_part:
            location = 'CL'  # Convert to CL format used in biosample names
        
        # Extract tissue type
        tissue = None
        if '-LM_' in sample_part:
            tissue = 'leaf'
        elif '-RM_' in sample_part:
            tissue = 'root' 
        elif '-RZM_' in sample_part:
            tissue = 'rhizo'
        
        # Extract replicate number
        replicate_match = re.search(r'_([0-9]+)$', sample_part)
        replicate = replicate_match.group(1) if replicate_match else None
        
        # Extract base sample ID (everything before location)
        base_id_match = re.search(r'([A-Z]+-[0-9]+-?[0-9]*)', sample_part)
        if base_id_match:
            base_id = base_id_match.group(1)
        else:
            base_id = None
            
        return {
            'sample_part': sample_part,
            'base_id': base_id,
            'location': location,
            'tissue': tissue,
            'replicate': replicate
        }
    
    return None

def map_files_to_biosamples(ftp_df, biosample_df):
    """
    Map raw data files to biosamples based on filename patterns.
    """
    
    # Create a copy to avoid modifying original dataframes
    result_df = ftp_df.copy()
    
    # Initialize mapping columns
    result_df['extracted_info'] = None
    result_df['biosample_id'] = None
    
    for idx, row in result_df.iterrows():
        filename = row['raw_data_file_short']
        
        # Extract sample information
        sample_info = extract_sample_info_from_filename(filename)
        result_df.at[idx, 'extracted_info'] = str(sample_info) if sample_info else None
        
        if sample_info and sample_info['base_id'] and sample_info['location'] and sample_info['tissue'] and sample_info['replicate']:
            
            # Try to find matching biosample
            base_id = sample_info['base_id']
            location = sample_info['location']
            tissue = sample_info['tissue']
            replicate = sample_info['replicate']
            
            # Create potential matching patterns
            potential_matches = []
            
            # Look for samples that contain the base ID, location, and replicate
            for bs_idx, bs_row in biosample_df.iterrows():
                bs_name = bs_row['samp_name']
                bs_tissue = bs_row['tissue_type']
                
                # Check if base_id, location, and replicate are in the biosample name
                # Pattern should be like: GW-9591-Co2_xx_xx (where 2 is the replicate)
                location_replicate_pattern = f"{location}{replicate}"
                
                if base_id in bs_name and location_replicate_pattern in bs_name:
                    # Check tissue type compatibility - ONLY add high confidence if tissue matches
                    if ((tissue == 'leaf' and bs_tissue == 'leaf') or
                        (tissue == 'root' and bs_tissue == 'root') or  
                        (tissue == 'rhizo' and bs_tissue == 'rhizo')):
                        potential_matches.append({
                            'biosample_id': bs_row['id'],
                            'biosample_name': bs_name,
                            'confidence': 'high'
                        })
            
            # Select best match
            if potential_matches:
                # Prefer high confidence matches, then medium, then low
                high_conf = [m for m in potential_matches if m['confidence'] == 'high']
                if len(high_conf) > 1:
                    raise ValueError(f"Multiple high confidence matches found for {filename}: {high_conf}")

                if high_conf:
                    best_match = high_conf[0]
                    result_df.at[idx, 'biosample_id'] = best_match['biosample_id']
    
    return result_df

def analyze_mapping_results(mapped_df):
    """
    Analyze the mapping results and provide statistics.
    """
    total_files = len(mapped_df)
    mapped_files = len(mapped_df[mapped_df['biosample_id'].notna()])
    unmapped_files = total_files - mapped_files
    
    
    print(f"Mapping Results Summary:")
    print(f"Total files: {total_files}")
    print(f"Successfully mapped: {mapped_files} ({mapped_files/total_files*100:.1f}%)")
    print(f"Unmapped files: {unmapped_files} ({unmapped_files/total_files*100:.1f}%)")

    # print the max, min, and median of the number of mapped_files to mapped biosamples
    mapped_counts = mapped_df['biosample_id'].value_counts()
    print(f"Mapped files to biosamples: max {mapped_counts.max()}, min {mapped_counts.min()}, median {mapped_counts.median()}")

    # print the number of files with extracted info, but without mapped biosamples
    extracted_info_files = len(mapped_df[mapped_df['extracted_info'].notna() & mapped_df['biosample_id'].isna()])
    print(f"Files with extracted info but without mapped biosamples: {extracted_info_files}")

def map_files_to_instruments(mapped_df):
    """
    Add a column called "instrument_used" to each 

    Use "Thermo Orbitrap IQ-X Tribrid" as a value if IDX_ in filename, "QExactHF03" if QE in file name.  Assert that all file names have "instrument_used" filled
    """
    mapped_df['instrument_used'] = mapped_df['raw_data_file_short'].apply(
        lambda x: "Thermo Orbitrap IQ-X Tribrid" if "IDX_" in x else ("QExactHF03" if "QE" in x else None)
    )
    assert mapped_df['instrument_used'].notna().all(), "Some files are missing 'instrument_used' information."

    return mapped_df

def map_files_to_chromat_config(mapped_df):
    """
    Add a column called "chromat_configuration_name" to each row in the mapped_df DataFrame.
    Use "JGI/LBNL Metabolomics Standard LC Method - Polar HILIC-Z" if "_HILICZ_" in filename, 
    Use "JGI/LBNL Metabolomics Standard LC Method - Nonpolar C18" if "_C18_" in filename
    """
    mapped_df['chromat_configuration_name'] = mapped_df['raw_data_file_short'].apply(
        lambda x: "JGI/LBNL Metabolomics Standard LC Method - Polar HILIC-Z" if "_HILICZ_" in x else (
            "JGI/LBNL Metabolomics Standard LC Method - Nonpolar C18" if "_C18_" in x else None
        )
    )
    assert mapped_df['chromat_configuration_name'].notna().all(), "Some files are missing 'chromat_configuration_name' information."

    return mapped_df

def map_files_to_mass_spec_config(mapped_df):
    """
    Add a column called "mass_spec_configuration_name" to each row in the mapped_df DataFrame.
    Use "JGI/LBNL Standard Metabolomics Method, positive @10,20,40CE" if "_POS_" and "CE102040" in filename,
    Use "JGI/LBNL Standard Metabolomics Method, negative @10,20,40CE" if "_NEG_" and "CE102040" in filename,
    Use "JGI/LBNL Standard Metabolomics Method, positive @20,50,60CE" if "_POS_" and "CE205060" in filename,
    Use "JGI/LBNL Standard Metabolomics Method, negative @20,50,60CE" if "_NEG_" and "CE205060" in filename.
    """
    mapped_df['mass_spec_configuration_name'] = mapped_df['raw_data_file_short'].apply(
        lambda x: "JGI/LBNL Standard Metabolomics Method, positive @10,20,40CE" if "_POS_" in x and "CE102040" in x else (
            "JGI/LBNL Standard Metabolomics Method, negative @10,20,40CE" if "_NEG_" in x and "CE102040" in x else (
                "JGI/LBNL Standard Metabolomics Method, positive @20,50,60CE" if "_POS_" in x and "CE205060" in x else (
                    "JGI/LBNL Standard Metabolomics Method, negative @20,50,60CE" if "_NEG_" in x and "CE205060" in x else None
                )
            )
        )
    )
    assert mapped_df['mass_spec_configuration_name'].notna().all(), "Some files are missing 'mass_spec_configuration_name' information."

    return mapped_df

def convert_ftp_to_https_massive(ftp_url):
    """
    Convert FTP URL to HTTPS URL for MASSIVE.
    """
    if ftp_url.startswith("ftp:/"):
        # ftp://massive-ftp.ucsd.edu/v02/MSV000083475/raw/RAW/PLATE1and2/1A10_1_12_mayer-34-s008-a04.raw
        # turns into
        # https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f.MSV000083475/raw/RAW/PLATE1and2/1A10_1_12_mayer-34-s008-a04.raw&forceDownload=true
        # Remove "ftp://massive-ftp.ucsd.edu" and replace with "https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file="
        https_stub = "https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f."
        # remove everything before MSV
        file_loc = "MSV" + ftp_url.split("MSV")[1]
        # url encode the ftp_path
        file_loc_encoded = urllib.parse.quote(file_loc, safe='')
        https_url = https_stub + file_loc_encoded + "&forceDownload=true"
        return https_url
    else:
        raise ValueError("FTP URL must start with 'ftp://massive-ftp.ucsd.edu'")

# Example usage:
if __name__ == "__main__":
    # Load your data
    ftp_df = pd.read_csv('_bioscales_lcms_metabolomics/bioscales_ftp_locs.csv')
    biosample_df = prep_biosample_df()
    
    print("Sample of FTP data:")
    print(ftp_df.head(3))
    print(f"\nTotal FTP files: {len(ftp_df)}")
    
    print("\nSample of biosample data:")  
    print(biosample_df.head(3))
    print(f"\nTotal biosamples: {len(biosample_df)}")
    
    # Perform mapping
    print("\nPerforming mapping...")
    mapped_df = map_files_to_biosamples(ftp_df, biosample_df)
    
    # Analyze results
    analyze_mapping_results(mapped_df)
    
    # Remove rows with missing biosample ids and save.  These do not have matched biosamples in NMDC
    mapped_df = mapped_df[mapped_df['biosample_id'].notna()]

    # Map to instrument
    mapped_df = map_files_to_instruments(mapped_df)

    # Map to chromatographic configuration
    mapped_df = map_files_to_chromat_config(mapped_df)

    # Map to mass spectrometry configuration
    mapped_df = map_files_to_mass_spec_config(mapped_df)

    # Convert FTP URLs to HTTPS
    mapped_df["url"] = mapped_df["ftp_location"].apply(convert_ftp_to_https_massive)

    # Add static metadata
    mapped_df["biosample.associated_studies"] = "['nmdc:sty-11-r2h77870']"
    mapped_df["material_processing_type"] = "unknown"


    #TODO: finish this

    # Save the final mapped DataFrame
    mapped_df.to_csv('_bioscales_lcms_metabolomics/mapped_files_to_biosamples.csv', index=False)
    print(f"\nResults saved to '_bioscales_lcms_metabolomics/mapped_files_to_biosamples.csv'")