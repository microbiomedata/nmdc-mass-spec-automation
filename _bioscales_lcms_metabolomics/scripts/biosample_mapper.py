"""
This script will use the NMDC API utilities package to get all the biosamples from
the EMP 500 project pull out the biosample id, gold id, and biosample name

Then it will create a mapping from biosample id to raw data file name.
"""
import pandas as pd
from nmdc_api_utilities.biosample_search import BiosampleSearch


if __name__ == "__main__":
    # Fetch all biosamples associated with the study "nmdc:sty-11-r2h77870" in the NMDC database
    biosample_search = BiosampleSearch()
    biosamples = biosample_search.get_record_by_filter(
        filter='{"associated_studies":"nmdc:sty-11-r2h77870"}',
        max_page_size=1000,
        fields="",
        all_pages=True,
    )
    biosample_df = pd.DataFrame(biosamples)
    
    # Read in ftp locations from the bioscales_ftp_locs.csv file
    ftp_locs_df = pd.read_csv("_bioscales_lcms_metabolomics/bioscales_ftp_locs.csv")

    print("here")
