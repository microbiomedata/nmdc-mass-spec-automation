"""
This script will use the NMDC API utilities package to get all the biosamples from
the EMP 500 project pull out the biosample id, gold id, and biosample name

Then it will create a mapping from biosample id to raw data file name.
"""
import pandas as pd
from nmdc_api_utilities.biosample_search import BiosampleSearch
import requests

def fetch_biosample_report(accession_id):
    """
    Fetch a BioSample report using the `/biosample/accession/{accessions}/biosample_report` endpoint.

    Args:
        accession_id (str): The accession ID for the BioSample (e.g., SAMN12345678).

    Returns:
        dict or None: The JSON response containing the BioSample report, or None if there's an error.
    """
    # Define the API base URL and endpoint
    base_url = "https://api.ncbi.nlm.nih.gov/datasets/v2"
    endpoint = f"/biosample/accession/{accession_id}/biosample_report"

    # Construct the full URL
    url = f"{base_url}{endpoint}"

    try:
        # Send the GET request to the endpoint
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:
            return response.json()  # Parse and return the JSON response
        else:
            print(f"Error: Unable to fetch data for accession ID {accession_id}. HTTP Status Code: {response.status_code}")
            print(f"Message: {response.text}")
            return None

    except requests.RequestException as e:
        print(f"An error occurred while making the request: {e}")
        return None

def fetch_lcms_id_from_biosample_report(biosample_report):
    # from report, find attributes, then get the lcms_sample_name_cmn attribute value
    bio_sample_attributes = biosample_report.get("attributes", [])
    for attr in bio_sample_attributes:
        if attr.get("name") == "lcms_sample_name_cmn":
            return attr.get("value")
    return None

def fetch_submitter_id_from_biosample_report(biosample_report):
    # from report, find attributes, then get the Submitter Id attribute value
    bio_sample_attributes = biosample_report.get("attributes", [])
    for attr in bio_sample_attributes:
        if attr.get("name") == "Submitter Id":
            return attr.get("value")
    return None

def convert_ncbi_id_to_attributes(ncbi_id):
    biosample_report = fetch_biosample_report(ncbi_id)['reports'][0]
    lcms_name = fetch_lcms_id_from_biosample_report(biosample_report)
    submitter_id = fetch_submitter_id_from_biosample_report(biosample_report)
    if lcms_name is None:
        print(f"Warning: No LC-MS name found for NCBI ID {ncbi_id}.")
    if submitter_id is None:
        print(f"Warning: No Submitter ID found for NCBI ID {ncbi_id}.")
    return (
        lcms_name if lcms_name else "Unknown LC-MS Name",
        submitter_id if submitter_id else "Unknown Submitter ID"
    )

# Example usage:
if __name__ == "__main__":
    biosample_search = BiosampleSearch()
    biosamples = biosample_search.get_record_by_filter(
        filter='{"associated_studies":"nmdc:sty-11-547rwq94"}',
        max_page_size=1000,
        fields="id,name,description,gold_biosample_identifiers,samp_name,insdc_biosample_identifiers",
        all_pages=True,
    )
    # turn this into a dataframe
    df = pd.DataFrame(biosamples)
    # extract the correct insdc_biosample_identifiers (remove from a list and take off the leading "biosample:")
    df['insdc_biosample_identifiers'] = df['insdc_biosample_identifiers'].apply(lambda x: x[0].replace("biosample:", "") if x else None)
    df['lcms_name'], df['submitter_id'] = zip(*df['insdc_biosample_identifiers'].apply(lambda x: convert_ncbi_id_to_attributes(x) if x else (None, None)))
    print("finished converting NCBI IDs to LC-MS names and Submitter IDs")
    # save the dataframe to a csv file
    df.to_csv("_emp_500_lcms_metabolomics/biosample_attributes.csv", index=False)

