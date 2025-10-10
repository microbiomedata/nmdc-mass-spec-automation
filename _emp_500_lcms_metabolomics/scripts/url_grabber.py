import json
import csv
import pandas as pd

def extract_lcms_urls(json_file_path, output_csv_path):
    """
    Extract URLs for DataObjects with data_object_type "LC-MS Metabolomics Results"
    and save them to a CSV file.
    """
    
    # Read the JSON file
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    
    # List to store the extracted information
    lcms_urls = []
    
    # Navigate through the JSON structure to find DataObjects
    if 'data_object_set' in data:
        for data_object in data['data_object_set']:
            # Check if this is an LC-MS Metabolomics Results object
            if data_object.get('data_object_type') == 'LC-MS Metabolomics Results':
                # Extract relevant information
                url = data_object.get('url', '')
                object_id = data_object.get('id', '')
                
                lcms_urls.append({
                    'id': object_id,
                    'url': url
                })
    
    # Save to CSV
    if lcms_urls:
        df = pd.DataFrame(lcms_urls)
        df.to_csv(output_csv_path, index=False)
        print(f"Found {len(lcms_urls)} LC-MS Metabolomics Results URLs")
        print(f"Data saved to {output_csv_path}")
        
        # Display first few rows
        print("\nFirst few entries:")
        print(df.head())
    else:
        print("No LC-MS Metabolomics Results found in the JSON file")
    
    return lcms_urls

if __name__ == "__main__":
    # File paths
    json_file = "_emp_500_lcms_metabolomics/metadata/workflow_metadata_batch1_202508.json"
    csv_output = "emp_500_lcms_metab_result_urls.csv"
    
    # Extract URLs and save to CSV
    urls = extract_lcms_urls(json_file, csv_output)