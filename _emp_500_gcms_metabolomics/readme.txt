460 samples to process with 15 associated FAMES

Study id:

- nmdc:sty-11-547rwq94

Raw data located here: 

- /Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_emp500_metabolomics/raw

Processed data located here:

- /Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_emp500_metabolomics/processed_20250212

Mapping located here:


Minio locations (in metabolomics bucket):
- emp500_11_547rwq94/raw
- emp500_11_547rwq94/processed_20250212


Metadata script:
################
from datetime import datetime
from src.metadata_generator import GCMSMetabolomicsMetadataGenerator
import os

# Set up output file with datetime stame
output_file = (
    "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_metabolomics/emp500_metabolomics_metadata"
    + datetime.now().strftime("%Y%m%d%H%M%S")
    + ".json"
)

# Start the metadata generation setup
generator = GCMSMetabolomicsMetadataGenerator(
    metadata_file="/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_metabolomics/emp500_metabolomics_metadata.csv",
    database_dump_json_path=output_file,
    raw_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/emp500_11_547rwq94/raw/",
    process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/emp500_11_547rwq94/processed_20250212/",
)

# Run the metadata generation process
generator.run()
assert os.path.exists(output_file)

#####
Metadata json = /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_metabolomics/emp500_metabolomics_metadata20250312172507.json