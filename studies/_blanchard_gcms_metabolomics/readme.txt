27 files to process

Study id:

- nmdc:sty-11-8ws97026

Raw data located here: 

- /Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_blanchard_metabolomics/raw

Processed data located here:

- /Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_blanchard_metabolomics/processed_20250205

Mapping located here:

- /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_blanchard_metabolomics/blanchard_metabolomics_metadata2.csv

Minio locations (in metabolomics bucket):
- blanchard_11_8ws97026/raw
- blanchard_11_8ws97026/processed_20250205

NMDC metadata dumped here:
- /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_blanchard_metabolomics/nmdc_metadata.json




###### metadata script
from datetime import datetime
from metadata_generator import GCMSMetabolomicsMetadataGenerator

if __name__ == "__main__":
    # Set up output file with datetime stame
    output_file = (
        "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_blanchard_metabolomics/nmdc_metadata.json"
    )

    # Start the metadata generation setup
    generator = GCMSMetabolomicsMetadataGenerator(
        metadata_file="/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_blanchard_metabolomics/blanchard_metabolomics_metadata2.csv",
        database_dump_json_path=output_file,
        raw_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/blanchard_11_8ws97026/raw",
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/blanchard_11_8ws97026/processed_20250205",
        minting_config_creds="metaMS/nmdc_lipidomics_metadata_generation/.config.yaml",
    )

    # Run the metadata generation process
    generator.run()
