from nmdc_ms_metadata_gen.lcms_metab_metadata_generator import LCMSMetabolomicsMetadataGenerator
from datetime import datetime
import os

if __name__ == "__main__":
    # Set up output file with datetime stamp
    output_file = (
        "tests/test_data/test_database_lcms_lipid_"
        + datetime.now().strftime("%Y%m%d%H%M%S")
        + ".json"
    )
    # Start the metadata generation setup
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file="path", #TODO KRH: This should have raw_dataurl and manifest information
        database_dump_json_path=output_file,
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/", #TODO KRH: This will need to be updated
        existing_data_objects= None, #TODO KRH: Add LCMS Metabolomics database here
    )
    # Run the metadata generation process
    generator.run()