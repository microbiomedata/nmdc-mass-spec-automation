import os

from nmdc_ms_metadata_gen.lcms_metab_metadata_generator import LCMSMetabolomicsMetadataGenerator


if __name__ == "__main__":
    processed_data_url = "https://nmdcdemo.emsl.pnnl.gov/metabolomics/bioscales_11_r2h77870/processed_20251013/"
    # Make dictionary of metadata inputs that change per run
    metadata_inputs = [{
        "metadata_file": "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/metadata/metadata_gen_input_csvs/JGILBNL_Standard_Metabolomics_Method_negative_JGILBNL_Metabolomics_Standard_LC_Method___Nonpolar_C18_metadata.csv",
        "output_file": "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/metadata/workflow_metadata_RP_neg.json",
    },
    {
        "metadata_file": "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/metadata/metadata_gen_input_csvs/JGILBNL_Standard_Metabolomics_Method_positive_JGILBNL_Metabolomics_Standard_LC_Method___Nonpolar_C18_metadata.csv",
        "output_file": "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/metadata/workflow_metadata_RP_pos.json",
    },
    {
        "metadata_file": "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/metadata/metadata_gen_input_csvs/JGILBNL_Standard_Metabolomics_Method_negative_JGILBNL_Metabolomics_Standard_LC_Method___Polar_HILIC_Z_metadata.csv",
        "output_file": "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/metadata/workflow_metadata_HILIC_neg.json",
    },
    {
        "metadata_file": "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/metadata/metadata_gen_input_csvs/JGILBNL_Standard_Metabolomics_Method_positive_JGILBNL_Metabolomics_Standard_LC_Method___Polar_HILIC_Z_metadata.csv",
        "output_file": "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/metadata/workflow_metadata_HILIC_pos.json",
    }]

    for metadata_input in metadata_inputs:
        # Check if the output_file exists, if so, skip to next
        if os.path.exists(metadata_input["output_file"]):
            print(f"Output file {metadata_input['output_file']} already exists, skipping...")
            continue

        # Start the metadata generation setup
        generator = LCMSMetabolomicsMetadataGenerator(
            metadata_file=metadata_input["metadata_file"],
            database_dump_json_path=metadata_input["output_file"],
            process_data_url=processed_data_url,
            existing_data_objects=["nmdc:dobj-11-7kab3m51"],
        )
        # Run the metadata generation process, validate without the API
        metadata = generator.run()
        validate = generator.validate_nmdc_database(json=metadata, use_api=False)
        assert validate["result"] == "All Okay!"

        # Run the metadata generation process, validate with the API
        validate = generator.validate_nmdc_database(json=metadata, use_api=True)