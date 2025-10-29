from nmdc_ms_metadata_gen.lcms_metab_metadata_generator import LCMSMetabolomicsMetadataGenerator


if __name__ == "__main__":
    # Set up output file with datetime stamp
    output_file = ("/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/metadata/workflow_metadata_batch2.json")
    # Start the metadata generation setup
    generator = LCMSMetabolomicsMetadataGenerator(
        metadata_file="/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/mapped_raw_data_files_batch2.csv",
        database_dump_json_path=output_file,
        process_data_url="https://nmdcdemo.emsl.pnnl.gov/metabolomics/emp500_11_547rwq94_lcms/processed_20251021/",
        existing_data_objects=["nmdc:dobj-11-7kab3m51"],
    )
    # Run the metadata generation process, validate without the API
    metadata = generator.run()
    validate = generator.validate_nmdc_database(json=metadata, use_api=False)
    assert validate["result"] == "All Okay!"

    # Run the metadata generation process, validate with the API
    validate = generator.validate_nmdc_database(json=metadata, use_api=True)