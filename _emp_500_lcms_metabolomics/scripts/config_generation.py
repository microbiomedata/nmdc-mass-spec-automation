# -*- coding: utf-8 -*-
from src.metadata_generator import NMDCMetadataGenerator
from pathlib import Path

# Example usage of NMDCMetadataGenerator
if __name__ == "__main__":
    generator = NMDCMetadataGenerator()

    # Load credentials from the config file
    client_id, client_secret = generator.load_credentials()

    # Make a database object
    db = generator.start_nmdc_database()

    # Make a configuration record for LC-MS Mass spec configuration
    emp500_ms_config = generator.generate_mass_spectrometry_configuration(
        name="LC-MS Metabolomics Method for EMP 500 Samples",
        description="Mass spectrometry configuration for LC-MS metabolomics data for EMP 500 samples. This configuration uses a Thermo Orbitrap mass spectrometer with data-dependent acquisition. More information can be found here: https://github.com/biocore/emp/blob/master/protocols/MetabolomicsLC.md",
        mass_spectrometry_acquisition_strategy="data_dependent_acquisition",
        resolution_categories=["high"],
        mass_analyzers=["Orbitrap"],
        ionization_source="electrospray_ionization",
        mass_spectrum_collection_modes=["profile"],
        polarity_mode="positive",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
    )
    # Add to the database
    db.configuration_set.append(emp500_ms_config)

    # Make a configuration record for LC-MS Chromatography configuration
    ## First, create all the portions of substances
    water_99_9 = generator.generate_portion_of_substance(
        substance_name="water",
        final_concentration_value=99.9,
        concentration_unit="%",
    )
    acetonitrile_99_9 = generator.generate_portion_of_substance(
        substance_name="acetonitrile",
        final_concentration_value=99.9,
        concentration_unit="%",
    )
    formic_acid__1 = generator.generate_portion_of_substance(
        substance_name="formic_acid",
        final_concentration_value=0.1,
        concentration_unit="%",
    )
    ## Then, create the mobile phase segments
    mobile_phase_segment_A = generator.generate_mobile_phase_segment(
        duration_value=11,
        duration_unit="minute",
        substances_used=[water_99_9, formic_acid__1]
    )
    mobile_phase_segment_B = generator.generate_mobile_phase_segment(
        duration_value=11,
        duration_unit="minute",
        substances_used=[acetonitrile_99_9, formic_acid__1]
    )

    ## Finally, create the chromatography configuration
    emp500_chromat_config = generator.generate_chromatography_configuration(
        name="LC-MS Chromatography Configuration for EMP 500 Samples",
        description="Chromatography configuration for LC-MS metabolomics data for EMP 500 samples. This configuration uses a C18 column with a gradient of water and acetonitrile. More information can be found here: https://github.com/biocore/emp/blob/master/protocols/MetabolomicsLC.md",
        chromatographic_category="liquid_chromatography",
        ordered_mobile_phases=[mobile_phase_segment_A, mobile_phase_segment_B],
        stationary_phase="C18",
        temperature_value=40,
        temperature_unit="Cel",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
    )
    # Add to the database
    db.configuration_set.append(emp500_chromat_config)


    # Dump the database to JSON
    generator.dump_nmdc_database(nmdc_database=db, json_path=Path("emp500_configs.json"))

    generator.validate_nmdc_database(json_path=Path("emp500_configs.json"))
    print("Database dumped to emp500_configs.json")
