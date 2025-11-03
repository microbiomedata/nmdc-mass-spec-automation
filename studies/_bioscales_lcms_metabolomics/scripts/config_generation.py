# -*- coding: utf-8 -*-
from nmdc_ms_metadata_gen.metadata_generator import NMDCMetadataGenerator
from pathlib import Path

def generate_c18_protocol(generator):
    # Make a protocol record for the LC-MS method that we'll add to both the mass spectrometry and chromatography configurations
    return generator.generate_protocol(
        name="JGI/LBNL Metabolomics - Standard LC-MS/MS ESI Method - Nonpolar C18",
        url="https://www.protocols.io/view/jgi-lbnl-metabolomics-standard-lc-ms-ms-esi-method-ewov19r27lr2/v1"
    )
def generate_hilic_protocol(generator):
    # Make a protocol record for the LC-MS method that we'll add to both the mass spectrometry and chromatography configurations
    return generator.generate_protocol(
        name="JGI/LBNL Metabolomics - Standard LC-MS/MS ESI Method - Polar HILIC-Z",
        url="https://www.protocols.io/view/jgi-lbnl-metabolomics-standard-lc-ms-ms-esi-method-kxygxydwkl8j/v1"
    )

def generate_mass_spectrometry_configs(generator, client_secret, client_id):
    # mass spec configurations (C18 and HILIC use same MS settings)
    bioscales_orbi_positive_102040 = generator.generate_mass_spectrometry_configuration(
        name="JGI/LBNL Standard Metabolomics Method, positive @10,20,40CE",
        description="Mass spectrometry configuration for LC-MS metabolomics data for standard JGI/LBNL Metabolomics analysis, positive polarity. " \
        "This configuration uses an Orbitrap mass spectrometer with data-dependent acquisition " \
        "with HCD fragmentation at 10, 20, and 40 eV, stepped", \
        mass_spectrometry_acquisition_strategy="data_dependent_acquisition",
        resolution_categories=["high"],
        mass_analyzers=["Orbitrap"],
        ionization_source="electrospray_ionization",
        mass_spectrum_collection_modes=["centroid"],
        polarity_mode="positive",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
        protocol_link=generate_c18_protocol(generator)
    )

    bioscales_orbi_negative_102040 = generator.generate_mass_spectrometry_configuration(
        name="JGI/LBNL Standard Metabolomics Method, negative @10,20,40CE",
        description="Mass spectrometry configuration for LC-MS metabolomics data for standard JGI/LBNL Metabolomics analysis, negative polarity. " \
        "This configuration uses an Orbitrap mass spectrometer with data-dependent acquisition " \
        "with HCD fragmentation at 10, 20, and 40 eV, stepped",
        mass_spectrometry_acquisition_strategy="data_dependent_acquisition",
        resolution_categories=["high"],
        mass_analyzers=["Orbitrap"],
        ionization_source="electrospray_ionization",
        mass_spectrum_collection_modes=["centroid"],
        polarity_mode="negative",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
        protocol_link=generate_c18_protocol(generator)
    )

    bioscales_orbi_positive_205060 = generator.generate_mass_spectrometry_configuration(
        name="JGI/LBNL Standard Metabolomics Method, positive @20,50,60CE",
        description="Mass spectrometry configuration for LC-MS metabolomics data for standard JGI/LBNL Metabolomics analysis, positive polarity. " \
        "This configuration uses an Orbitrap mass spectrometer with data-dependent acquisition " \
        "with HCD fragmentation at 20, 50, and 60 eV, stepped",
        mass_spectrometry_acquisition_strategy="data_dependent_acquisition",
        resolution_categories=["high"],
        mass_analyzers=["Orbitrap"],
        ionization_source="electrospray_ionization",
        mass_spectrum_collection_modes=["centroid"],
        polarity_mode="positive",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
        protocol_link=generate_c18_protocol(generator)
    )

    bioscales_orbi_negative_205060 = generator.generate_mass_spectrometry_configuration(
        name="JGI/LBNL Standard Metabolomics Method, negative @20,50,60CE",
        description="Mass spectrometry configuration for LC-MS metabolomics data for standard JGI/LBNL Metabolomics analysis, negative polarity. " \
        "This configuration uses an Orbitrap mass spectrometer with data-dependent acquisition " \
        "with HCD fragmentation at 20, 50, and 60 eV, stepped",
        mass_spectrometry_acquisition_strategy="data_dependent_acquisition",
        resolution_categories=["high"],
        mass_analyzers=["Orbitrap"],
        ionization_source="electrospray_ionization",
        mass_spectrum_collection_modes=["centroid"],
        polarity_mode="negative",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
        protocol_link=generate_c18_protocol(generator)
    )

    return [bioscales_orbi_positive_102040, bioscales_orbi_negative_102040,
            bioscales_orbi_positive_205060, bioscales_orbi_negative_205060]

def generate_portion_of_substance(generator, substance_name, final_concentration_value, concentration_unit):
    return generator.generate_portion_of_substance(
        substance_name=substance_name,
        final_concentration_value=final_concentration_value,
        concentration_unit=concentration_unit,
    )

def generate_all_portions_of_substance(generator):
    # Create all the portions of substances
    # Solvent B components of HILIC
    acetonitrile_94_81 = generate_portion_of_substance(generator, "acetonitrile", 94.81, "%") #99.8% of 95%
    water_4_99 = generate_portion_of_substance(generator, "water", 4.99, "%") #99.8% of 5%
    acetic_acid_0_2 = generate_portion_of_substance(generator, "acetic_acid", 0.2, "%")
    ammonium_acetate_5mM = generate_portion_of_substance(generator, "ammonium_acetate", 5, "mmol/L")

    # Solvent A components of HILIC
    # ammonium_acetate_5mM + acetic_acid_0_2 + 5 µM methylene-di-phosphonic acid +
    water_99_8 = generate_portion_of_substance(generator, "water", 99.8, "%")
    medronic_acid_5uM = generate_portion_of_substance(generator, "medronic_acid", 5, "umol/L")

    # Solvent A of RP
    water_99_9 = generate_portion_of_substance(generator, "water", 99.9, "%")
    formic_acid__1 = generate_portion_of_substance(generator, "formic_acid", 0.1, "%")

    # Solvent B of RP
    # formic_acid__1 + 
    acetonitrile_99_9 = generate_portion_of_substance(generator, "acetonitrile", 99.9, "%")

    return {
        "water_99_9": water_99_9,
        "water_99_8": water_99_8,
        "water_4_99": water_4_99,
        "acetonitrile_99_9": acetonitrile_99_9,
        "acetonitrile_94_81": acetonitrile_94_81,
        "acetic_acid_0_2": acetic_acid_0_2,
        "ammonium_acetate_5mM": ammonium_acetate_5mM,
        "formic_acid__1": formic_acid__1,
        "medronic_acid_5uM": medronic_acid_5uM,
    }

def generate_hilic_chromatography_config(generator, client_id, client_secret, portions_of_substance):
    # Make a configuration record for LC-MS HILIC chromatography configuration
    hilic_solvent_A = generator.generate_mobile_phase_segment(
        duration_value=11,
        duration_unit="minute",
        substances_used=[portions_of_substance["water_99_8"], portions_of_substance["acetic_acid_0_2"],
                         portions_of_substance["ammonium_acetate_5mM"], portions_of_substance["medronic_acid_5uM"]]
    )
    hilic_solvent_B = generator.generate_mobile_phase_segment(
        duration_value=11,
        duration_unit="minute",
        substances_used=[portions_of_substance["acetonitrile_94_81"], portions_of_substance["water_4_99"],
                         portions_of_substance["ammonium_acetate_5mM"], portions_of_substance["formic_acid__1"]]
    )
    hilic_chromatography_config = generator.generate_chromatography_configuration(
        name="JGI/LBNL Metabolomics Standard LC Method - Polar HILIC-Z",
        description="HILIC Chromatography configuration for standard JGI/LBNL Metabolomics analysis for polar compounds. " \
        "This configuration uses a HILIC column (InfinityLab Poroshell 120 HILIC-Z, 2.1x150 mm, 2.7 um, Agilent, #683775-924) held at 40 degC, " \
        "with mobile phase solvents running at a flow rate of 0.45 mL/min. " \
        "For each sample, 2 uL were injected onto the column.",
        chromatographic_category="liquid_chromatography",
        ordered_mobile_phases=[hilic_solvent_B, hilic_solvent_A],
        stationary_phase="HILIC",
        temperature_value=40,
        temperature_unit="Cel",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
        protocol_link=generate_hilic_protocol(generator)
    )
    return hilic_chromatography_config

def generate_c18_chromatography_config(generator, client_id, client_secret, portions_of_substance):
    # Make a configuration record for LC-MS C18 chromatography configuration
    c18_mobile_phase_segment_A = generator.generate_mobile_phase_segment(
        duration_value=11,
        duration_unit="minute",
        substances_used=[portions_of_substance["water_99_9"], portions_of_substance["formic_acid__1"]]
    )
    c18_mobile_phase_segment_B = generator.generate_mobile_phase_segment(
        duration_value=11,
        duration_unit="minute",
        substances_used=[portions_of_substance["acetonitrile_99_9"], portions_of_substance["formic_acid__1"]]
    )
    c18_chromatography_config = generator.generate_chromatography_configuration(
        name="JGI/LBNL Metabolomics Standard LC Method - Nonpolar C18",
        description="C18 Chromatography configuration for standard JGI/LBNL Metabolomics analysis for nonpolar compounds. " \
        "The LC was equipped with a C18 column (Agilent ZORBAX Eclipse Plus C18, Rapid Resolution HD, " \
        "2.1 x 50 mm, 1.8 μm) held at 60 degC with mobile phase solvents running at a flow rate of 0.4 mL/min. " \
        "For each sample, 2 uL were injected onto the column.",
        chromatographic_category="liquid_chromatography",
        ordered_mobile_phases=[c18_mobile_phase_segment_B, c18_mobile_phase_segment_A],
        stationary_phase="C18",
        temperature_value=60,
        temperature_unit="Cel",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
        protocol_link=generate_c18_protocol(generator)
    )
    return c18_chromatography_config

# Example usage of NMDCMetadataGenerator
if __name__ == "__main__":
    generator = NMDCMetadataGenerator()

    # Load credentials from the config file
    client_id, client_secret = generator.load_credentials()

    # Make a database object
    db = generator.start_nmdc_database()

    # Generate mass spectrometry configurations
    mass_spectrometry_configs = generate_mass_spectrometry_configs(generator, client_secret, client_id)

    for config in mass_spectrometry_configs:
        db.configuration_set.append(config)

    # Generate all portions of substances
    portions_of_substance = generate_all_portions_of_substance(generator)

    # Make a configuration record for LC-MS HILIC chromatography configuration
    hilic_chromatography_config = generate_hilic_chromatography_config(
        generator, client_id, client_secret, portions_of_substance)
    db.configuration_set.append(hilic_chromatography_config)

    # Make a configuration record for C18 chromatography configuration
    c18_chromatography_config = generate_c18_chromatography_config(
        generator, client_id, client_secret, portions_of_substance)
    db.configuration_set.append(c18_chromatography_config)

    # Dump the database to JSON
    generator.dump_nmdc_database(nmdc_database=db, json_path=Path("_bioscales_lcms_metabolomics/metadata/configs.json"))
    generator.validate_nmdc_database(json_path=Path("_bioscales_lcms_metabolomics/metadata/configs.json"))
    print("Database dumped to _bioscales_lcms_metabolomics/metadata/configs.json")
