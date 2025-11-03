# -*- coding: utf-8 -*-
from pathlib import Path

from nmdc_ms_metadata_gen.metadata_generator import NMDCMetadataGenerator


def generate_c18_protocol(generator):
    # Make a protocol record for the LC-MS method that we'll add to both the mass spectrometry and chromatography configurations
    return generator.generate_protocol(
        name="JGI/LBNL Metabolomics - Standard LC-MS/MS ESI Method - Nonpolar C18",
        url="https://www.protocols.io/view/jgi-lbnl-metabolomics-standard-lc-ms-ms-esi-method-ewov19r27lr2/v1",
    )


def generate_hilic_protocol(generator):
    # Make a protocol record for the LC-MS method that we'll add to both the mass spectrometry and chromatography configurations
    return generator.generate_protocol(
        name="JGI/LBNL Metabolomics - Standard LC-MS/MS ESI Method - Polar HILIC-Z",
        url="https://www.protocols.io/view/jgi-lbnl-metabolomics-standard-lc-ms-ms-esi-method-kxygxydwkl8j/v1",
    )


def generate_mass_spectrometry_configs(generator, client_secret, client_id):
    # mass spec configurations (C18 and HILIC use same MS settings)
    bioscales_orbi_positive_20 = generator.generate_mass_spectrometry_configuration(
        name="JGI/LBNL Standard Metabolomics Method, positive @20 CE",
        description="Mass spectrometry configuration for LC-MS metabolomics data for standard JGI/LBNL Metabolomics analysis, positive polarity. "
        "This configuration uses an Orbitrap mass spectrometer with data-dependent acquisition "
        "with HCD fragmentation at 20 eV",
        mass_spectrometry_acquisition_strategy="data_dependent_acquisition",
        resolution_categories=["high"],
        mass_analyzers=["Orbitrap"],
        ionization_source="electrospray_ionization",
        mass_spectrum_collection_modes=["centroid"],
        polarity_mode="positive",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
        protocol_link=generate_hilic_protocol(generator),
    )

    return [bioscales_orbi_positive_20]


# Example usage of NMDCMetadataGenerator
if __name__ == "__main__":
    generator = NMDCMetadataGenerator()

    # Load credentials from the config file
    client_id, client_secret = generator.load_credentials()

    # Make a database object
    db = generator.start_nmdc_database()

    # Generate mass spectrometry configurations
    mass_spectrometry_configs = generate_mass_spectrometry_configs(
        generator, client_secret, client_id
    )

    for config in mass_spectrometry_configs:
        db.configuration_set.append(config)

    # Dump the database to JSON
    generator.dump_nmdc_database(
        nmdc_database=db,
        json_path=Path("_ecofab_lcms_11_ev70y104/metadata/configs.json"),
    )
    print("Database dumped to _ecofab_lcms_11_ev70y104/metadata/configs.json")

    result = generator.validate_nmdc_database(
        json="_ecofab_lcms_11_ev70y104/metadata/configs.json",
        use_api=False,
    )
    assert result['result']=='All Okay!'
    print("Database validation passed")

    # Submit the json (submitted via SWAGGER UI)
    """
    generator.json_submit(
        json="_ecofab_lcms_11_ev70y104/metadata/configs.json",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret,
    )
    print("Database submitted to NMDC API")
    """