# -*- coding: utf-8 -*-
from nmdc_ms_metadata_gen.metadata_generator import NMDCMetadataGenerator
from pathlib import Path

# Example usage of NMDCMetadataGenerator
if __name__ == "__main__":
    generator = NMDCMetadataGenerator()

    # Load credentials from the config file
    client_id, client_secret = generator.load_credentials()

    # Make a database object
    db = generator.start_nmdc_database()

    # Generate instrument records 
    #TODO KRH: Add instrument record for Thermo Orbitrap IQ-X Tribrid (IQX)
    instrument = generator.generate_instrument(
        name="Thermo Orbitrap IQ-X Tribrid",
        description="Thermo Orbitrap IQ-X Tribrid (IQX) mass spectrometer.",
        model="orbitrap_eclipse_tribid",  #TODO KRH: Chang to orbitrap_iqx_tribrid when available in schema
        vendor="thermo_fisher",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret
    )
    db.instrument_set.append(instrument)

    # Dump the database to JSON
    generator.dump_nmdc_database(nmdc_database=db, json_path=Path("_bioscales_lcms_metabolomics/metadata/instrument.json"))
    generator.validate_nmdc_database(json_path=Path("_bioscales_lcms_metabolomics/metadata/instrument.json"))
    print("Database dumped to _bioscales_lcms_metabolomics/metadata/instrument.json")
