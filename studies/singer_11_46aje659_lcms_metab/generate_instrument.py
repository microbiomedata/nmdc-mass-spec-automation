# This script generates an instrument records for the 
# Thermo Orbitrap ID-X

# -*- coding: utf-8 -*-
from nmdc_ms_metadata_gen.metadata_generator import NMDCMetadataGenerator
from pathlib import Path

JSON_PATH = "studies/singer_11_46aje659_lcms_metab/metadata/nmdc_submission_packages/instrument.json"

# Example usage of NMDCMetadataGenerator
if __name__ == "__main__":
    generator = NMDCMetadataGenerator()

    # Load credentials from the config file
    client_id, client_secret = generator.load_credentials()

    # Make a database object
    db = generator.start_nmdc_database()

    # Generate instrument records 
    instrument = generator.generate_instrument(
        name="Thermo Orbitrap ID-X",
        description="Thermo Orbitrap ID-X Tribrid Mass Spectrometer",
        model="orbitrap_idx_tribrid",
        vendor="thermo_fisher",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret
    )
    db.instrument_set.append(instrument)

    # Dump the database to JSON
    generator.dump_nmdc_database(nmdc_database=db, json_path=JSON_PATH)
    generator.validate_nmdc_database(json=JSON_PATH, use_api=False)
    generator.validate_nmdc_database(json=JSON_PATH, use_api=True)
    print(f"Database dumped to {JSON_PATH}")