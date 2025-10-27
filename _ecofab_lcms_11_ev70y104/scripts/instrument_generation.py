# See issue: 

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
    instrument = generator.generate_instrument(
        name="Thermo Orbitrap Exploris",
        description="Thermo Orbitrap Exploris mass spectrometer.",
        model="orbitrap_q_exactive", # when available in schema release use "orbitrap_exploris"
        vendor="thermo_fisher",
        CLIENT_ID=client_id,
        CLIENT_SECRET=client_secret
    )
    db.instrument_set.append(instrument)

    # Dump the database to JSON
    generator.dump_nmdc_database(nmdc_database=db, json_path=Path("_ecofab_lcms_11_ev70104/metadata/instrument.json"))
    generator.validate_nmdc_database(json_path=Path("_ecofab_lcms_11_ev70104/metadata/instrument.json"))
    print("Database dumped to _ecofab_lcms_11_ev70104/metadata/instrument.json")

    # Submit the json
    generator.json_submit(json_path="_ecofab_lcms_11_ev70104/metadata/instrument.json", CLIENT_ID=client_id, CLIENT_SECRET=client_secret)