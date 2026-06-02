This directory contains input files used for configuring workflows in the NMDC Mass Spec Automation system.

Some of the key input files include:
- `metams_jgi_scan_translator.toml`: A scan translator configuration file for LCMS metabolomics workflows, tailored for JGI/LBNL metabolomics data.
- `metams_hilic_corems.toml`: CoreMS configuration for HILIC chromatography for LCMS metabolomics workflows.
- `metams_rp_corems.toml`: CoreMS configuration for Reversed Phase chromatography for LCMS metabolomics workflows. Expects profile MS1 and centroided MS2.
- `metams_rp_corems_jgi.toml`: CoreMS configuration for Reversed Phase chromatography for LCMS metabolomics workflows for JGI/LBNL data. Expects centroided MS1 and centroided MS2.
- `20250407_database.msp`: An example MSP file used for metabolite identification in LCMS metabolomics workflows. This file is too large to be stored in the repository and should be obtained separately from https://nmdcdemo.emsl.pnnl.gov/metabolomics/databases/20250407_database.msp and placed in this directory. You can use the makefile command `make download-msp-db` to achieve this as well.
- `202412_lipid_ref.sqlite`: An example SQLite database file used for lipid identification in LCMS lipidomics workflows. This file is too large to be stored in the repository and should be obtained separately from https://nmdcdemo.emsl.pnnl.gov/minio/lipidomics/parameter_files/202412_lipid_ref.sqlite and placed in this directory.  You can use the makefile command `make download-lipid-db` to achieve this as well.
- `enviroms_nom_corems.toml`: CoreMS configuration for direct infusion FTICR natural organic matter workflow.
- `Hawkes_neg.ref`: commonly used reference file for CoreMS-based FTICR workflows in negative mode.