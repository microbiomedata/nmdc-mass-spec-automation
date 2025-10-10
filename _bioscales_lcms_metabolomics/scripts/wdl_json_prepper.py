"""
# This script will write multiple .json files to run the lipidomics wdl workflow, with 50 samples per batch
# Example json below:
{
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.file_paths": [
        "./test_data/test_lipid_data/Blanch_Nat_Lip_C_4_AB_M_08_NEG_25Jan18_Brandi-WCSH5801.raw",
        "./test_data/test_lipid_data/Blanch_Nat_Lip_C_4_AB_M_08_POS_23Jan18_Brandi-WCSH5801.raw"
    ],
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.output_directory": "output",
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.corems_toml_path": "./configuration/lcms_metab_configs/emsl_lcms_metab_corems_params.toml",
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.msp_file_path": "./test_data/test_lcms_metab_data/database.msp",
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.scan_translator_path": "./configuration/lcms_metab_configs/emsl_lcms_metab_scan_translator.toml",
    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.cores": 1
}

Inputs will be:
1. path to the directory containing the raw data files
2. path to the output directory (default is output)
3. path to the corems toml file
4. path to the msp file
5. path to the scan translator file
6. number of cores to use (default is 1)
7. batch size (default is 50)
8. output directory for json files
"""

import json
import pandas as pd
from pathlib import Path


def generate_batch_jsons(
    raw_data_files,
    processed_data_dir,
    corems_toml,
    db_location,
    scan_translator_path,
    cores=5,
    output_dir="output",
    batch_size=10,
    json_output_dir=".",
    problem_files=None,
):
    # Remove the exisiting files in the output directory
    json_output_dir = Path(json_output_dir)
    if json_output_dir.exists():
        for file in json_output_dir.glob("*.json"):
            file.unlink()
        print(f"Removed existing JSON files in {json_output_dir}")
    else:
        json_output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created JSON output directory: {json_output_dir}")

    # For each of the raw data files, look in the processed data directory for a corresponding .corems directory
    processed_data_files = [str(f) for f in Path(processed_data_dir).rglob("*.corems")]
    processed_data_files = [
        f.replace(str(Path(processed_data_dir)), "")
        .lstrip("/")
        .replace(".corems", ".mzML")
        for f in processed_data_files
    ]
    raw_data_files = [
        f
        for f in raw_data_files
        if f.replace(str(Path(raw_data_dir)), "").lstrip("/")
        not in processed_data_files
    ]

    print(f"Found {len(raw_data_files)} raw data files that are not processed yet")

    # Filter out problem files if specified
    if problem_files:
        original_count = len(raw_data_files)
        raw_data_files = [
            f
            for f in raw_data_files
            if not any(problem in f for problem in problem_files)
        ]
        filtered_count = original_count - len(raw_data_files)
        print(f"Filtered out {filtered_count} problem files")
        print(f"Remaining {len(raw_data_files)} files for processing")

    # Split files into batches
    batches = [
        raw_data_files[i : i + batch_size]
        for i in range(0, len(raw_data_files), batch_size)
    ]

    print(f"Creating {len(batches)} batches with up to {batch_size} files each")

    # Create JSON file for each batch
    for batch_num, batch_files in enumerate(batches, 1):
        json_obj = {
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.file_paths": batch_files,
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.output_directory": output_dir,
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.corems_toml_path": corems_toml,
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.msp_file_path": db_location,
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.scan_translator_path": scan_translator_path,
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.cores": cores,
        }

        # Create output filename
        output_json = (
            f"{json_output_dir}/run_metaMS_lcms_metabolomics_batch{batch_num}.json"
        )

        # Write the json object to a file
        with open(output_json, "w") as f:
            json.dump(json_obj, f, indent=4)

        print(f"Batch {batch_num}: {len(batch_files)} files written to {output_json}")


if __name__ == "__main__":
    raw_data_dir = "/Users/heal742/LOCAL/staging"
    all_expected = False

    # Read in the metadata file
    mapped_metadata = pd.read_csv(
        "_bioscales_lcms_metabolomics/mapped_files_to_biosamples.csv"
    )
    print(f"Total samples in metadata: {len(mapped_metadata)}")

    # RP, positive mode
    rp_pos_files = mapped_metadata[
        (
            mapped_metadata["chromat_configuration_name"]
            == "JGI/LBNL Metabolomics Standard LC Method - Nonpolar C18"
        ) & (mapped_metadata["mass_spec_configuration_name"].str.contains("positive", case=False, na=False))
    ]["raw_data_file_short"].to_list()
    rp_pos_files = [raw_data_dir + "/" + f for f in rp_pos_files]
    # check that all files exist
    if all_expected:
        if not all(Path(f).exists() for f in rp_pos_files):
            missing_files = [f for f in rp_pos_files if not Path(f).exists()]
            print(f"Warning: Some RP positive files do not exist: {missing_files}")
            assert False, "Some RP positive files are missing, please check."
    rp_pos_files = [f for f in rp_pos_files if Path(f).exists()]
    print(f"Total RP positive files: {len(rp_pos_files)}")

    # RP, negative mode
    rp_neg_files = mapped_metadata[
        (
            mapped_metadata["chromat_configuration_name"]
            == "JGI/LBNL Metabolomics Standard LC Method - Nonpolar C18"
        ) & (mapped_metadata["mass_spec_configuration_name"].str.contains("negative", case=False, na=False))
    ]["raw_data_file_short"].to_list()
    rp_neg_files = [raw_data_dir + "/" + f for f in rp_neg_files]
    if all_expected:
        if not all(Path(f).exists() for f in rp_neg_files):
            missing_files = [f for f in rp_neg_files if not Path(f).exists()]
            print(f"Warning: Some RP negative files do not exist: {missing_files}")
            assert False, "Some RP negative files are missing, please check."
    rp_neg_files = [f for f in rp_neg_files if Path(f).exists()]
    print(f"Total RP negative files: {len(rp_neg_files)}")

    # HILIC, positive mode
    hilic_pos_files = mapped_metadata[
        (
            mapped_metadata["chromat_configuration_name"]
            == "JGI/LBNL Metabolomics Standard LC Method - Polar HILIC-Z"
        ) & (mapped_metadata["mass_spec_configuration_name"].str.contains("positive", case=False, na=False))
    ]["raw_data_file_short"].to_list()
    hilic_pos_files = [raw_data_dir + "/" + f for f in hilic_pos_files]
    if all_expected:
        if not all(Path(f).exists() for f in hilic_pos_files):
            missing_files = [f for f in hilic_pos_files if not Path(f).exists()]
            print(f"Warning: Some HILIC positive files do not exist: {missing_files}")
            assert False, "Some HILIC positive files are missing, please check."
    hilic_pos_files = [f for f in hilic_pos_files if Path(f).exists()]
    print(f"Total HILIC positive files: {len(hilic_pos_files)}")

    # HILIC, negative mode
    hilic_neg_files = mapped_metadata[
        (
            mapped_metadata["chromat_configuration_name"]                   
            == "JGI/LBNL Metabolomics Standard LC Method - Polar HILIC-Z"
        ) & (mapped_metadata["mass_spec_configuration_name"].str.contains("negative", case=False, na=False))
    ]["raw_data_file_short"].to_list()
    hilic_neg_files = [raw_data_dir + "/" + f for f in hilic_neg_files]
    if all_expected:
        if not all(Path(f).exists() for f in hilic_neg_files):
            missing_files = [f for f in hilic_neg_files if not Path(f).exists()]
            print(f"Warning: Some HILIC negative files do not exist: {missing_files}")
            assert False, "Some HILIC negative files are missing, please check."
    hilic_neg_files = [f for f in hilic_neg_files if Path(f).exists()]
    print(f"Total HILIC negative files: {len(hilic_neg_files)}")

    # Must be set for the script to run
    processed_data_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_bioscales_lcms/processed_20251010"
    corems_rp_toml = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/metadata/bioscales_rp_corems.toml"
    corems_hilic_toml = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/metadata/bioscales_hilic_corems.toml"
    db_location = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/metams/test_data/test_lcms_metab_data/20250407_database.msp"
    scan_translator_path = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/metadata/bioscales_scan_translator.toml"
    # Check that all these exist
    assert Path(raw_data_dir).exists(), f"Raw data directory does not exist: {raw_data_dir}"
    assert Path(processed_data_dir).exists(), f"Processed data directory does not exist: {processed_data_dir}"
    assert Path(corems_rp_toml).exists(), f"CoreMS TOML file does not exist: {corems_rp_toml}"
    assert Path(db_location).exists(), f"Database file does not exist: {db_location}"
    assert Path(scan_translator_path).exists(), f"Scan translator file does not exist: {scan_translator_path}"

    # Optional arguments
    cores = 5
    output_dir = "output"
    batch_size = 100

    files = [rp_pos_files, rp_neg_files, hilic_pos_files, hilic_neg_files]
    output_dirs = ["rp_pos", "rp_neg", "hilic_pos", "hilic_neg"]
    parameter_files = [corems_rp_toml, corems_rp_toml, corems_hilic_toml, corems_hilic_toml]

    for file_set, out_dir, corems_toml in zip(files, output_dirs, parameter_files):
        json_output_dir = f"/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/wdl_jsons/{out_dir}"

        generate_batch_jsons(
            file_set,
            processed_data_dir,
            corems_toml,
            db_location,
            scan_translator_path,
            cores,
            output_dir,
            batch_size,
            json_output_dir
        )
