"""
Integration tests for NMDCWorkflowDataProcessManager mixin.

Tests end-to-end WDL JSON generation and script creation with real file structures.
Focuses on testing workflow manager logic, not WDL execution.
"""

import json
import pytest
import shutil
from pathlib import Path
import pandas as pd
from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager


# Get test data directory
TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.mark.integration
class TestWDLGenerationEndToEnd:
    """Integration tests for complete WDL JSON generation pipeline."""

    def test_lcms_json_and_script_generation(self, tmp_path, integration_lcms_config):
        """Test complete LCMS workflow: JSON generation + script creation."""
        # Setup paths
        integration_lcms_config["paths"]["base_directory"] = str(tmp_path)
        config_file = tmp_path / "lcms_config.json"
        config_file.write_text(json.dumps(integration_lcms_config))
        
        # Create workflow_inputs directory with dummy files
        workflow_inputs_dir = tmp_path / "workflow_inputs"
        workflow_inputs_dir.mkdir(parents=True)
        
        # Create minimal dummy MSP file (just header, no actual data needed)
        (workflow_inputs_dir / "20250407_database.msp").write_text(
            "NAME: Dummy Reference Database\n"
            "PRECURSORMZ: 100.0\n"
            "PRECURSORTYPE: [M+H]+\n"
            "Num Peaks: 0\n\n"
        )
        
        # Create minimal dummy TOML files
        (workflow_inputs_dir / "metams_rp_corems.toml").write_text(
            "[MolecularSearch]\n"
            "url_database = 'postgresql://localhost:5432/coremsdb'\n"
        )
        (workflow_inputs_dir / "metams_jgi_scan_translator.toml").write_text(
            "[ScanTranslation]\n"
            "enabled = true\n"
        )
        
        # Create directory structure
        workflow_name = integration_lcms_config["workflow"]["name"]
        workflow_dir = tmp_path / "studies" / workflow_name
        metadata_dir = workflow_dir / "metadata"
        metadata_dir.mkdir(parents=True)
        raw_info_dir = workflow_dir / "raw_file_info"
        raw_info_dir.mkdir(parents=True)
        scripts_dir = workflow_dir / "scripts"
        scripts_dir.mkdir(parents=True)
        
        # Copy test inspection results
        shutil.copy(
            TEST_DATA_DIR / "raw_file_info" / "lcms_inspection_results.csv",
            raw_info_dir / "raw_file_inspection_results.csv"
        )
        
        # Read inspection results to get actual file names
        inspection_df = pd.read_csv(raw_info_dir / "raw_file_inspection_results.csv")
        
        # Create raw files matching inspection results
        data_dir = tmp_path / "data" / integration_lcms_config["study"]["name"] / "raw"
        data_dir.mkdir(parents=True)
        for file_name in inspection_df["file_name"]:
            (data_dir / file_name).touch()
        
        # Create mapped_raw_files.csv
        mapped_df = pd.DataFrame({
            "raw_file_path": [str(data_dir / f) for f in inspection_df["file_name"]]
        })
        mapped_df.to_csv(metadata_dir / "mapped_raw_files.csv", index=False)
        
        # Copy biosample mapping from test data
        shutil.copy(
            TEST_DATA_DIR / "metadata" / "lcms_biosample_mapping.csv",
            metadata_dir / "rp_pos_biosample_mapping.csv"
        )
        shutil.copy(
            TEST_DATA_DIR / "metadata" / "lcms_biosample_mapping.csv",
            metadata_dir / "rp_neg_biosample_mapping.csv"
        )
        
        # Run generation
        manager = NMDCWorkflowManager(str(config_file))
        manager.generate_wdl_jsons(batch_size=10)
        manager.generate_wdl_runner_script()
        
        # Verify outputs
        wdl_dir = workflow_dir / "wdl_jsons"
        assert wdl_dir.exists()
        
        json_files = list(wdl_dir.glob("**/*.json"))
        assert len(json_files) > 0, "Should generate WDL JSON files"
        
        # Verify JSON structure
        with open(json_files[0]) as f:
            wdl_json = json.load(f)
        assert "lcmsLipidomics.runMetaMSLCMSLipidomics.file_paths" in wdl_json
        assert len(wdl_json["lcmsLipidomics.runMetaMSLCMSLipidomics.file_paths"]) > 0
        
        # Verify script
        script_path = workflow_dir / "scripts" / f"{workflow_name}_wdl_runner.sh"
        assert script_path.exists()
        assert "#!/bin/bash" in script_path.read_text()

    def test_gcms_with_calibration_files(self, tmp_path, integration_gcms_config):
        """Test GCMS workflow with calibration file assignment."""
        # Setup
        integration_gcms_config["paths"]["base_directory"] = str(tmp_path)
        config_file = tmp_path / "gcms_config.json"
        config_file.write_text(json.dumps(integration_gcms_config))
        
        # Create workflow_inputs directory with dummy files
        workflow_inputs_dir = tmp_path / "workflow_inputs"
        workflow_inputs_dir.mkdir(parents=True)
        
        # Create minimal dummy TOML file for GCMS
        (workflow_inputs_dir / "metams_gcms_corems.toml").write_text(
            "[MolecularSearch]\n"
            "url_database = 'postgresql://localhost:5432/coremsdb'\n"
        )
        
        # Create structure
        workflow_name = integration_gcms_config["workflow"]["name"]
        workflow_dir = tmp_path / "studies" / workflow_name
        metadata_dir = workflow_dir / "metadata"
        raw_info_dir = workflow_dir / "raw_file_info"
        metadata_dir.mkdir(parents=True)
        raw_info_dir.mkdir(parents=True)
        
        # Copy test inspection results
        shutil.copy(
            TEST_DATA_DIR / "raw_file_info" / "gcms_inspection_results.csv",
            raw_info_dir / "raw_file_inspection_results.csv"
        )
        
        # Read inspection results to get actual file names
        inspection_df = pd.read_csv(raw_info_dir / "raw_file_inspection_results.csv")
        
        # Create raw files matching inspection results
        data_dir = tmp_path / "data" / integration_gcms_config["study"]["name"] / "raw"
        data_dir.mkdir(parents=True)
        for file_name in inspection_df["file_name"]:
            (data_dir / file_name).touch()
        
        # Create mapped_raw_files.csv
        mapped_df = pd.DataFrame({
            "raw_file_path": [str(data_dir / f) for f in inspection_df["file_name"]]
        })
        mapped_df.to_csv(metadata_dir / "mapped_raw_files.csv", index=False)
        
        # Copy biosample mapping from test data
        shutil.copy(
            TEST_DATA_DIR / "metadata" / "gcms_biosample_mapping.csv",
            metadata_dir / "mapped_raw_file_biosample_mapping.csv"
        )
        
        # Generate JSONs
        manager = NMDCWorkflowManager(str(config_file))
        manager.generate_wdl_jsons(batch_size=10)
        
        # Verify JSONs created
        wdl_dir = workflow_dir / "wdl_jsons"
        json_files = list(wdl_dir.glob("**/*.json"))
        assert len(json_files) > 0
        
        # Verify calibration file in JSON
        with open(json_files[0]) as f:
            wdl_json = json.load(f)
        assert "gcmsMetabolomics.runMetaMSGCMS.calibration_file_path" in wdl_json
        assert wdl_json["gcmsMetabolomics.runMetaMSGCMS.calibration_file_path"].endswith("calibration.cdf")
