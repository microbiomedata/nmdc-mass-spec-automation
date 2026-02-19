"""
Integration tests for WorkflowMetadataManager mixin.

These tests use real study data to verify end-to-end metadata generation functionality.
"""

import pytest
from pathlib import Path
import pandas as pd
import shutil
import json
import os


class TestWorkflowMetadataManagerIntegration:
    """Integration test suite for WorkflowMetadataManager mixin."""

    @pytest.mark.integration
    def test_lcms_metadata_generation_end_to_end(self, tmp_path, integration_lcms_config):
        """
        Integration test: Test complete LCMS metadata generation workflow CSV outputs.
        
        This test verifies the CSV metadata mapping generation:
        - CSV metadata mapping generation with multiple configurations
        - Configuration-specific file filtering
        - Metadata overrides with patterns
        
        Note: NMDC JSON package generation requires actual raw data files for file size
        calculations, which are not available in test environment. This test focuses on
        CSV generation which is the primary workflow manager responsibility.
        """
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Create workflow directory structure
        workflow_dir = tmp_path / "studies" / integration_lcms_config["workflow"]["name"]
        metadata_dir = workflow_dir / "metadata"
        raw_info_dir = workflow_dir / "raw_file_info"
        nmdc_packages_dir = metadata_dir / "nmdc_submission_packages"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        raw_info_dir.mkdir(parents=True, exist_ok=True)
        nmdc_packages_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy test data files
        test_data_dir = Path(__file__).parent / "test_data"
        shutil.copy(
            test_data_dir / "metadata" / "lcms_biosample_mapping.csv",
            metadata_dir / "mapped_raw_file_biosample_mapping.csv"
        )
        shutil.copy(
            test_data_dir / "raw_file_info" / "lcms_inspection_results.csv",
            raw_info_dir / "raw_file_inspection_results.csv"
        )
        
        # Create mock material processing workflowreference CSV
        # This simulates the output from generate_material_processing_metadata()
        biosample_mapping = pd.read_csv(metadata_dir / "mapped_raw_file_biosample_mapping.csv")
        workflowref_data = []
        for _, row in biosample_mapping.iterrows():
            if row["match_confidence"] == "high":
                workflowref_data.append({
                    "biosample_id": row["biosample_id"],
                    "raw_data_identifier": row["raw_file_name"],
                    "last_processed_sample": f"nmdc:procsm-99-test{len(workflowref_data):03d}"
                })
        workflowref_df = pd.DataFrame(workflowref_data)
        workflowref_df.to_csv(
            nmdc_packages_dir / "material_processing_metadata_workflowreference.csv",
            index=False
        )
        
        # Create mapped_raw_files_wprocessed_MANUAL.csv (simulates manual creation in real workflow)
        # Merge biosample mapping with workflowref data to include processed sample IDs
        manual_mapping = biosample_mapping[biosample_mapping["match_confidence"] == "high"].copy()
        manual_mapping = manual_mapping.merge(
            workflowref_df[["raw_data_identifier", "last_processed_sample"]],
            left_on="raw_file_name",
            right_on="raw_data_identifier",
            how="left"
        )
        # Keep raw_data_identifier as the filename (drop the duplicate from merge)
        manual_mapping["raw_data_identifier"] = manual_mapping["raw_file_name"]
        manual_mapping.to_csv(
            metadata_dir / "mapped_raw_files_wprocessed_MANUAL.csv",
            index=False
        )
        
        # Save config
        config_file = workflow_dir / "test_config.json"
        with open(config_file, "w") as f:
            json.dump(integration_lcms_config, f, indent=4)
        
        # Create manager
        manager = NMDCWorkflowManager(str(config_file))
        
        # Generate CSV metadata mappings (now includes processed_sample_id mapping)
        result = manager.generate_workflow_metadata_generation_inputs()
        
        # Verify generation succeeded
        assert result is True, "CSV metadata generation failed - check logs"
        
        # Verify skip trigger was set
        assert manager.should_skip("metadata_mapping_generated") is True
        
        # === Verify CSV metadata mapping files ===
        csv_output_dir = manager.workflow_path / "metadata" / "metadata_gen_input_csvs"
        assert csv_output_dir.exists(), "CSV output directory not created"
        
        csv_files = list(csv_output_dir.glob("*_metadata.csv"))
        assert len(csv_files) > 0, "No metadata CSV files generated"
        
        # Verify expected configurations
        expected_configs = ["rp_pos", "rp_neg"]
        generated_configs = [f.stem.replace("_metadata", "") for f in csv_files]
        
        for expected in expected_configs:
            assert expected in generated_configs, f"Missing CSV file for {expected} configuration"
        
        # Verify CSV contents for one configuration
        rp_pos_file = csv_output_dir / "rp_pos_metadata.csv"
        assert rp_pos_file.exists(), "rp_pos CSV file not found"
        
        df = pd.read_csv(rp_pos_file)
        
        # Check required columns for LCMS
        required_columns = [
            "sample_id",
            "raw_data_file",
            "processed_data_directory",
            "mass_spec_configuration_name",
            "chromat_configuration_name",
            "instrument_used",
            "processing_institution_workflow",
            "processing_institution_generation",
            "instrument_analysis_end_date",
            "instrument_instance_specifier",
        ]
        
        for col in required_columns:
            assert col in df.columns, f"Missing required column: {col}"
        
        # Verify data integrity
        assert len(df) > 0, "No rows in metadata file"
        assert all(df["sample_id"].str.startswith("nmdc:procsm-")), "Invalid processed sample IDs (should start with nmdc:procsm-)"
        
        # Verify processed_data_directory ends with .corems
        assert all(df["processed_data_directory"].str.endswith(".corems")), \
            "Invalid processed_data_directory format for LCMS"

    @pytest.mark.integration
    def test_gcms_metadata_generation_with_calibrations(self, tmp_path, integration_gcms_config):
        """
        Integration test: Test complete GCMS metadata generation CSV outputs.
        
        This test verifies the CSV metadata generation:
        - GCMS-specific CSV metadata generation
        - Chronological calibration file assignment
        - processed_data_file generation (CSV format)
        - Calibration file path construction
        
        Note: NMDC JSON package generation requires actual raw data files for file size
        calculations, which are not available in test environment. This test focuses on
        CSV generation which is the primary workflow manager responsibility.
        """
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Create workflow directory structure
        workflow_dir = tmp_path / "studies" / integration_gcms_config["workflow"]["name"]
        metadata_dir = workflow_dir / "metadata"
        raw_info_dir = workflow_dir / "raw_file_info"
        nmdc_packages_dir = metadata_dir / "nmdc_submission_packages"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        raw_info_dir.mkdir(parents=True, exist_ok=True)
        nmdc_packages_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy test data files
        test_data_dir = Path(__file__).parent / "test_data"
        shutil.copy(
            test_data_dir / "metadata" / "gcms_biosample_mapping.csv",
            metadata_dir / "mapped_raw_file_biosample_mapping.csv"
        )
        shutil.copy(
            test_data_dir / "raw_file_info" / "gcms_inspection_results.csv",
            raw_info_dir / "raw_file_inspection_results.csv"
        )
        
        # Create mock material processing workflowreference CSV
        # This simulates the output from generate_material_processing_metadata()
        biosample_mapping = pd.read_csv(metadata_dir / "mapped_raw_file_biosample_mapping.csv")
        workflowref_data = []
        for _, row in biosample_mapping.iterrows():
            if row["match_confidence"] == "high":
                workflowref_data.append({
                    "biosample_id": row["biosample_id"],
                    "raw_data_identifier": row["raw_file_name"],
                    "last_processed_sample": f"nmdc:procsm-99-test{len(workflowref_data):03d}"
                })
        workflowref_df = pd.DataFrame(workflowref_data)
        workflowref_df.to_csv(
            nmdc_packages_dir / "material_processing_metadata_workflowreference.csv",
            index=False
        )
        
        # Create mapped_raw_files_wprocessed_MANUAL.csv (simulates manual creation in real workflow)
        # Merge biosample mapping with workflowref data to include processed sample IDs
        manual_mapping = biosample_mapping[biosample_mapping["match_confidence"] == "high"].copy()
        manual_mapping = manual_mapping.merge(
            workflowref_df[["raw_data_identifier", "last_processed_sample"]],
            left_on="raw_file_name",
            right_on="raw_data_identifier",
            how="left"
        )
        # Keep raw_data_identifier as the filename (drop the duplicate from merge)
        manual_mapping["raw_data_identifier"] = manual_mapping["raw_file_name"]
        manual_mapping.to_csv(
            metadata_dir / "mapped_raw_files_wprocessed_MANUAL.csv",
            index=False
        )
        
        # Save config
        config_file = workflow_dir / "test_config.json"
        with open(config_file, "w") as f:
            json.dump(integration_gcms_config, f, indent=4)
        
        # Create manager
        manager = NMDCWorkflowManager(str(config_file))
        
        # Verify prerequisites exist
        biosample_mapping_path = manager.workflow_path / "metadata" / "mapped_raw_file_biosample_mapping.csv"
        assert biosample_mapping_path.exists(), f"Biosample mapping not found at {biosample_mapping_path}"
        
        # Verify calibration file exists in mapping
        mapping_df = pd.read_csv(biosample_mapping_path)
        calibration_files = mapping_df[mapping_df["raw_file_type"] == "calibration"]
        assert len(calibration_files) > 0, "No calibration files found in biosample mapping"
        
        # Generate CSV metadata mappings (now includes processed_sample_id mapping)
        result = manager.generate_workflow_metadata_generation_inputs()
        
        # Verify generation succeeded
        assert result is True, "CSV metadata generation failed - check logs"
        
        # Verify skip trigger was set
        assert manager.should_skip("metadata_mapping_generated") is True
        
        # === Verify CSV metadata mapping files ===
        csv_output_dir = manager.workflow_path / "metadata" / "metadata_gen_input_csvs"
        assert csv_output_dir.exists(), "CSV output directory not created"
        
        csv_files = list(csv_output_dir.glob("*_metadata.csv"))
        assert len(csv_files) > 0, "No metadata CSV files generated"
        
        # Verify GCMS configuration exists
        gcms_file = csv_output_dir / "gcms_metadata.csv"
        assert gcms_file.exists(), "GCMS CSV metadata file not created"
        
        # Verify CSV contents
        df = pd.read_csv(gcms_file)
        
        # Check required columns for GCMS
        required_columns = [
            "sample_id",
            "raw_data_file",
            "processed_data_file",
            "calibration_file",
            "mass_spec_configuration_name",
            "chromat_configuration_name",
            "instrument_used",
            "processing_institution_workflow",
            "processing_institution_generation",
            "instrument_analysis_end_date",
            "instrument_instance_specifier",
        ]
        
        for col in required_columns:
            assert col in df.columns, f"Missing required column: {col}"
        
        # Verify data integrity
        assert len(df) > 0, "No rows in metadata file"
        assert all(df["sample_id"].str.startswith("nmdc:procsm-")), "Invalid processed sample IDs (should start with nmdc:procsm-)"
        
        # Verify calibration files were assigned
        assert all(df["calibration_file"].notna()), "Some samples missing calibration file"
        assert all(df["calibration_file"].str.contains("GCMS_FAMEs")), \
            "Calibration files not properly assigned"
        
        # Verify processed_data_file format (should be CSV for GCMS)
        assert all(df["processed_data_file"].str.endswith(".csv")), \
            "Invalid processed_data_file format for GCMS (should be .csv)"

    @pytest.mark.integration
    def test_metadata_generation_error_handling(self, tmp_path):
        """
        Integration test: Test metadata generation with missing prerequisites.
        
        Verifies that metadata generation properly handles:
        - Missing biosample mapping file
        - Missing raw inspection results
        - No high-confidence matches
        """
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Create a minimal config
        config = {
            "study": {
                "name": "test_error_handling",
                "id": "nmdc:sty-11-test",
                "description": "Test study for error handling"
            },
            "workflow": {
                "name": "test_workflow",
                "workflow_type": "LCMS Metabolomics",
                "file_type": ".raw"
            },
            "paths": {
                "base_directory": str(tmp_path),
                "data_directory": str(tmp_path / "data")
            },
            "configurations": [{"name": "test_config"}],
            "metadata": {
                "instrument_used": "Test Instrument"
            },
            "skip_triggers": {
                "metadata_mapping_generated": False
            }
        }
        
        config_file = tmp_path / "test_config.json"
        with open(config_file, "w") as f:
            json.dump(config, f)
        
        manager = NMDCWorkflowManager(str(config_file))
        
        # Test 1: Missing biosample mapping file
        result = manager.generate_workflow_metadata_generation_inputs()
        assert result is False, "Should fail with missing biosample mapping"
        
        # Test 2: Create biosample mapping but no raw inspection results
        metadata_dir = manager.workflow_path / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        
        mapping_df = pd.DataFrame({
            "raw_file_name": ["test1.raw", "test2.raw"],
            "biosample_id": ["nmdc:bsm-11-001", "nmdc:bsm-11-002"],
            "match_confidence": ["high", "high"]
        })
        mapping_df.to_csv(metadata_dir / "mapped_raw_file_biosample_mapping.csv", index=False)
        
        result = manager.generate_workflow_metadata_generation_inputs()
        assert result is False, "Should fail with missing raw inspection results"
        
        # Test 3: Create raw inspection results but with no high-confidence matches
        raw_info_dir = manager.workflow_path / "raw_file_info"
        raw_info_dir.mkdir(parents=True, exist_ok=True)
        
        inspection_df = pd.DataFrame({
            "file_name": ["test1.raw", "test2.raw"],
            "write_time": ["2025-01-01T10:00:00", "2025-01-01T11:00:00"],
            "instrument_serial_number": ["SN001", "SN002"]
        })
        inspection_df.to_csv(raw_info_dir / "raw_file_inspection_results.csv", index=False)
        
        # Update mapping to have no high-confidence matches
        mapping_df = pd.DataFrame({
            "raw_file_name": ["test1.raw", "test2.raw"],
            "biosample_id": ["", ""],
            "match_confidence": ["low", "low"]
        })
        mapping_df.to_csv(metadata_dir / "mapped_raw_file_biosample_mapping.csv", index=False)
        
        result = manager.generate_workflow_metadata_generation_inputs()
        assert result is False, "Should fail with no high-confidence matches"

    @pytest.mark.integration
    def test_massive_url_generation_and_serial_filtering(self, tmp_path, integration_lcms_config, monkeypatch):
        """
        Integration test: Test MASSIVE URL generation and serial filtering CSV outputs.
        
        This test verifies the CSV metadata generation with MASSIVE:
        - MASSIVE URL construction from FTP locations
        - Serial number filtering (removal of specific instrument IDs)
        - FTP location mapping and path parsing
        - URL validation is called
        - Metadata overrides with collision energy patterns
        
        Note: NMDC JSON package generation requires actual raw data files for file size
        calculations, which are not available in test environment. This test focuses on
        CSV generation which is the primary workflow manager responsibility.
        """
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Modify the LCMS config for MASSIVE
        config = integration_lcms_config.copy()
        config["workflow"]["massive_id"] = "v07/MSV000094090"
        config["workflow"]["name"] = "test_massive_workflow"
        config["metadata"]["raw_data_location"] = "massive"
        config["metadata"]["serial_numbers_to_remove"] = ["Unknown", "Exactive Series slot #1"]
        
        # Update configurations to match test data (HILICZ POS only)
        config["configurations"] = [
            {
                "name": "hilic_pos",
                "file_filter": ["HILICZ", "_POS_"],
                "chromat_configuration_name": "JGI/LBNL Metabolomics Standard LC Method - Polar HILIC-Z",
                "mass_spec_configuration_name": "JGI/LBNL Standard Metabolomics Method, positive",
                "metadata_overrides": {
                    "mass_spec_configuration_name": {
                        "CE102040": "JGI/LBNL Standard Metabolomics Method, positive @10,20,40CE",
                        "CE205060": "JGI/LBNL Standard Metabolomics Method, positive @20,50,60CE"
                    }
                }
            }
        ]
        
        # Create workflow directory structure
        workflow_dir = tmp_path / "studies" / config["workflow"]["name"]
        metadata_dir = workflow_dir / "metadata"
        raw_info_dir = workflow_dir / "raw_file_info"
        nmdc_packages_dir = metadata_dir / "nmdc_submission_packages"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        raw_info_dir.mkdir(parents=True, exist_ok=True)
        nmdc_packages_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy test data files
        test_data_dir = Path(__file__).parent / "test_data"
        shutil.copy(
            test_data_dir / "metadata" / "massive_biosample_mapping.csv",
            metadata_dir / "mapped_raw_file_biosample_mapping.csv"
        )
        shutil.copy(
            test_data_dir / "raw_file_info" / "massive_inspection_results.csv",
            raw_info_dir / "raw_file_inspection_results.csv"
        )
        shutil.copy(
            test_data_dir / "raw_file_info" / "massive_ftp_test_locs.csv",
            raw_info_dir / "massive_ftp_locs.csv"
        )
        
        # Create mock material processing workflowreference CSV
        # This simulates the output from generate_material_processing_metadata()
        biosample_mapping = pd.read_csv(metadata_dir / "mapped_raw_file_biosample_mapping.csv")
        workflowref_data = []
        for _, row in biosample_mapping.iterrows():
            if row["match_confidence"] == "high":
                workflowref_data.append({
                    "biosample_id": row["biosample_id"],
                    "raw_data_identifier": row["raw_file_name"],
                    "last_processed_sample": f"nmdc:procsm-99-test{len(workflowref_data):03d}"
                })
        workflowref_df = pd.DataFrame(workflowref_data)
        workflowref_df.to_csv(
            nmdc_packages_dir / "material_processing_metadata_workflowreference.csv",
            index=False
        )
        
        # Create mapped_raw_files_wprocessed_MANUAL.csv (simulates manual creation in real workflow)
        # Merge biosample mapping with workflowref data to include processed sample IDs
        manual_mapping = biosample_mapping[biosample_mapping["match_confidence"] == "high"].copy()
        manual_mapping = manual_mapping.merge(
            workflowref_df[["raw_data_identifier", "last_processed_sample"]],
            left_on="raw_file_name",
            right_on="raw_data_identifier",
            how="left"
        )
        # Keep raw_data_identifier as the filename (drop the duplicate from merge)
        manual_mapping["raw_data_identifier"] = manual_mapping["raw_file_name"]
        manual_mapping.to_csv(
            metadata_dir / "mapped_raw_files_wprocessed_MANUAL.csv",
            index=False
        )
        
        # Save config
        config_file = workflow_dir / "test_config.json"
        with open(config_file, "w") as f:
            json.dump(config, f, indent=4)
        
        # Mock the MASSIVE URL validation to avoid network calls
        def mock_validate_massive_urls(self, urls):
            # Just verify the URL format without making network calls
            for url in urls:
                assert url.startswith("https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f."), \
                    f"Invalid MASSIVE URL format: {url}"
                assert "MSV000094090" in url, f"MASSIVE ID not in URL: {url}"
        
        monkeypatch.setattr(
            "nmdc_dp_utils.workflow_manager_mixins.WorkflowMetadataManager._validate_massive_urls",
            mock_validate_massive_urls
        )
        
        # Create manager
        manager = NMDCWorkflowManager(str(config_file))
        
        # Generate CSV metadata mappings (now includes processed_sample_id mapping)
        result = manager.generate_workflow_metadata_generation_inputs()
        
        # Verify generation succeeded
        assert result is True, "MASSIVE CSV metadata generation failed"
        
        # Verify skip trigger was set
        assert manager.should_skip("metadata_mapping_generated") is True
        
        # === Verify CSV files with MASSIVE URLs ===
        csv_output_dir = manager.workflow_path / "metadata" / "metadata_gen_input_csvs"
        assert csv_output_dir.exists(), "CSV output directory not created"
        
        hilic_pos_file = csv_output_dir / "hilic_pos_metadata.csv"
        assert hilic_pos_file.exists(), "HILIC POS CSV metadata file not created"
        
        df = pd.read_csv(hilic_pos_file)
        
        # Check that raw_data_url column exists for MASSIVE
        assert "raw_data_url" in df.columns, "raw_data_url column missing for MASSIVE"
        
        # Verify all MASSIVE URLs are properly formatted
        for url in df["raw_data_url"]:
            assert url.startswith("https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f."), \
                f"Invalid MASSIVE URL: {url}"
            assert "MSV000094090" in url, f"MASSIVE ID not in URL: {url}"
            assert "%2F" in url or "raw" in url, "URL should contain encoded path"
        
        # Verify serial number filtering was applied
        serial_numbers = df["instrument_instance_specifier"].tolist()
        valid_serials = [s for s in serial_numbers if pd.notna(s) and s != ""]
        
        # Should have exactly 1 file with valid serial (LCMS-12345)
        assert len(valid_serials) == 1, f"Expected 1 file with valid serial after filtering, got {len(valid_serials)}"
        assert valid_serials[0] == "LCMS-12345", f"Expected LCMS-12345, got {valid_serials[0]}"
        
        # Verify metadata overrides were applied based on CE patterns
        mass_specs = df["mass_spec_configuration_name"].unique()
        expected_overrides = [
            "JGI/LBNL Standard Metabolomics Method, positive @10,20,40CE",
            "JGI/LBNL Standard Metabolomics Method, positive @20,50,60CE"
        ]
        for ms in mass_specs:
            assert ms in expected_overrides, f"Unexpected mass spec config: {ms}"

    @pytest.mark.integration
    def test_material_processing_metadata_generation(self, tmp_path, integration_lcms_config):
        """
        Integration test: Test material processing metadata generation.
        
        This test verifies:
        - Material processing metadata generation with real-like inputs
        - YAML outline parsing and processing
        - Test/production mode behavior
        - Validation workflow
        """
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Create workflow directory structure
        workflow_dir = tmp_path / "studies" / integration_lcms_config["workflow"]["name"]
        protocol_dir = workflow_dir / "protocol_info"
        metadata_dir = workflow_dir / "metadata"
        nmdc_packages_dir = metadata_dir / "nmdc_submission_packages"
        protocol_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)
        nmdc_packages_dir.mkdir(parents=True, exist_ok=True)
        
        # Create valid YAML outline (based on Kroeger study structure but simplified for testing)
        yaml_path = protocol_dir / "llm_generated_protocol_outline.yaml"
        yaml_content = """test_metabolites:
  steps:
    - Step 1_test_metabolites:
        SubSamplingProcess:
          id: # nmdc:subspr-{shoulder}-{blade}
          type: nmdc:SubSamplingProcess
          name: Subsample for metabolite extraction
          description: A 2 g portion taken from <Biosample> for metabolite extraction.
          has_input:
            - Biosample
          has_output:
            - ProcessedSample1_test_metabolites
          processing_institution: EMSL
          mass:
            type: nmdc:QuantityValue
            has_raw_value: 2 g
            has_unit: g
            has_numeric_value: 2
          protocol_link:
            name: Test Protocol
            url: https://doi.org/10.1093/test
            type: nmdc:Protocol

    - Step 2_test_metabolites:
        Extraction:
          id: # nmdc:extrp-{shoulder}-{blade}
          type: nmdc:Extraction
          name: Water extraction of metabolites
          description: Metabolites extracted from <ProcessedSample1_test_metabolites> using LC-MS water.
          has_input:
            - ProcessedSample1_test_metabolites
          has_output:
            - ProcessedSample2_test_metabolites
          processing_institution: EMSL
          extraction_targets:
            - metabolite
          substances_used:
            - type: nmdc:PortionOfSubstance
              known_as: water
              substance_role: solvent
              volume:
                type: nmdc:QuantityValue
                has_raw_value: 1.2 mL
                has_unit: mL
                has_numeric_value: 1.2
          protocol_link:
            name: Test Protocol
            url: https://doi.org/10.1093/test
            type: nmdc:Protocol

  processedsamples:
    - ProcessedSample1_test_metabolites:
        ProcessedSample:
          id: #nmdc:
          type: nmdc:ProcessedSample
          name: <Biosample>_subsample
          description: A 2 g subsample of <Biosample> prepared for metabolite extraction.

    - ProcessedSample2_test_metabolites:
        ProcessedSample:
          id: #nmdc:
          type: nmdc:ProcessedSample
          name: <Biosample>_extract
          description: Metabolite extract obtained from <ProcessedSample1_test_metabolites> after water extraction.
          sampled_portion: aqueous_layer
"""
        with open(yaml_path, "w") as f:
            f.write(yaml_content)
        
        # Create input CSV with biosample to raw file mapping and protocol ID
        input_csv_path = metadata_dir / "mapped_raw_files_wprocessed_MANUAL.csv"
        test_data_dir = Path(__file__).parent / "test_data"
        biosample_mapping = pd.read_csv(test_data_dir / "metadata" / "lcms_biosample_mapping.csv")
        
        # Create mapping with processed sample placeholder and protocol ID
        mapping_rows = []
        for idx, row in biosample_mapping.iterrows():
            if row["match_confidence"] == "high":
                mapping_rows.append({
                    "raw_data_identifier": row["raw_file_name"],
                    "biosample_id": row["biosample_id"],
                    "biosample_name": f"Test Sample {idx}",
                    "match_confidence": "high",
                    "processedsample_placeholder": "ProcessedSample2_test_metabolites",
                    "material_processing_protocol_id": "test_metabolites"
                })
        
        mapping_df = pd.DataFrame(mapping_rows)
        mapping_df.to_csv(input_csv_path, index=False)
        
        # Add study info to config
        integration_lcms_config["study"] = {
            "id": "nmdc:sty-11-integrationtest",
            "name": "test_material_processing_study"
        }
        
        # Set environment variables for minting API credentials (required for test mode)
        os.environ["CLIENT_ID"] = "test_client_id"
        os.environ["CLIENT_SECRET"] = "test_client_secret"
        
        # Save config
        config_file = workflow_dir / "test_config.json"
        with open(config_file, "w") as f:
            json.dump(integration_lcms_config, f, indent=4)
        
        # Create manager
        manager = NMDCWorkflowManager(str(config_file))
        
        # Test generation in test mode (actually runs the generator, no mocking)
        result = manager.generate_material_processing_metadata(test=True)
        
        assert result is True, "Material processing metadata generation should succeed"
        
        # Verify output files were created
        output_file = nmdc_packages_dir / "material_processing_metadata.json"
        assert output_file.exists(), "Material processing metadata JSON should be created"
        
        # Load and verify the generated metadata
        with open(output_file, "r") as f:
            metadata = json.load(f)
        
        # Verify expected structure
        assert "processed_sample_set" in metadata, "Metadata should contain processed_sample_set"
        assert "material_processing_set" in metadata, "Metadata should contain material_processing_set"
        
        # Verify processed samples were created
        assert len(metadata["processed_sample_set"]) > 0, "Should have generated processed samples"
        
        # Verify material processing records were created
        assert len(metadata["material_processing_set"]) > 0, "Should have generated material processing records"
        
        # Verify workflowreference CSV was created
        workflowref_file = nmdc_packages_dir / "material_processing_metadata_workflowreference.csv"
        assert workflowref_file.exists(), "Workflowreference CSV should be created"
        
        # Load and verify workflowreference CSV
        workflowref_df = pd.read_csv(workflowref_file)
        assert "biosample_id" in workflowref_df.columns, "Should have biosample_id column"
        assert "raw_data_identifier" in workflowref_df.columns, "Should have raw_data_identifier column"
        assert "last_processed_sample" in workflowref_df.columns, "Should have last_processed_sample column"
        
        # Verify processed sample IDs are in correct format
        processed_sample_ids = workflowref_df["last_processed_sample"].tolist()
        assert all("nmdc:procsm-" in pid for pid in processed_sample_ids), "All processed sample IDs should be valid NMDC IDs"
        assert all("-00-" in pid for pid in processed_sample_ids), "Test mode IDs should contain '-00-'"
        assert len(processed_sample_ids) > 0, "Should have generated processed sample IDs"
        
        # Verify skip trigger was set
        assert manager.should_skip("material_processing_metadata_generated") is True



        
        # Clean up environment variables
        del os.environ["CLIENT_ID"]
        del os.environ["CLIENT_SECRET"]