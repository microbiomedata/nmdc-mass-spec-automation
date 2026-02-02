"""
Unit tests for NMDC Workflow Manager.

Tests initialization, config loading, skip triggers, paths, and workflow info.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch


class TestNMDCWorkflowManager:
    """Test suite for NMDCWorkflowManager class."""

    def test_initialization_and_config_loading(self, lcms_config_file, lcms_config):
        """Test workflow manager initialization and config loading."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Verify basic attributes
        assert manager.workflow_name == "test_lcms_workflow"
        assert manager.study_name == "test_lcms_study"
        assert manager.study_id == "nmdc:sty-11-test"
        assert manager.config_path == str(lcms_config_file.resolve())
        
        # Verify config loaded correctly
        assert manager.config["workflow"]["name"] == lcms_config["workflow"]["name"]
        assert manager.config["study"]["id"] == lcms_config["study"]["id"]
        assert "skip_triggers" in manager.config

    def test_skip_trigger_management(self, lcms_config_file):
        """Test skip trigger initialization, checking, setting, and resetting."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Verify initialization
        assert "skip_triggers" in manager.config
        assert isinstance(manager.config["skip_triggers"], dict)
        assert manager.config["skip_triggers"]["study_structure_created"] is False
        
        # Test checking
        manager.config["skip_triggers"]["test_trigger"] = True
        assert manager.should_skip("test_trigger") is True
        assert manager.should_skip("nonexistent_trigger") is False
        
        # Test setting without saving
        manager.set_skip_trigger("new_trigger", True, save=False)
        assert manager.config["skip_triggers"]["new_trigger"] is True
        
        # Test setting with saving
        manager.set_skip_trigger("saved_trigger", True, save=True)
        with open(lcms_config_file, "r") as f:
            saved_config = json.load(f)
        assert saved_config["skip_triggers"]["saved_trigger"] is True
        
        # Test reset
        manager.set_skip_trigger("trigger1", True, save=False)
        manager.set_skip_trigger("trigger2", True, save=False)
        manager.reset_all_triggers(save=False)
        assert manager.config["skip_triggers"]["trigger1"] is False
        assert manager.config["skip_triggers"]["trigger2"] is False

    def test_path_construction(self, lcms_config_file, temp_config_dir):
        """Test path construction."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        assert manager.base_path == temp_config_dir / "lcms_base"
        assert manager.workflow_path == temp_config_dir / "lcms_base/studies/test_lcms_workflow"
        assert manager.raw_data_directory == temp_config_dir / "lcms_data/test_lcms_study/raw"

    def test_processed_data_directory_with_date_tag(self, temp_config_dir, lcms_config):
        """Test processed data directory path with date tag."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Add date tag to config
        lcms_config["workflow"]["processed_data_date_tag"] = "20250107"
        config_path = temp_config_dir / "test_config_with_date.json"
        with open(config_path, "w") as f:
            json.dump(lcms_config, f)
        
        manager = NMDCWorkflowManager(str(config_path))
        
        expected_path = temp_config_dir / "lcms_data/test_lcms_study/processed_20250107"
        assert manager.processed_data_directory == expected_path

    def test_get_workflow_info(self, lcms_config_file):
        """Test workflow info retrieval."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        info = manager.get_workflow_info()
        
        assert info["workflow_name"] == "test_lcms_workflow"
        assert info["study_name"] == "test_lcms_study"
        assert info["study_id"] == "nmdc:sty-11-test"
        assert info["massive_id"] == "MSV000094090"
        assert info["file_type"] == ".raw"
        assert "hilic_pos" in info["configuration_names"]
        assert info["minio_enabled"] is False  # No credentials in env

    def test_show_available_workflow_types(self, lcms_config_file):
        """Test listing available workflow types."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        workflow_types = manager.show_available_workflow_types()
        
        assert isinstance(workflow_types, list)
        assert len(workflow_types) > 0
        assert "GCMS Metabolomics" in workflow_types

    #TODO: Add test for minio with valid credentials in env for GHA

    def test_minio_client_initialization_without_credentials(self, lcms_config_file):
        """Test MinIO client is None when credentials are missing."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # MinIO client should return None when credentials are not available
        assert manager.minio_client is None

    def test_minio_client_lazy_loading(self, lcms_config_file):
        """Test that MinIO client is only initialized when first accessed."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Private attribute should be None before first access
        assert manager._minio_client is None
        
        # Access the property (will attempt to initialize)
        _ = manager.minio_client
        
        # Should no longer be None (even if it failed, it should be set)
        # In this case it will be None because no credentials, but the lazy-load happened
        assert manager._minio_client is None  # No credentials, so it's None
        
        # Second access should not reinitialize
        client1 = manager.minio_client
        client2 = manager.minio_client
        assert client1 is client2  # Same object

    def test_invalid_config_file(self):
        """Test that invalid config file raises appropriate error."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        with pytest.raises(FileNotFoundError):
            NMDCWorkflowManager("/nonexistent/config.json")

    def test_invalid_json_config(self, temp_config_dir):
        """Test that invalid JSON raises appropriate error."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        bad_config = temp_config_dir / "bad_config.json"
        with open(bad_config, "w") as f:
            f.write("{ invalid json }")
        
        with pytest.raises(json.JSONDecodeError):
            NMDCWorkflowManager(str(bad_config))

    def test_generate_material_processing_metadata(self, lcms_config_file, tmp_path):
        """Test material processing metadata generation method."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        from unittest.mock import MagicMock, patch
        
        # Create manager
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create required directory structure
        protocol_dir = manager.workflow_path / "protocol_info"
        protocol_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir = manager.workflow_path / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # Create mock YAML outline file
        yaml_path = protocol_dir / "llm_generated_protocol_outline.yaml"
        yaml_content = """
processing_steps:
  - step_name: Sample Collection
    has_input: biosample
    has_output: processed_sample
"""
        with open(yaml_path, "w") as f:
            f.write(yaml_content)
        
        # Create mock input CSV
        input_csv_path = metadata_dir / "mapped_raw_files_wprocessed_MANUAL.csv"
        input_csv_content = "biosample_id,raw_file_name,processed_sample_id\nnmdc:bsm-11-test,test_file.raw,nmdc:procsm-11-test\n"
        with open(input_csv_path, "w") as f:
            f.write(input_csv_content)
        
        # Add study_id to config
        manager.config["study"] = {"id": "nmdc:sty-11-test"}
        
        # Mock MaterialProcessingMetadataGenerator
        mock_generator_instance = MagicMock()
        mock_generator_instance.run.return_value = {"test": "metadata"}
        mock_generator_instance.validate_nmdc_database.return_value = {"result": "All Okay!"}
        
        mock_generator_class = MagicMock(return_value=mock_generator_instance)
        
        # Patch the MaterialProcessingMetadataGenerator import
        with patch("nmdc_dp_utils.workflow_manager_mixins.MaterialProcessingMetadataGenerator", mock_generator_class):
            # Test with test=True (should only run once in test mode)
            result = manager.generate_material_processing_metadata(test=True)
            
            assert result is True, "Material processing metadata generation should succeed"
            
            # Verify generator was called with test=True
            assert mock_generator_class.call_count == 1
            call_args = mock_generator_class.call_args
            assert call_args.kwargs["test"] is True
            assert call_args.kwargs["study_id"] == "nmdc:sty-11-test"
            assert "llm_generated_protocol_outline.yaml" in call_args.kwargs["yaml_outline_path"]
            assert "mapped_raw_files_wprocessed_MANUAL.csv" in call_args.kwargs["sample_to_dg_mapping_path"]
            
            # Verify skip trigger was set
            assert manager.should_skip("material_processing_metadata_generated") is True
            
            # Reset for test=False scenario
            manager.reset_all_triggers(save=False)
            mock_generator_class.reset_mock()
            
            # Test with test=False (should run twice: test mode then production mode)
            result = manager.generate_material_processing_metadata(test=False)
            
            assert result is True, "Material processing metadata generation should succeed"
            
            # Verify generator was called twice (test mode + production mode)
            assert mock_generator_class.call_count == 2
            
            # First call should be test=True
            first_call_args = mock_generator_class.call_args_list[0]
            assert first_call_args.kwargs["test"] is True
            
            # Second call should be test=False
            second_call_args = mock_generator_class.call_args_list[1]
            assert second_call_args.kwargs["test"] is False
            
            # Verify skip trigger was set
            assert manager.should_skip("material_processing_metadata_generated") is True

    def test_generate_material_processing_metadata_missing_yaml(self, lcms_config_file):
        """Test that missing YAML outline returns False."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Don't create the YAML file
        result = manager.generate_material_processing_metadata(test=True)
        
        assert result is False, "Should return False when YAML outline is missing"
        assert manager.should_skip("material_processing_metadata_generated") is False

    def test_generate_material_processing_metadata_missing_input_csv(self, lcms_config_file, tmp_path):
        """Test that missing input CSV returns False."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create YAML but not input CSV
        protocol_dir = manager.workflow_path / "protocol_info"
        protocol_dir.mkdir(parents=True, exist_ok=True)
        yaml_path = protocol_dir / "llm_generated_protocol_outline.yaml"
        with open(yaml_path, "w") as f:
            f.write("test: yaml")
        
        result = manager.generate_material_processing_metadata(test=True)
        
        assert result is False, "Should return False when input CSV is missing"
        assert manager.should_skip("material_processing_metadata_generated") is False
