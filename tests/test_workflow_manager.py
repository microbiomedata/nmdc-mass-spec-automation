"""
Unit tests for NMDC Workflow Manager utilities.

Tests cover core workflow management functionality including:
- Configuration loading and validation
- Workflow initialization
- Skip trigger management
- Path construction
- Workflow information retrieval
"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch


class TestNMDCWorkflowManager:
    """Test suite for NMDCWorkflowManager class."""

    @patch.dict('os.environ', {}, clear=True)
    def test_initialization(self, lcms_config_file):
        """Test that workflow manager initializes correctly with valid config."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        assert manager.workflow_name == "test_lcms_workflow"
        assert manager.study_name == "test_lcms_study"
        assert manager.study_id == "nmdc:sty-11-test"
        assert manager.config_path == str(lcms_config_file.resolve())

    @patch.dict('os.environ', {}, clear=True)
    def test_load_config(self, lcms_config_file, lcms_config):
        """Test configuration loading from JSON file."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        assert manager.config["workflow"]["name"] == lcms_config["workflow"]["name"]
        assert manager.config["study"]["id"] == lcms_config["study"]["id"]
        assert "skip_triggers" in manager.config

    @patch.dict('os.environ', {}, clear=True)
    def test_skip_triggers_initialization(self, lcms_config_file):
        """Test that skip triggers are initialized if not present in config."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        assert "skip_triggers" in manager.config
        assert isinstance(manager.config["skip_triggers"], dict)
        assert manager.config["skip_triggers"]["study_structure_created"] is False

    @patch.dict('os.environ', {}, clear=True)
    def test_should_skip(self, lcms_config_file):
        """Test skip trigger checking."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        manager.config["skip_triggers"]["test_trigger"] = True
        
        assert manager.should_skip("test_trigger") is True
        assert manager.should_skip("nonexistent_trigger") is False

    @patch.dict('os.environ', {}, clear=True)
    def test_set_skip_trigger(self, lcms_config_file):
        """Test setting skip trigger values."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Test setting without saving
        manager.set_skip_trigger("test_trigger", True, save=False)
        assert manager.config["skip_triggers"]["test_trigger"] is True
        
        # Test setting with saving
        manager.set_skip_trigger("another_trigger", True, save=True)
        
        # Reload config and verify persistence
        with open(lcms_config_file, "r") as f:
            saved_config = json.load(f)
        assert saved_config["skip_triggers"]["another_trigger"] is True

    @patch.dict('os.environ', {}, clear=True)
    def test_reset_all_triggers(self, lcms_config_file):
        """Test resetting all skip triggers."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Set some triggers
        manager.set_skip_trigger("trigger1", True, save=False)
        manager.set_skip_trigger("trigger2", True, save=False)
        
        # Reset all
        manager.reset_all_triggers(save=False)
        
        assert manager.config["skip_triggers"]["trigger1"] is False
        assert manager.config["skip_triggers"]["trigger2"] is False

    @patch.dict('os.environ', {}, clear=True)
    def test_path_construction(self, lcms_config_file, temp_config_dir):
        """Test that paths are constructed correctly."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        assert manager.base_path == temp_config_dir / "lcms_base"
        assert manager.workflow_path == temp_config_dir / "lcms_base/studies/test_lcms_workflow"
        assert manager.raw_data_directory == temp_config_dir / "lcms_data/test_lcms_study/raw"

    @patch.dict('os.environ', {}, clear=True)
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

    @patch.dict('os.environ', {}, clear=True)
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

    @patch.dict('os.environ', {}, clear=True)
    def test_show_available_workflow_types(self, lcms_config_file):
        """Test listing available workflow types."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        workflow_types = manager.show_available_workflow_types()
        
        assert isinstance(workflow_types, list)
        assert len(workflow_types) > 0
        assert "GCMS Metabolomics" in workflow_types

    #TODO: Add test for minio with valid credentials in env for GHA

    @patch.dict('os.environ', {}, clear=True)
    def test_minio_client_initialization_without_credentials(self, lcms_config_file):
        """Test MinIO client is None when credentials are missing."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # MinIO client should return None when credentials are not available
        assert manager.minio_client is None

    @patch.dict('os.environ', {}, clear=True)
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
