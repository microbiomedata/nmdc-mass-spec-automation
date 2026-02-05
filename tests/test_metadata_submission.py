"""
Unit tests for submit_metadata_packages functionality.

Tests the metadata submission workflow including:
- Production ID verification (-11- tag)
- JSON validation using nmdc_api_utilities
- JSON submission to dev/prod environments
- Material processing submission before workflow metadata
- Proper error handling
"""

import pytest
import json
import os
from unittest.mock import patch, MagicMock, call
from pathlib import Path


class TestMetadataSubmission:
    """Unit test suite for metadata submission functionality."""

    @pytest.fixture
    def sample_metadata_with_production_ids(self):
        """Sample metadata with production IDs (-11- tag)."""
        return {
            "material_processing_set": [
                {
                    "id": "nmdc:subspr-11-abc123",
                    "type": "nmdc:SubSamplingProcess",
                    "has_input": ["nmdc:bsm-11-xyz789"],
                    "has_output": ["nmdc:procsm-11-def456"]
                }
            ]
        }

    @pytest.fixture
    def sample_metadata_with_test_ids(self):
        """Sample metadata with test IDs (-13- tag)."""
        return {
            "material_processing_set": [
                {
                    "id": "nmdc:subspr-13-abc123",
                    "type": "nmdc:SubSamplingProcess",
                    "has_input": ["nmdc:bsm-13-xyz789"],
                    "has_output": ["nmdc:procsm-13-def456"]
                }
            ]
        }

    @pytest.fixture
    def metadata_packages_dir(self, tmp_path):
        """Create a temporary metadata packages directory with sample files."""
        packages_dir = tmp_path / "metadata" / "nmdc_submission_packages"
        packages_dir.mkdir(parents=True, exist_ok=True)
        return packages_dir

    def test_verify_production_ids_success(self, lcms_config_file, sample_metadata_with_production_ids):
        """Test that production IDs with -11- tag are verified successfully."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Should return True for production IDs
        result = manager._verify_production_ids(sample_metadata_with_production_ids)
        assert result is True

    def test_verify_production_ids_failure(self, lcms_config_file, sample_metadata_with_test_ids):
        """Test that test IDs with -13- tag are rejected."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Should return False for test IDs
        result = manager._verify_production_ids(sample_metadata_with_test_ids)
        assert result is False

    def test_verify_production_ids_nested_structures(self, lcms_config_file):
        """Test ID verification in nested structures."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Data with nested IDs
        nested_data = {
            "workflows": [
                {
                    "id": "nmdc:wfmb-11-aaa111",
                    "has_input": ["nmdc:dobj-11-bbb222"],
                    "has_output": ["nmdc:dobj-11-ccc333"]
                },
                {
                    "id": "nmdc:wfmb-11-ddd444",
                    "has_input": ["nmdc:dobj-11-eee555"]
                }
            ]
        }
        
        result = manager._verify_production_ids(nested_data)
        assert result is True
        
        # Mix of production and test IDs should fail
        mixed_data = {
            "workflows": [
                {
                    "id": "nmdc:wfmb-11-aaa111",
                    "has_input": ["nmdc:dobj-13-bbb222"]  # Test ID
                }
            ]
        }
        
        result = manager._verify_production_ids(mixed_data)
        assert result is False

    @patch.dict(os.environ, {"CLIENT_ID": "test_client", "CLIENT_SECRET": "test_secret"})
    @patch('nmdc_api_utilities.metadata.Metadata')
    @patch('nmdc_api_utilities.auth.NMDCAuth')
    def test_submit_metadata_packages_success(
        self, mock_auth_class, mock_metadata_class, lcms_config_file, 
        metadata_packages_dir, sample_metadata_with_production_ids
    ):
        """Test successful metadata submission to dev environment."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Create manager with temporary config
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Override workflow path to use our temp directory
        manager.workflow_path = metadata_packages_dir.parent.parent
        
        # Create sample metadata files
        material_file = metadata_packages_dir / "material_processing_metadata.json"
        with open(material_file, "w") as f:
            json.dump(sample_metadata_with_production_ids, f)
        
        workflow_file = metadata_packages_dir / "workflow_metadata_hilic_pos.json"
        with open(workflow_file, "w") as f:
            json.dump(sample_metadata_with_production_ids, f)
        
        # Mock the API client
        mock_auth_instance = MagicMock()
        mock_auth_class.return_value = mock_auth_instance
        
        mock_metadata_instance = MagicMock()
        mock_metadata_class.return_value = mock_metadata_instance
        mock_metadata_instance.validate_json.return_value = 200
        mock_metadata_instance.submit_json.return_value = 200
        
        # Mock time.sleep to avoid waiting
        with patch('time.sleep'):
            result = manager.submit_metadata_packages(environment="dev")
        
        assert result is True
        
        # Verify auth was created with correct credentials
        mock_auth_class.assert_called_once_with(
            client_id="test_client",
            client_secret="test_secret",
            env="dev"
        )
        
        # Verify metadata client was created
        mock_metadata_class.assert_called_once_with(env="dev", auth=mock_auth_instance)
        
        # Verify validate_json was called twice (material + workflow)
        assert mock_metadata_instance.validate_json.call_count == 2
        
        # Verify submit_json was called twice
        assert mock_metadata_instance.submit_json.call_count == 2

    @patch.dict(os.environ, {"CLIENT_ID": "test_client", "CLIENT_SECRET": "test_secret"})
    @patch('nmdc_api_utilities.metadata.Metadata')
    @patch('nmdc_api_utilities.auth.NMDCAuth')
    def test_submit_metadata_packages_prod_environment(
        self, mock_auth_class, mock_metadata_class, lcms_config_file, 
        metadata_packages_dir, sample_metadata_with_production_ids
    ):
        """Test metadata submission to prod environment."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        manager.workflow_path = metadata_packages_dir.parent.parent
        
        # Create sample metadata file
        material_file = metadata_packages_dir / "material_processing_metadata.json"
        with open(material_file, "w") as f:
            json.dump(sample_metadata_with_production_ids, f)
        
        mock_auth_instance = MagicMock()
        mock_auth_class.return_value = mock_auth_instance
        
        mock_metadata_instance = MagicMock()
        mock_metadata_class.return_value = mock_metadata_instance
        mock_metadata_instance.validate_json.return_value = 200
        mock_metadata_instance.submit_json.return_value = 200
        
        with patch('time.sleep'):
            result = manager.submit_metadata_packages(environment="prod")
        
        assert result is True
        
        # Verify prod environment was used
        mock_auth_class.assert_called_once_with(
            client_id="test_client",
            client_secret="test_secret",
            env="prod"
        )
        mock_metadata_class.assert_called_once_with(env="prod", auth=mock_auth_instance)

    def test_submit_metadata_packages_missing_credentials(self, lcms_config_file):
        """Test that submission fails when credentials are missing."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Clear environment variables
        with patch.dict(os.environ, {}, clear=True):
            manager = NMDCWorkflowManager(str(lcms_config_file))
            result = manager.submit_metadata_packages(environment="dev")
            
            assert result is False

    def test_submit_metadata_packages_missing_directory(self, lcms_config_file):
        """Test that submission fails when packages directory doesn't exist."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        with patch.dict(os.environ, {"CLIENT_ID": "test", "CLIENT_SECRET": "test"}):
            manager = NMDCWorkflowManager(str(lcms_config_file))
            result = manager.submit_metadata_packages(environment="dev")
            
            assert result is False

    @patch.dict(os.environ, {"CLIENT_ID": "test_client", "CLIENT_SECRET": "test_secret"})
    def test_submit_metadata_packages_no_json_files(
        self, lcms_config_file, metadata_packages_dir
    ):
        """Test that submission fails when no JSON files are found."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        manager.workflow_path = metadata_packages_dir.parent.parent
        
        result = manager.submit_metadata_packages(environment="dev")
        
        assert result is False

    @patch.dict(os.environ, {"CLIENT_ID": "test_client", "CLIENT_SECRET": "test_secret"})
    def test_submit_metadata_packages_test_ids_rejected(
        self, lcms_config_file, metadata_packages_dir, sample_metadata_with_test_ids
    ):
        """Test that submission fails when test IDs are present."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        manager.workflow_path = metadata_packages_dir.parent.parent
        
        # Create metadata file with test IDs
        material_file = metadata_packages_dir / "material_processing_metadata.json"
        with open(material_file, "w") as f:
            json.dump(sample_metadata_with_test_ids, f)
        
        result = manager.submit_metadata_packages(environment="dev")
        
        # Should fail due to test IDs
        assert result is False

    @patch.dict(os.environ, {"CLIENT_ID": "test_client", "CLIENT_SECRET": "test_secret"})
    @patch('nmdc_api_utilities.metadata.Metadata')
    @patch('nmdc_api_utilities.auth.NMDCAuth')
    def test_submit_metadata_packages_validation_failure(
        self, mock_auth_class, mock_metadata_class, lcms_config_file, 
        metadata_packages_dir, sample_metadata_with_production_ids
    ):
        """Test that submission fails when validation fails."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        manager.workflow_path = metadata_packages_dir.parent.parent
        
        # Create sample metadata file
        material_file = metadata_packages_dir / "material_processing_metadata.json"
        with open(material_file, "w") as f:
            json.dump(sample_metadata_with_production_ids, f)
        
        mock_auth_instance = MagicMock()
        mock_auth_class.return_value = mock_auth_instance
        
        mock_metadata_instance = MagicMock()
        mock_metadata_class.return_value = mock_metadata_instance
        
        # Make validation fail
        mock_metadata_instance.validate_json.side_effect = Exception("Validation failed")
        
        result = manager.submit_metadata_packages(environment="dev")
        
        # Should fail due to validation error
        assert result is False

    @patch.dict(os.environ, {"CLIENT_ID": "test_client", "CLIENT_SECRET": "test_secret"})
    @patch('nmdc_api_utilities.metadata.Metadata')
    @patch('nmdc_api_utilities.auth.NMDCAuth')
    def test_submit_metadata_packages_submission_failure(
        self, mock_auth_class, mock_metadata_class, lcms_config_file, 
        metadata_packages_dir, sample_metadata_with_production_ids
    ):
        """Test that submission fails when API submission fails."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        manager.workflow_path = metadata_packages_dir.parent.parent
        
        # Create sample metadata file
        material_file = metadata_packages_dir / "material_processing_metadata.json"
        with open(material_file, "w") as f:
            json.dump(sample_metadata_with_production_ids, f)
        
        mock_auth_instance = MagicMock()
        mock_auth_class.return_value = mock_auth_instance
        
        mock_metadata_instance = MagicMock()
        mock_metadata_class.return_value = mock_metadata_instance
        mock_metadata_instance.validate_json.return_value = 200
        
        # Make submission fail
        mock_metadata_instance.submit_json.side_effect = Exception("Submission failed")
        
        result = manager.submit_metadata_packages(environment="dev")
        
        # Should fail due to submission error
        assert result is False

    @patch.dict(os.environ, {"CLIENT_ID": "test_client", "CLIENT_SECRET": "test_secret"})
    @patch('nmdc_api_utilities.metadata.Metadata')
    @patch('nmdc_api_utilities.auth.NMDCAuth')
    @patch('time.sleep')
    def test_submit_metadata_packages_waits_after_material_processing(
        self, mock_sleep, mock_auth_class, mock_metadata_class, lcms_config_file, 
        metadata_packages_dir, sample_metadata_with_production_ids
    ):
        """Test that submission waits 1 minute after material processing."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        manager.workflow_path = metadata_packages_dir.parent.parent
        
        # Create both material processing and workflow metadata files
        material_file = metadata_packages_dir / "material_processing_metadata.json"
        with open(material_file, "w") as f:
            json.dump(sample_metadata_with_production_ids, f)
        
        workflow_file = metadata_packages_dir / "workflow_metadata_hilic_pos.json"
        with open(workflow_file, "w") as f:
            json.dump(sample_metadata_with_production_ids, f)
        
        mock_auth_instance = MagicMock()
        mock_auth_class.return_value = mock_auth_instance
        
        mock_metadata_instance = MagicMock()
        mock_metadata_class.return_value = mock_metadata_instance
        mock_metadata_instance.validate_json.return_value = 200
        mock_metadata_instance.submit_json.return_value = 200
        
        result = manager.submit_metadata_packages(environment="dev")
        
        assert result is True
        
        # Verify sleep was called with 60 seconds
        mock_sleep.assert_called_once_with(60)

    @patch.dict(os.environ, {"CLIENT_ID": "test_client", "CLIENT_SECRET": "test_secret"})
    @patch('nmdc_api_utilities.metadata.Metadata')
    @patch('nmdc_api_utilities.auth.NMDCAuth')
    def test_submit_metadata_packages_to_dev_wrapper(
        self, mock_auth_class, mock_metadata_class, lcms_config_file, 
        metadata_packages_dir, sample_metadata_with_production_ids
    ):
        """Test the submit_metadata_packages_to_dev wrapper function."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        manager.workflow_path = metadata_packages_dir.parent.parent
        
        # Create sample metadata file
        material_file = metadata_packages_dir / "material_processing_metadata.json"
        with open(material_file, "w") as f:
            json.dump(sample_metadata_with_production_ids, f)
        
        mock_auth_instance = MagicMock()
        mock_auth_class.return_value = mock_auth_instance
        
        mock_metadata_instance = MagicMock()
        mock_metadata_class.return_value = mock_metadata_instance
        mock_metadata_instance.validate_json.return_value = 200
        mock_metadata_instance.submit_json.return_value = 200
        
        with patch('time.sleep'):
            result = manager.submit_metadata_packages_to_dev()
        
        assert result is True
        
        # Verify skip trigger was set
        assert manager.should_skip("metadata_submitted_dev") is True

    @patch.dict(os.environ, {"CLIENT_ID": "test_client", "CLIENT_SECRET": "test_secret"})
    @patch('nmdc_api_utilities.metadata.Metadata')
    @patch('nmdc_api_utilities.auth.NMDCAuth')
    def test_submit_metadata_packages_to_prod_wrapper(
        self, mock_auth_class, mock_metadata_class, lcms_config_file, 
        metadata_packages_dir, sample_metadata_with_production_ids
    ):
        """Test the submit_metadata_packages_to_prod wrapper function."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        manager.workflow_path = metadata_packages_dir.parent.parent
        
        # Set dev skip trigger to indicate dev submission was successful
        manager.set_skip_trigger("metadata_submitted_dev", True)
        
        # Create sample metadata file
        material_file = metadata_packages_dir / "material_processing_metadata.json"
        with open(material_file, "w") as f:
            json.dump(sample_metadata_with_production_ids, f)
        
        mock_auth_instance = MagicMock()
        mock_auth_class.return_value = mock_auth_instance
        
        mock_metadata_instance = MagicMock()
        mock_metadata_class.return_value = mock_metadata_instance
        mock_metadata_instance.validate_json.return_value = 200
        mock_metadata_instance.submit_json.return_value = 200
        
        with patch('time.sleep'):
            result = manager.submit_metadata_packages_to_prod()
        
        assert result is True
        
        # Verify skip trigger was set
        assert manager.should_skip("metadata_submitted_prod") is True

    def test_submit_metadata_packages_to_prod_requires_dev_success(self, lcms_config_file):
        """Test that submit_metadata_packages_to_prod requires dev submission to be successful first."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Do NOT set dev skip trigger - simulating dev submission not completed
        
        # Should fail because dev was not successful
        result = manager.submit_metadata_packages_to_prod()
        
        assert result is False

    def test_submit_metadata_packages_to_dev_skip_if_complete(self, lcms_config_file):
        """Test that submit_metadata_packages_to_dev respects skip trigger."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Set skip trigger
        manager.set_skip_trigger("metadata_submitted_dev", True)
        
        # Should skip and return True immediately
        result = manager.submit_metadata_packages_to_dev()
        
        assert result is True
        assert manager.should_skip("metadata_submitted_dev") is True

    def test_submit_metadata_packages_to_prod_skip_if_complete(self, lcms_config_file):
        """Test that submit_metadata_packages_to_prod respects skip trigger."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Set both dev and prod skip triggers
        manager.set_skip_trigger("metadata_submitted_dev", True)
        manager.set_skip_trigger("metadata_submitted_prod", True)
        
        # Should skip and return True immediately (without checking dev)
        result = manager.submit_metadata_packages_to_prod()
        
        assert result is True
        assert manager.should_skip("metadata_submitted_prod") is True
