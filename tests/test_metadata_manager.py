"""
Unit tests for WorkflowMetadataManager mixin.

Tests the metadata generation workflow including:
- Configuration-based file separation
- LCMS/GCMS-specific metadata processing
- Calibration file assignment for GCMS
- MASSIVE URL generation and validation
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import json


class TestWorkflowMetadataManager:
    """Unit test suite for WorkflowMetadataManager mixin."""

    def test_separate_files_by_configuration(self, lcms_config_file):
        """Test that files are correctly separated by configuration filters."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create test dataframe
        test_df = pd.DataFrame({
            'raw_data_file_short': [
                'sample_HILIC_POS_01.raw',
                'sample_HILIC_NEG_02.raw',
                'sample_RP_POS_03.raw'
            ],
            'biosample_id': ['nmdc:bsm-1', 'nmdc:bsm-2', 'nmdc:bsm-3']
        })
        
        # Test configuration separation
        metadata_config = manager.config.get("metadata", {})
        config_dfs = manager._separate_files_by_configuration(test_df, metadata_config)
        
        # Should have one config: hilic_pos
        assert 'hilic_pos' in config_dfs
        
        # Only one file should match (HILIC and POS)
        assert len(config_dfs['hilic_pos']) == 1
        assert 'HILIC_POS' in config_dfs['hilic_pos'].iloc[0]['raw_data_file_short']
        
        # Check metadata fields are applied
        assert config_dfs['hilic_pos'].iloc[0]['mass_spec_configuration_name'] == 'QE_HILIC_POS'

    def test_separate_files_by_configuration_fallback(self, lcms_config_file):
        """Test fallback when no configurations match."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create test dataframe with files that don't match any filter
        test_df = pd.DataFrame({
            'raw_data_file_short': [
                'sample_OTHER_01.raw',
                'sample_OTHER_02.raw'
            ],
            'biosample_id': ['nmdc:bsm-1', 'nmdc:bsm-2']
        })
        
        metadata_config = manager.config.get("metadata", {})
        config_dfs = manager._separate_files_by_configuration(test_df, metadata_config)
        
        # Should create fallback configuration
        assert 'all_data' in config_dfs
        assert len(config_dfs['all_data']) == 2

    def test_generate_metadata_missing_prerequisites(self, lcms_config_file):
        """Test that metadata generation fails gracefully when prerequisites are missing."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Should fail because biosample mapping doesn't exist
        result = manager.generate_workflow_metadata_generation_inputs()
        
        assert result is False

    def test_lcms_metadata_generator_exists(self, lcms_config_file):
        """Test that the LCMS metadata generator method exists."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Verify the method exists and is callable
        assert hasattr(manager, '_generate_lcms_workflow_metadata_inputs')
        assert callable(manager._generate_lcms_workflow_metadata_inputs)
        
        # Verify GCMS method exists too
        assert hasattr(manager, '_generate_gcms_workflow_metadata_inputs')
        assert callable(manager._generate_gcms_workflow_metadata_inputs)

    def test_validate_massive_urls_success(self, lcms_config_file):
        """Test MASSIVE URL validation with successful HEAD request."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        test_urls = [
            "https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f.MSV000094090%2Fraw%2Ftest.raw&forceDownload=true"
        ]
        
        # Mock urllib.request.urlopen
        with patch('urllib.request.urlopen') as mock_urlopen:
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.headers = {'Content-Length': '1000000'}
            mock_urlopen.return_value = mock_response
            
            result = manager._validate_massive_urls(test_urls)
            
            assert result is True
            assert mock_urlopen.called

    def test_validate_massive_urls_failure(self, lcms_config_file):
        """Test MASSIVE URL validation raises ValueError when all URLs fail."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        import urllib.error
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        test_urls = ["https://massive.ucsd.edu/invalid"]
        
        # Mock urllib to raise HTTPError
        with patch('urllib.request.urlopen') as mock_urlopen:
            mock_urlopen.side_effect = urllib.error.HTTPError(
                test_urls[0], 404, "Not Found", {}, None
            )
            
            # Should raise ValueError when all URLs fail
            with pytest.raises(ValueError, match="None of the .* tested MASSIVE URLs are accessible"):
                manager._validate_massive_urls(test_urls)

    def test_assign_calibration_files_chronological(self, gcms_config_file, tmp_path):
        """Test GCMS calibration file assignment based on chronological order."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(gcms_config_file))
        
        # Create biosample mapping file with calibrations
        metadata_dir = manager.workflow_path / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        
        mapping_df = pd.DataFrame({
            'raw_file_name': [
                'calibration_01.cdf',
                'sample_01.cdf',
                'sample_02.cdf',
                'calibration_02.cdf',
                'sample_03.cdf'
            ],
            'raw_file_type': [
                'calibration',
                'sample',
                'sample',
                'calibration',
                'sample'
            ],
            'biosample_id': [
                None,
                'nmdc:bsm-1',
                'nmdc:bsm-2',
                None,
                'nmdc:bsm-3'
            ],
            'match_confidence': [
                None,
                'high',
                'high',
                None,
                'high'
            ]
        })
        
        mapping_file = metadata_dir / "mapped_raw_file_biosample_mapping.csv"
        mapping_df.to_csv(mapping_file, index=False)
        
        # Create inspection results with timestamps
        raw_file_info = manager.workflow_path / "raw_file_info"
        raw_file_info.mkdir(parents=True, exist_ok=True)
        
        inspection_df = pd.DataFrame({
            'file_name': [
                'calibration_01.cdf',
                'sample_01.cdf',
                'sample_02.cdf',
                'calibration_02.cdf',
                'sample_03.cdf'
            ],
            'write_time': [
                '2024-01-01 08:00:00',
                '2024-01-01 10:00:00',
                '2024-01-01 12:00:00',
                '2024-01-01 14:00:00',
                '2024-01-01 16:00:00'
            ],
            'total_scans': [1000, 1000, 1000, 1000, 1000]
        })
        
        inspection_file = raw_file_info / "raw_file_inspection_results.csv"
        inspection_df.to_csv(inspection_file, index=False)
        
        # Test dataframe with samples (write_time column needed)
        sample_df = pd.DataFrame({
            'raw_data_file_short': ['sample_01.cdf', 'sample_02.cdf', 'sample_03.cdf'],
            'biosample_id': ['nmdc:bsm-1', 'nmdc:bsm-2', 'nmdc:bsm-3'],
            'write_time': [
                '2024-01-01 10:00:00',
                '2024-01-01 12:00:00',
                '2024-01-01 16:00:00'
            ]
        })
        
        # Call calibration assignment
        result_df = manager._assign_calibration_files_to_samples(
            sample_df, str(inspection_file)
        )
        
        # Verify calibration assignments
        assert 'calibration_file' in result_df.columns
        
        # sample_01 and sample_02 should use calibration_01 (run before calibration_02)
        # sample_03 should use calibration_02
        cal_files = result_df['calibration_file'].tolist()
        
        assert 'calibration_01.cdf' in cal_files[0]  # sample_01 uses cal_01
        assert 'calibration_01.cdf' in cal_files[1]  # sample_02 uses cal_01
        assert 'calibration_02.cdf' in cal_files[2]  # sample_03 uses cal_02

    def test_metadata_generation_skip_trigger(self, lcms_config_file):
        """Test that metadata generation respects skip trigger."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Set skip trigger
        manager.set_skip_trigger("metadata_mapping_generated", True)
        
        # Should skip and return True immediately
        result = manager.generate_workflow_metadata_generation_inputs()
        
        assert result is True
        assert manager.should_skip("metadata_mapping_generated") is True

    def test_metadata_generation_unsupported_workflow(self, tmp_path):
        """Test that unsupported workflow types raise appropriate error."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        config = {
            "workflow": {
                "name": "test_unsupported",
                "type": "unsupported_type",
                "workflow_type": "Unsupported Workflow"
            },
            "study": {"id": "test", "name": "test"},
            "paths": {
                "base_directory": str(tmp_path / "base"),
                "data_directory": str(tmp_path / "data")
            }
        }
        
        config_path = tmp_path / "config.json"
        with open(config_path, 'w') as f:
            json.dump(config, f)
        
        manager = NMDCWorkflowManager(str(config_path))
        
        # Should return False for unsupported workflow
        result = manager.generate_workflow_metadata_generation_inputs()
        
        assert result is False

    def test_configuration_file_filtering(self, lcms_config_file):
        """Test configuration file filtering with multiple patterns."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Use existing config which has both hilic_pos and rp_neg configurations
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        test_df = pd.DataFrame({
            'raw_data_file_short': [
                'sample_HILIC_POS_01.raw',
                'sample_HILIC_NEG_02.raw',
                'sample_RP_NEG_03.raw',
                'sample_RP_POS_04.raw'
            ],
            'biosample_id': ['nmdc:bsm-1', 'nmdc:bsm-2', 'nmdc:bsm-3', 'nmdc:bsm-4']
        })
        
        metadata_config = manager.config.get("metadata", {})
        config_dfs = manager._separate_files_by_configuration(test_df, metadata_config)
        
        # Should have two configs that match
        assert 'hilic_pos' in config_dfs
        assert 'rp_neg' in config_dfs
        
        # hilic_pos should only have HILIC_POS file
        assert len(config_dfs['hilic_pos']) == 1
        assert 'HILIC_POS' in config_dfs['hilic_pos'].iloc[0]['raw_data_file_short']
        
        # rp_neg should only have RP_NEG file
        assert len(config_dfs['rp_neg']) == 1
        assert 'RP_NEG' in config_dfs['rp_neg'].iloc[0]['raw_data_file_short']
        
        # Check configuration-specific metadata is applied
        assert config_dfs['hilic_pos'].iloc[0]['mass_spec_configuration_name'] == 'QE_HILIC_POS'
        assert config_dfs['rp_neg'].iloc[0]['mass_spec_configuration_name'] == 'QE_RP_NEG'
