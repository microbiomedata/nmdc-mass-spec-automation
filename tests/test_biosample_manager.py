"""
Unit tests for NMDCWorkflowBiosampleManager mixin.

Tests cover biosample management functionality including:
- Fetching biosample attributes from NMDC API
- Generating biosample mapping scripts from templates
- Running biosample mapping scripts
- Filtering mapped files for processing
"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
import pandas as pd


class TestNMDCWorkflowBiosampleManager:
    """Test suite for NMDCWorkflowBiosampleManager mixin."""

    @patch.dict('os.environ', {}, clear=True)
    @patch('nmdc_api_utilities.biosample_search.BiosampleSearch')
    def test_get_biosample_attributes_success(self, mock_biosample_search_class, lcms_config_file, temp_config_dir):
        """Test successful fetching of biosample attributes."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Mock the BiosampleSearch instance and its methods
        mock_search_instance = MagicMock()
        mock_biosample_search_class.return_value = mock_search_instance
        
        # Mock biosample data
        mock_biosamples = [
            {
                "id": "nmdc:bsm-11-test001",
                "name": "Sample 1",
                "samp_name": "S1",
                "description": "Test sample 1"
            },
            {
                "id": "nmdc:bsm-11-test002",
                "name": "Sample 2",
                "samp_name": "S2",
                "description": "Test sample 2"
            }
        ]
        mock_search_instance.get_record_by_filter.return_value = mock_biosamples
        
        # Create manager
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Call the method
        result = manager.get_biosample_attributes()
        
        # Verify success
        assert result is True
        assert manager.should_skip("biosample_attributes_fetched") is True
        
        # Verify the API was called correctly
        mock_search_instance.get_record_by_filter.assert_called_once()
        call_kwargs = mock_search_instance.get_record_by_filter.call_args[1]
        assert "nmdc:sty-11-test" in call_kwargs['filter']
        
        # Verify CSV was created
        csv_path = manager.workflow_path / "metadata" / "biosample_attributes.csv"
        assert csv_path.exists()
        
        # Verify CSV contents
        df = pd.read_csv(csv_path)
        assert len(df) == 2
        assert "id" in df.columns
        assert df.iloc[0]["id"] == "nmdc:bsm-11-test001"

    @patch.dict('os.environ', {}, clear=True)
    @patch('nmdc_api_utilities.biosample_search.BiosampleSearch')
    def test_get_biosample_attributes_no_results(self, mock_biosample_search_class, lcms_config_file):
        """Test handling of no biosamples found."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Mock empty results
        mock_search_instance = MagicMock()
        mock_biosample_search_class.return_value = mock_search_instance
        mock_search_instance.get_record_by_filter.return_value = []
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        result = manager.get_biosample_attributes()
        
        assert result is False
        assert manager.should_skip("biosample_attributes_fetched") is False

    @patch.dict('os.environ', {}, clear=True)
    @patch('nmdc_api_utilities.biosample_search.BiosampleSearch')
    def test_get_biosample_attributes_api_error(self, mock_biosample_search_class, lcms_config_file):
        """Test handling of API errors."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Mock API error
        mock_search_instance = MagicMock()
        mock_biosample_search_class.return_value = mock_search_instance
        mock_search_instance.get_record_by_filter.side_effect = Exception("API connection failed")
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        result = manager.get_biosample_attributes()
        
        assert result is False

    @patch.dict('os.environ', {}, clear=True)
    def test_generate_biosample_mapping_script_default(self, lcms_config_file):
        """Test generation of biosample mapping script with defaults."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create scripts directory
        scripts_dir = manager.workflow_path / "scripts"
        scripts_dir.mkdir(parents=True, exist_ok=True)
        
        result = manager.generate_biosample_mapping_script()
        
        assert result is True
        assert manager.should_skip("biosample_mapping_script_generated") is True
        
        # Verify script was created with TEMPLATE suffix
        script_path = manager.workflow_path / "scripts" / "map_raw_files_to_biosamples_TEMPLATE.py"
        assert script_path.exists()
        
        # Verify script is executable
        assert script_path.stat().st_mode & 0o111  # Check executable bits
        
        # Verify script contains study-specific values
        content = script_path.read_text()
        assert "test_lcms_study" in content
        assert "Test LCMS study for unit testing" in content

    @patch.dict('os.environ', {}, clear=True)
    def test_generate_biosample_mapping_script_custom_name(self, lcms_config_file):
        """Test generation with custom script name."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create scripts directory
        scripts_dir = manager.workflow_path / "scripts"
        scripts_dir.mkdir(parents=True, exist_ok=True)
        
        result = manager.generate_biosample_mapping_script(script_name="custom_mapper.py")
        
        assert result is True
        
        script_path = manager.workflow_path / "scripts" / "custom_mapper.py"
        assert script_path.exists()

    @patch.dict('os.environ', {}, clear=True)
    def test_generate_biosample_mapping_script_missing_template(self, lcms_config_file, temp_config_dir):
        """Test handling of missing template file."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Try with non-existent template
        fake_template = temp_config_dir / "nonexistent_template.py"
        result = manager.generate_biosample_mapping_script(template_path=str(fake_template))
        
        assert result is False

    @patch.dict('os.environ', {}, clear=True)
    def test_run_biosample_mapping_script_template_error(self, lcms_config_file):
        """Test that running TEMPLATE script directly is prevented."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Generate template
        manager.generate_biosample_mapping_script()
        
        # Try to run template directly (should fail)
        template_path = manager.workflow_path / "scripts" / "map_raw_files_to_biosamples_TEMPLATE.py"
        result = manager.run_biosample_mapping_script(script_path=str(template_path))
        
        assert result is False

    @patch.dict('os.environ', {}, clear=True)
    def test_run_biosample_mapping_script_not_found(self, lcms_config_file):
        """Test handling of missing mapping script."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        result = manager.run_biosample_mapping_script()
        
        assert result is False

    @patch.dict('os.environ', {}, clear=True)
    @patch('subprocess.run')
    def test_run_biosample_mapping_script_success(self, mock_subprocess, lcms_config_file):
        """Test successful execution of biosample mapping script."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create a non-template script
        script_path = manager.workflow_path / "scripts" / "map_raw_files_to_biosamples.py"
        script_path.parent.mkdir(parents=True, exist_ok=True)
        script_path.write_text("#!/usr/bin/env python3\nprint('Mapping complete')")
        
        # Create fake mapping output for _generate_mapped_files_list
        metadata_dir = manager.workflow_path / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        mapping_df = pd.DataFrame({
            "raw_file_name": ["file1.raw", "file2.raw", "file3.raw"],
            "biosample_id": ["nmdc:bsm-11-001", "nmdc:bsm-11-002", ""],
            "biosample_name": ["Sample 1", "Sample 2", ""],
            "match_confidence": ["high", "medium", "no_match"]
        })
        mapping_df.to_csv(metadata_dir / "mapped_raw_file_biosample_mapping.csv", index=False)
        
        # Mock successful subprocess execution
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_subprocess.return_value = mock_result
        
        result = manager.run_biosample_mapping_script()
        
        assert result is True
        assert manager.should_skip("biosample_mapping_completed") is True
        
        # Verify subprocess was called
        mock_subprocess.assert_called_once()
        
        # Verify mapped_raw_files.csv was generated
        mapped_files = metadata_dir / "mapped_raw_files.csv"
        assert mapped_files.exists()

    @patch.dict('os.environ', {}, clear=True)
    @patch('subprocess.run')
    def test_run_biosample_mapping_script_failure(self, mock_subprocess, lcms_config_file):
        """Test handling of script execution failure."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create a non-template script
        script_path = manager.workflow_path / "scripts" / "map_raw_files_to_biosamples.py"
        script_path.parent.mkdir(parents=True, exist_ok=True)
        script_path.write_text("#!/usr/bin/env python3\nimport sys; sys.exit(1)")
        
        # Mock failed subprocess execution
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_subprocess.return_value = mock_result
        
        result = manager.run_biosample_mapping_script()
        
        assert result is False
        assert manager.should_skip("biosample_mapping_completed") is False

    @patch.dict('os.environ', {}, clear=True)
    def test_generate_mapped_files_list_with_file_types(self, lcms_config_file):
        """Test generation of mapped files list with raw_file_type column."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create metadata directory
        metadata_dir = manager.workflow_path / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # Create mapping with raw_file_type column (new format)
        mapping_df = pd.DataFrame({
            "raw_file_name": ["sample1.raw", "sample2.raw", "cal1.raw", "sample3.raw"],
            "biosample_id": ["nmdc:bsm-11-001", "nmdc:bsm-11-002", "", ""],
            "biosample_name": ["Sample 1", "Sample 2", "", ""],
            "match_confidence": ["high", "medium", "no_match", "low"],
            "raw_file_type": ["sample", "sample", "calibration", "sample"]
        })
        mapping_df.to_csv(metadata_dir / "mapped_raw_file_biosample_mapping.csv", index=False)
        
        # Create raw data directory
        raw_data_dir = Path(manager.raw_data_directory)
        raw_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Call the internal method
        manager._generate_mapped_files_list()
        
        # Verify output file
        output_file = metadata_dir / "mapped_raw_files.csv"
        assert output_file.exists()
        
        # Verify contents - should include high/medium confidence AND calibration files
        result_df = pd.read_csv(output_file)
        assert len(result_df) == 3  # 2 samples + 1 calibration
        assert "sample1.raw" in result_df["raw_file_path"].apply(lambda x: Path(x).name).values
        assert "sample2.raw" in result_df["raw_file_path"].apply(lambda x: Path(x).name).values
        assert "cal1.raw" in result_df["raw_file_path"].apply(lambda x: Path(x).name).values

    @patch.dict('os.environ', {}, clear=True)
    def test_generate_mapped_files_list_legacy_format(self, lcms_config_file):
        """Test generation of mapped files list without raw_file_type (backwards compatibility)."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create metadata directory
        metadata_dir = manager.workflow_path / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # Create mapping without raw_file_type column (old format)
        mapping_df = pd.DataFrame({
            "raw_file_name": ["sample1.raw", "sample2.raw", "sample3.raw"],
            "biosample_id": ["nmdc:bsm-11-001", "nmdc:bsm-11-002", ""],
            "biosample_name": ["Sample 1", "Sample 2", ""],
            "match_confidence": ["high", "medium", "low"]
        })
        mapping_df.to_csv(metadata_dir / "mapped_raw_file_biosample_mapping.csv", index=False)
        
        # Create raw data directory
        raw_data_dir = Path(manager.raw_data_directory)
        raw_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Call the internal method
        manager._generate_mapped_files_list()
        
        # Verify output file
        output_file = metadata_dir / "mapped_raw_files.csv"
        assert output_file.exists()
        
        # Verify contents - should only include high/medium confidence
        result_df = pd.read_csv(output_file)
        assert len(result_df) == 2  # Only high and medium

    @patch.dict('os.environ', {}, clear=True)
    def test_generate_mapped_files_list_no_matches(self, lcms_config_file):
        """Test handling of no matched files."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create metadata directory
        metadata_dir = manager.workflow_path / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # Create mapping with no matches
        mapping_df = pd.DataFrame({
            "raw_file_name": ["sample1.raw", "sample2.raw"],
            "biosample_id": ["", ""],
            "biosample_name": ["", ""],
            "match_confidence": ["no_match", "low"]
        })
        mapping_df.to_csv(metadata_dir / "mapped_raw_file_biosample_mapping.csv", index=False)
        
        # Call the internal method (should warn and return early)
        manager._generate_mapped_files_list()
        
        # Method returns early when no matches, so no output file should be created
        output_file = metadata_dir / "mapped_raw_files.csv"
        # The early return means the file never gets created
        assert not output_file.exists()

    @patch.dict('os.environ', {}, clear=True)
    def test_skip_triggers_for_biosample_workflow(self, lcms_config_file):
        """Test that skip triggers work correctly for biosample workflow steps."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Initially all should be False
        assert manager.should_skip("biosample_attributes_fetched") is False
        assert manager.should_skip("biosample_mapping_script_generated") is False
        assert manager.should_skip("biosample_mapping_completed") is False
        
        # Set triggers
        manager.set_skip_trigger("biosample_attributes_fetched", True, save=False)
        manager.set_skip_trigger("biosample_mapping_script_generated", True, save=False)
        manager.set_skip_trigger("biosample_mapping_completed", True, save=False)
        
        # Verify they're set
        assert manager.should_skip("biosample_attributes_fetched") is True
        assert manager.should_skip("biosample_mapping_script_generated") is True
        assert manager.should_skip("biosample_mapping_completed") is True
        
        # Reset and verify
        manager.reset_all_triggers(save=False)
        assert manager.should_skip("biosample_attributes_fetched") is False
        assert manager.should_skip("biosample_mapping_script_generated") is False
        assert manager.should_skip("biosample_mapping_completed") is False
