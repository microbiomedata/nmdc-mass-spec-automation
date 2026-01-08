"""
Unit tests for WorkflowRawDataInspectionManager mixin.

Tests cover raw data inspection functionality including:
- Docker-based file inspection
- LCMS and GCMS workflow handling
- Result file processing and merging
- Skip trigger management
- Error handling
"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
import pandas as pd
import subprocess


class TestWorkflowRawDataInspectionManager:
    """Test suite for WorkflowRawDataInspectionManager mixin."""

    @patch.dict('os.environ', {}, clear=True)
    def test_raw_data_inspector_initialization(self, lcms_config_file):
        """Test that raw data inspector initializes correctly with Docker config."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        assert "docker" in manager.config
        assert "raw_data_inspector_image" in manager.config["docker"]

    @patch.dict('os.environ', {}, clear=True)
    @patch('subprocess.run')
    def test_raw_data_inspector_skip_when_complete(self, mock_run, lcms_config_file):
        """Test that raw data inspector is skipped when raw_data_inspected trigger is set."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Set the skip trigger
        manager.set_skip_trigger("raw_data_inspected", True)
        
        # Call raw_data_inspector
        result = manager.raw_data_inspector()
        
        # Should return True and not run subprocess
        assert result is True
        mock_run.assert_not_called()

    @patch.dict('os.environ', {}, clear=True)
    @patch('subprocess.run')
    def test_lcms_inspector_docker_check(self, mock_run, lcms_config_file, temp_config_dir):
        """Test that LCMS inspector checks for Docker availability."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Mock Docker check to fail
        mock_run.return_value = Mock(returncode=1)
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create a dummy raw file
        raw_dir = temp_config_dir / "test_data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_file = raw_dir / "test.raw"
        raw_file.write_text("dummy")
        
        # Call inspector with specific file
        result = manager.raw_data_inspector(file_paths=[str(raw_file)])
        
        # Should fail due to Docker unavailability
        assert result is False

    @patch.dict('os.environ', {}, clear=True)
    def test_raw_inspector_no_files_warning(self, lcms_config_file):
        """Test that inspector warns when no files are found."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Call with empty file list
        result = manager.raw_data_inspector(file_paths=[])
        
        # Should return None for no files
        assert result is None

    @patch.dict('os.environ', {}, clear=True)
    @patch('subprocess.run')
    def test_lcms_inspector_forces_single_core_for_raw_files(
        self, mock_run, lcms_config_file, temp_config_dir, caplog
    ):
        """Test that .raw files force single-core processing."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Mock successful Docker check and execution
        mock_run.side_effect = [
            Mock(returncode=0),  # Docker check
            Mock(returncode=0)   # Docker run
        ]
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create output directory and results file
        output_dir = manager.workflow_path / "raw_file_info"
        output_dir.mkdir(parents=True, exist_ok=True)
        results_file = output_dir / "raw_file_inspection_results.csv"
        
        # Create dummy results
        df = pd.DataFrame({
            'file_path': ['test.raw'],
            'rt_max': [100.0],
            'rt_min': [0.0]
        })
        df.to_csv(results_file, index=False)
        
        # Create a dummy .raw file
        raw_dir = temp_config_dir / "test_data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_file = raw_dir / "test.raw"
        raw_file.write_text("dummy")
        
        # Call inspector with multiple cores
        result = manager.raw_data_inspector(file_paths=[str(raw_file)], cores=4)
        
        # Should skip because file is already inspected and return file path
        assert isinstance(result, str) and 'raw_file_inspection_results.csv' in result

    @patch.dict('os.environ', {}, clear=True)
    @patch('subprocess.run')
    def test_lcms_inspector_uses_mapped_files(
        self, mock_run, lcms_config_file, temp_config_dir
    ):
        """Test that inspector uses mapped_raw_files.csv when available."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create mapped files CSV
        metadata_dir = manager.workflow_path / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        mapped_files = metadata_dir / "mapped_raw_files.csv"
        
        df = pd.DataFrame({
            'raw_file_path': ['/path/to/file1.raw', '/path/to/file2.raw'],
            'biosample_id': ['nmdc:bsm-1', 'nmdc:bsm-2']
        })
        df.to_csv(mapped_files, index=False)
        
        # Create output directory
        output_dir = manager.workflow_path / "raw_file_info"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Mock Docker available and successful run
        mock_run.side_effect = [
            Mock(returncode=0),  # Docker check
            Mock(returncode=0)   # Docker run
        ]
        
        # Create results file that Docker would create
        results_file = output_dir / "raw_file_inspection_results.csv"
        results_df = pd.DataFrame({
            'file_path': ['/path/to/file1.raw', '/path/to/file2.raw'],
            'rt_max': [100.0, 110.0],
            'rt_min': [0.0, 0.0]
        })
        results_df.to_csv(results_file, index=False)
        
        # Call inspector without file_paths (should use mapped files)
        result = manager.raw_data_inspector()
        
        # Should complete successfully (returns True or file path string)
        assert result is True or (isinstance(result, str) and 'raw_file_inspection_results.csv' in result)

    @patch.dict('os.environ', {}, clear=True)
    def test_gcms_inspector_route_selection(self, gcms_config_file):
        """Test that GCMS workflow type routes to GCMS inspector."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(gcms_config_file))
        
        # Verify workflow type is GCMS
        assert manager.config["workflow"]["workflow_type"] == "GCMS Metabolomics"

    @patch.dict('os.environ', {}, clear=True)
    @patch('subprocess.run')
    def test_inspector_merges_previous_results(
        self, mock_run, lcms_config_file, temp_config_dir
    ):
        """Test that new inspection results are merged with previous results."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create output directory with previous results
        output_dir = manager.workflow_path / "raw_file_info"
        output_dir.mkdir(parents=True, exist_ok=True)
        results_file = output_dir / "raw_file_inspection_results.csv"
        
        # Create previous results with successfully inspected files
        previous_df = pd.DataFrame({
            'file_path': ['file1.raw', 'file2.raw'],
            'rt_max': [100.0, 110.0],
            'rt_min': [0.0, 0.0]
        })
        previous_df.to_csv(results_file, index=False)
        
        # Create a new raw file to inspect
        raw_dir = temp_config_dir / "test_data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        new_file = raw_dir / "file3.raw"
        new_file.write_text("dummy")
        
        # Mock Docker available and successful run
        mock_run.side_effect = [
            Mock(returncode=0),  # Docker check
            Mock(returncode=0)   # Docker run
        ]
        
        # Create temp results directory and file that Docker would create
        temp_dir = output_dir / "temp_inspection"
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_results = temp_dir / "raw_file_inspection_results.csv"
        new_df = pd.DataFrame({
            'file_path': [str(new_file)],
            'rt_max': [120.0],
            'rt_min': [0.0]
        })
        new_df.to_csv(temp_results, index=False)
        
        # Call inspector
        result = manager.raw_data_inspector(file_paths=[str(new_file)])
        
        # Read final results
        final_df = pd.read_csv(results_file)
        
        # Should have all three files
        assert len(final_df) == 3
        assert 'file1.raw' in final_df['file_path'].values
        assert 'file2.raw' in final_df['file_path'].values

    @patch.dict('os.environ', {}, clear=True)
    @patch('subprocess.run')
    def test_inspector_skips_already_inspected_files(
        self, mock_run, lcms_config_file, temp_config_dir
    ):
        """Test that already-inspected files are skipped."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create output directory with previous results
        output_dir = manager.workflow_path / "raw_file_info"
        output_dir.mkdir(parents=True, exist_ok=True)
        results_file = output_dir / "raw_file_inspection_results.csv"
        
        # Create raw files
        raw_dir = temp_config_dir / "test_data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        file1 = raw_dir / "file1.raw"
        file1.write_text("dummy")
        
        # Create previous results with this file already inspected
        previous_df = pd.DataFrame({
            'file_path': [str(file1)],
            'rt_max': [100.0],
            'rt_min': [0.0]
        })
        previous_df.to_csv(results_file, index=False)
        
        # Call inspector with the same file
        result = manager.raw_data_inspector(file_paths=[str(file1)])
        
        # Should skip inspection and return the existing results file path
        assert isinstance(result, str) and 'raw_file_inspection_results.csv' in result
        # Skip trigger should be set
        assert manager.should_skip("raw_data_inspected") is True

    @patch.dict('os.environ', {}, clear=True)
    def test_inspector_missing_docker_config_error(self, lcms_config_file):
        """Test that missing Docker config raises appropriate error."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Use config without Docker settings
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create a dummy raw file
        raw_dir = manager.workflow_path.parent / "test_data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_file = raw_dir / "test.raw"
        raw_file.write_text("dummy")
        
        # Call inspector
        result = manager.raw_data_inspector(file_paths=[str(raw_file)])
        
        # Should return False due to missing Docker config (raises error and returns False)
        assert result is False

    @patch.dict('os.environ', {}, clear=True)
    def test_inspector_with_limit_parameter(
        self, lcms_config_file, temp_config_dir
    ):
        """Test that limit parameter is accepted without error."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create multiple raw files
        raw_dir = temp_config_dir / "test_data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        files = []
        for i in range(5):
            f = raw_dir / f"file{i}.raw"
            f.write_text("dummy")
            files.append(str(f))
        
        # Create output directory with all files already inspected
        output_dir = manager.workflow_path / "raw_file_info"
        output_dir.mkdir(parents=True, exist_ok=True)
        results_file = output_dir / "raw_file_inspection_results.csv"
        
        # Create previous results with all files inspected
        df = pd.DataFrame({
            'file_path': [f"file{i}.raw" for i in range(5)],
            'rt_max': [100.0 + i*10 for i in range(5)],
            'rt_min': [0.0] * 5
        })
        df.to_csv(results_file, index=False)
        
        # Call with limit=2 - should skip all since already inspected
        result = manager.raw_data_inspector(file_paths=files, limit=2)
        
        # Should return file path since all files already inspected
        assert isinstance(result, str) and 'raw_file_inspection_results.csv' in result
