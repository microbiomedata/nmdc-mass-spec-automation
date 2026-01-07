"""
Integration tests for WorkflowRawDataInspectionManager mixin.

These tests interact with real files and Docker containers to verify end-to-end functionality.
The test downloads a real .raw file from MASSIVE and performs actual inspection.
"""

import pytest
from pathlib import Path
import pandas as pd
import requests
import subprocess


class TestWorkflowRawDataInspectionManagerIntegration:
    """Integration test suite for WorkflowRawDataInspectionManager mixin."""

    @pytest.mark.network
    @pytest.mark.slow
    def test_raw_data_inspector_real_file_download_and_inspection(
        self, integration_raw_data_config_file, integration_test_raw_file
    ):
        """
        Integration test: Download real .raw file from MASSIVE and inspect it.
        
        Uses session-scoped fixture to download file once and cache it.
        File: 20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490_NEG_MSMS_19_S16-D89_A_Rg70to1050-CE102040-soil-S1_Run84.raw
        
        Note: Requires network connection and Docker. May be slow (~95MB download on first run).
        """
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Check if Docker is available before attempting test
        try:
            docker_check = subprocess.run(
                ["docker", "--version"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if docker_check.returncode != 0:
                pytest.skip("Docker is not available")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pytest.skip("Docker is not installed or not available")
        
        # Create manager
        manager = NMDCWorkflowManager(str(integration_raw_data_config_file))
        
        # Copy the cached test file to the manager's raw data directory
        raw_data_dir = Path(manager.raw_data_directory)
        raw_data_dir.mkdir(parents=True, exist_ok=True)
        
        raw_file_path = raw_data_dir / integration_test_raw_file.name
        if not raw_file_path.exists():
            import shutil
            shutil.copy2(integration_test_raw_file, raw_file_path)
        
        print(f"\nUsing test file: {raw_file_path}")
        
        # Verify file was copied
        assert raw_file_path.exists(), f"Raw file not found: {raw_file_path}"
        assert raw_file_path.stat().st_size > 0, "Raw file is empty"
        
        # Run raw data inspection
        print(f"\nRunning raw data inspection with Docker...")
        result = manager.raw_data_inspector(
            file_paths=[str(raw_file_path)],
            cores=1  # Force single core for .raw files
        )
        
        # Verify the method succeeded
        assert result is True, "raw_data_inspector() returned False - check logs for errors"
        
        # Verify skip trigger was set
        assert manager.should_skip("raw_data_inspected") is True, "Skip trigger not set"
        
        # Verify results CSV was created
        results_csv = manager.workflow_path / "raw_file_info" / "raw_file_inspection_results.csv"
        assert results_csv.exists(), f"Results CSV not created at {results_csv}"
        
        # Verify CSV contents
        df = pd.read_csv(results_csv)
        assert len(df) > 0, "No results found in CSV"
        assert "file_path" in df.columns, "Missing 'file_path' column"
        assert "rt_max" in df.columns, "Missing 'rt_max' column"
        assert "rt_min" in df.columns, "Missing 'rt_min' column"
        
        # Verify the specific file was inspected
        raw_file_name = integration_test_raw_file.name
        assert any(raw_file_name in str(path) for path in df["file_path"]), \
            f"Expected file {raw_file_name} not found in results"
        
        # Verify retention time values are reasonable
        row = df[df["file_path"].str.contains(raw_file_name)].iloc[0]
        rt_max = float(row["rt_max"])
        rt_min = float(row["rt_min"])
        
        print(f"\n✓ Inspection successful!")
        print(f"  RT Range: {rt_min:.2f} - {rt_max:.2f} minutes")
        
        # Basic sanity checks on RT values
        assert rt_max > rt_min, "rt_max should be greater than rt_min"
        assert rt_min >= 0, "rt_min should be non-negative"
        assert rt_max < 1000, "rt_max seems unreasonably high (> 1000 min)"
        
        print(f"✓ All assertions passed!")

    @pytest.mark.network
    @pytest.mark.slow  
    def test_raw_data_inspector_handles_errors_gracefully(
        self, integration_raw_data_config_file
    ):
        """
        Integration test: Verify inspector handles non-existent files gracefully.
        """
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Check Docker availability
        try:
            docker_check = subprocess.run(
                ["docker", "--version"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if docker_check.returncode != 0:
                pytest.skip("Docker is not available")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pytest.skip("Docker is not installed")
        
        manager = NMDCWorkflowManager(str(integration_raw_data_config_file))
        
        # Try to inspect a non-existent file
        result = manager.raw_data_inspector(
            file_paths=["/nonexistent/file.raw"]
        )
        
        # Should handle error gracefully
        # The method may return False or None depending on error handling
        assert result in [False, None], "Should handle non-existent file gracefully"

    @pytest.mark.network
    @pytest.mark.slow
    def test_raw_data_inspector_empty_file_list(
        self, integration_raw_data_config_file
    ):
        """
        Integration test: Verify inspector handles empty file list.
        """
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(integration_raw_data_config_file))
        
        # Call with empty file list
        result = manager.raw_data_inspector(file_paths=[])
        
        # Should return None for no files
        assert result is None, "Should return None when no files to inspect"

    @pytest.mark.network
    @pytest.mark.slow
    def test_raw_data_inspector_skip_trigger_prevents_rerun(
        self, integration_raw_data_config_file
    ):
        """
        Integration test: Verify skip trigger prevents re-running inspection.
        """
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(integration_raw_data_config_file))
        
        # Set the data_processed skip trigger
        manager.set_skip_trigger("data_processed", True)
        
        # Create a dummy raw file
        raw_data_dir = Path(manager.raw_data_directory)
        raw_data_dir.mkdir(parents=True, exist_ok=True)
        dummy_file = raw_data_dir / "dummy.raw"
        dummy_file.write_text("dummy content")
        
        # Call inspector - should be skipped
        result = manager.raw_data_inspector(file_paths=[str(dummy_file)])
        
        # Should return True immediately without running (decorator skips execution)
        assert result is True, "Should skip when data_processed trigger is set"
        
        # Results file should not be created since we skipped
        results_csv = manager.workflow_path / "raw_file_info" / "raw_file_inspection_results.csv"
        # Note: file might exist from previous runs, but we shouldn't have added to it
        # The key test is that it returned True immediately via the decorator
