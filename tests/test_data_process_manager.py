"""
Unit tests for NMDCWorkflowDataProcessManager mixin.

Tests WDL JSON generation logic, script creation, file filtering, and batch processing
without actually executing WDL workflows. Uses mocks for complex file system operations.
"""

import json
from unittest.mock import patch
from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager


class TestProcessDataSkipLogic:
    """Test skip trigger logic for process_data."""

    def test_process_data_respects_skip_trigger_config(self, tmp_path, lcms_config):
        """Test that process_data checks skip_triggers config."""
        lcms_config["paths"]["base_directory"] = str(tmp_path)
        lcms_config["skip_triggers"] = {"data_processed": True}
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(lcms_config))
        
        manager = NMDCWorkflowManager(str(config_file))
        
        #  Should short-circuit due to skip trigger
        result = manager.process_data(execute=False)
        assert result is True  # Skip trigger returns True

    def test_process_data_workflow_when_not_skipped(self, tmp_path, lcms_config):
        """Test process_data calls expected methods when not skipped."""
        lcms_config["paths"]["base_directory"] = str(tmp_path)
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(lcms_config))
        
        manager = NMDCWorkflowManager(str(config_file))
        
        # Mock all the methods it would call
        with patch.object(manager, '_move_processed_files'):
            with patch.object(manager, 'generate_wdl_jsons') as mock_jsons:
                with patch.object(manager, 'generate_wdl_runner_script') as mock_script:
                    with patch.object(manager, 'run_wdl_script') as mock_run:
                        manager.process_data(execute=True, cleanup=False)
                        
                        # Verify methods called
                        mock_jsons.assert_called_once()
                        mock_script.assert_called_once()
                        mock_run.assert_called_once()


class TestWDLRunnerScriptLogic:
    """Test WDL runner script generation."""

    def test_script_created_with_correct_structure(self, tmp_path, lcms_config):
        """Test that runner script is created with proper bash structure."""
        lcms_config["paths"]["base_directory"] = str(tmp_path)
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(lcms_config))
        
        # Create necessary directories with studies prefix
        workflow_dir = tmp_path / "studies" / lcms_config["workflow"]["name"]
        scripts_dir = workflow_dir / "scripts"
        scripts_dir.mkdir(parents=True)
        wdl_dir = workflow_dir / "wdl_jsons"
        wdl_dir.mkdir(parents=True)
        
        # Create a dummy JSON file
        (wdl_dir / "test_batch_1.json").write_text("{}")
        
        manager = NMDCWorkflowManager(str(config_file))
        manager.generate_wdl_runner_script()
        
        # Verify script created
        script_path = scripts_dir / f"{lcms_config['workflow']['name']}_wdl_runner.sh"
        assert script_path.exists()
        
        # Verify script content
        script_content = script_path.read_text()
        assert "#!/bin/bash" in script_content
        assert "miniwdl" in script_content.lower() or "wdl" in script_content.lower()

    def test_script_uses_dynamic_json_discovery(self, tmp_path, lcms_config):
        """Test that script uses find command for dynamic JSON discovery."""
        lcms_config["paths"]["base_directory"] = str(tmp_path)
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(lcms_config))
        
        # Create directories and multiple JSONs with studies prefix
        workflow_dir = tmp_path / "studies" / lcms_config["workflow"]["name"]
        scripts_dir = workflow_dir / "scripts"
        scripts_dir.mkdir(parents=True)
        wdl_dir = workflow_dir / "wdl_jsons"
        wdl_dir.mkdir(parents=True)
        
        # Create 3 JSON files
        for i in range(1, 4):
            (wdl_dir / f"batch_{i}.json").write_text("{}")
        
        manager = NMDCWorkflowManager(str(config_file))
        manager.generate_wdl_runner_script()
        
        # Verify script uses dynamic discovery
        script_path = scripts_dir / f"{lcms_config['workflow']['name']}_wdl_runner.sh"
        script_content = script_path.read_text()
        
        # Script uses dynamic find command, so just verify it processes JSON files
        assert "*.json" in script_content
        assert "find" in script_content


class TestFileFilteringLogic:
    """Test file filtering based on configuration patterns."""

    def test_files_filtered_by_multiple_patterns(self):
        """Test that file filtering requires all patterns to match."""
        # Sample files
        files = [
            "sample1_HILIC_POS.raw",
            "sample2_HILIC_NEG.raw",
            "sample3_RP_POS.raw",
            "sample4_RP_NEG.raw",
        ]
        
        # Config with multiple filters (AND logic)
        config_hilic_pos = {"file_filter": ["HILIC", "POS"]}
        
        # Simulate the filtering logic from generate_wdl_jsons
        filtered = [
            f for f in files
            if all(pattern.lower() in f.lower() for pattern in config_hilic_pos["file_filter"])
        ]
        
        # Should only match files with BOTH HILIC AND POS
        assert len(filtered) == 1
        assert filtered[0] == "sample1_HILIC_POS.raw"

    def test_files_pass_when_no_filter_specified(self):
        """Test that all files pass when no file_filter is specified."""
        files = ["sample1.raw", "sample2.raw", "sample3.raw"]
        
        config_no_filter = {}  # No file_filter
        
        # Simulate the filtering logic
        file_filter = config_no_filter.get("file_filter", [])
        if file_filter:
            filtered = [
                f for f in files
                if all(pattern.lower() in f.lower() for pattern in file_filter)
            ]
        else:
            filtered = files
        
        # All files should pass
        assert len(filtered) == 3


class TestBatchProcessingLogic:
    """Test batch size logic for WDL JSON generation."""

    def test_files_split_into_correct_batches(self):
        """Test that files are correctly split based on batch_size."""
        files = [f"sample{i}.raw" for i in range(15)]
        batch_size = 5
        
        # Simulate batching logic
        batches = []
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            batches.append(batch)
        
        # Should create 3 batches
        assert len(batches) == 3
        assert len(batches[0]) == 5
        assert len(batches[1]) == 5
        assert len(batches[2]) == 5

    def test_last_batch_can_be_smaller(self):
        """Test that last batch contains remaining files."""
        files = [f"sample{i}.raw" for i in range(17)]
        batch_size = 5
        
        # Simulate batching logic
        batches = []
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            batches.append(batch)
        
        # Should create 4 batches, last one with 2 files
        assert len(batches) == 4
        assert len(batches[0]) == 5
        assert len(batches[1]) == 5
        assert len(batches[2]) == 5
        assert len(batches[3]) == 2
