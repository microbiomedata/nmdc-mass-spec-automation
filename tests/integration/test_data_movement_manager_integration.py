"""
Integration tests for WorkflowDataMovementManager mixin.

These tests use real MASSIVE FTP connections and Docker containers to validate
data movement functionality end-to-end. Requires network access and Docker.
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch



@pytest.mark.integration
@pytest.mark.network
class TestMASSIVEFTPIntegration:
    """Integration tests for MASSIVE FTP operations."""

    def test_crawl_real_massive_dataset(self, tmp_path, lcms_config):
        """Test crawling a real MASSIVE dataset (read-only, no downloads)."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Use a small, stable MASSIVE dataset for testing
        # MSV000094090 is the test dataset used in the project
        lcms_config["paths"]["base_directory"] = str(tmp_path)
        lcms_config["workflow"]["massive_id"] = "MSV000094090"
        lcms_config["workflow"]["file_type"] = ".raw"
        lcms_config["workflow"]["file_filters"] = ["HILICZ", "NEG"]
        
        import json
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(lcms_config))
        
        manager = NMDCWorkflowManager(str(config_file))
        manager.create_workflow_structure()
        
        # Crawl the FTP (this should work with real FTP server)
        log_file = manager._crawl_massive_ftp("v07/MSV000094090")
        
        # Verify log file created
        assert Path(log_file).exists()
        
        # Verify it found some .raw files
        with open(log_file, 'r') as f:
            contents = f.read()
            assert len(contents) > 0
            assert 'ftp://massive-ftp.ucsd.edu' in contents
            assert '.raw' in contents.lower()

    def test_parse_and_filter_massive_urls(self, tmp_path, lcms_config):
        """Test parsing and filtering MASSIVE FTP URLs."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        lcms_config["paths"]["base_directory"] = str(tmp_path)
        lcms_config["workflow"]["massive_id"] = "MSV000094090"
        lcms_config["workflow"]["file_type"] = ".raw"
        lcms_config["workflow"]["file_filters"] = ["HILICZ"]
        
        import json
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(lcms_config))
        
        manager = NMDCWorkflowManager(str(config_file))
        manager.create_workflow_structure()
        
        # Crawl and parse
        log_file = manager._crawl_massive_ftp("v07/MSV000094090")
        filtered_df = manager.parse_massive_ftp_log(log_file)
        
        # Verify filtering worked
        assert len(filtered_df) > 0
        assert 'ftp_location' in filtered_df.columns
        assert 'raw_data_file_short' in filtered_df.columns
        # All files should match the filter
        for filename in filtered_df['raw_data_file_short']:
            assert 'HILICZ' in filename.upper()



