"""
Unit tests for WorkflowDataMovementManager mixin.

Tests cover data movement functionality including:
- MASSIVE FTP crawling and URL discovery
- FTP log parsing and file filtering
- Downloading from MASSIVE
- Unified fetch_raw_data() method
"""

import json
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
import ftplib


class TestMASSIVEOperations:
    """Test MASSIVE FTP crawling, parsing, and downloading."""

    def test_crawl_and_parse_massive_ftp(self, lcms_config_file):
        """Test FTP crawling and log parsing with filters."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        manager.create_workflow_structure()
        
        # Mock FTP connection
        mock_ftp = MagicMock()
        mock_ftp.pwd.return_value = "v07/MSV000094090"
        
        mock_list_output = [
            "-rw-r--r-- 1 user group 1024 Jan 01 12:00 sample1_HILICZ_pos.raw",
            "-rw-r--r-- 1 user group 2048 Jan 01 12:00 sample2_HILICZ_neg.raw",
            "-rw-r--r-- 1 user group 512 Jan 01 12:00 sample3_rp_pos.raw",
        ]
        
        def mock_retrlines(cmd, callback):
            for line in mock_list_output:
                callback(line)
        
        mock_ftp.retrlines.side_effect = mock_retrlines
        
        with patch('ftplib.FTP', return_value=mock_ftp):
            log_file = manager._crawl_massive_ftp("v07/MSV000094090")
            assert Path(log_file).exists()
            
            # Parse with filters (config has HILICZ filter)
            result_df = manager.parse_massive_ftp_log(str(log_file))
            
            # Should only keep HILICZ files
            assert len(result_df) == 2
            assert all('HILICZ' in name for name in result_df['raw_data_file_short'])
            mock_ftp.login.assert_called_once()

    def test_ftp_crawl_error_handling(self, lcms_config_file):
        """Test handling of FTP errors."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        manager.create_workflow_structure()
        
        mock_ftp = MagicMock()
        mock_ftp.cwd.side_effect = ftplib.error_perm("550 Permission denied")
        
        with patch('ftplib.FTP', return_value=mock_ftp):
            log_file = manager._crawl_massive_ftp("invalid/path")
            assert log_file == []  # Returns empty list on error


class TestMASSIVEWorkflow:
    """Test complete MASSIVE workflow."""

    def test_get_massive_ftp_urls_workflow(self, lcms_config_file):
        """Test complete get_massive_ftp_urls workflow."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        with patch.object(manager, '_crawl_massive_ftp') as mock_crawl:
            with patch.object(manager, 'parse_massive_ftp_log') as mock_parse:
                log_path = str(manager.workflow_path / "raw_file_info" / "massive_ftp_locs.txt")
                mock_crawl.return_value = log_path
                mock_parse.return_value = pd.DataFrame({
                    'ftp_location': ['ftp://test/file1.raw'],
                    'raw_data_file_short': ['file1.raw']
                })
                
                result = manager.get_massive_ftp_urls()
                assert result is True
                mock_crawl.assert_called_once()

    def test_download_from_massive(self, lcms_config_file):
        """Test downloading files from MASSIVE."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        # Create FTP CSV
        ftp_csv = manager.workflow_path / "raw_file_info" / "massive_ftp_locs.csv"
        ftp_csv.parent.mkdir(parents=True, exist_ok=True)
        
        test_df = pd.DataFrame({
            'ftp_location': ['ftp://test/sample1.raw', 'ftp://test/sample2.raw'],
            'raw_data_file_short': ['sample1.raw', 'sample2.raw']
        })
        test_df.to_csv(ftp_csv, index=False)
        
        # Create raw data directory
        raw_dir = Path(manager.raw_data_directory)
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        with patch.object(manager, '_download_file_wget') as mock_download:
            with patch('os.path.exists', return_value=False):
                result = manager.download_from_massive(ftp_file="raw_file_info/massive_ftp_locs.csv")
                
                assert result is True
                assert mock_download.call_count == 2
                # Verify skip trigger set
                assert manager.should_skip("raw_data_downloaded") is True


class TestFTPLogParsing:
    """Test FTP log parsing edge cases."""

    def test_parse_without_filters_warning(self, tmp_path, lcms_config):
        """Test warning when no file_filters configured."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        # Remove file_filters from config
        lcms_config["workflow"].pop("file_filters", None)
        lcms_config["paths"]["base_directory"] = str(tmp_path)
        
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(lcms_config))
        
        manager = NMDCWorkflowManager(str(config_file))
        
        # Create log file
        log_file = manager.workflow_path / "raw_file_info" / "massive_ftp_locs.txt"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        test_urls = [
            "ftp://massive-ftp.ucsd.edu/v07/MSV000094090/file1.raw\n",
            "ftp://massive-ftp.ucsd.edu/v07/MSV000094090/file2.raw\n",
        ]
        
        log_file.write_text(''.join(test_urls))
        
        # Parse should return all files
        result_df = manager.parse_massive_ftp_log(str(log_file))
        assert len(result_df) == 2


class TestFetchRawData:
    """Test unified fetch_raw_data method."""

    def test_fetch_routes_to_massive(self, lcms_config_file):
        """Test fetch_raw_data routes to MASSIVE when massive_id configured."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        with patch.object(manager, 'get_massive_ftp_urls', return_value=True) as mock_urls:
            with patch.object(manager, 'download_from_massive', return_value=True) as mock_download:
                result = manager.fetch_raw_data()
                assert result is True
                mock_urls.assert_called_once()
                mock_download.assert_called_once()

    def test_fetch_skip_trigger(self, tmp_path, lcms_config):
        """Test that fetch_raw_data respects skip triggers."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        lcms_config["paths"]["base_directory"] = str(tmp_path)
        lcms_config["skip_triggers"] = {"raw_data_downloaded": True}
        
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(lcms_config))
        
        manager = NMDCWorkflowManager(str(config_file))
        
        with patch.object(manager, 'get_massive_ftp_urls') as mock_urls:
            result = manager.fetch_raw_data()
            assert result is True
            mock_urls.assert_not_called()

