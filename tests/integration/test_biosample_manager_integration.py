"""
Integration tests for NMDCWorkflowBiosampleManager mixin.

These tests interact with the real NMDC API to verify end-to-end functionality.
"""

import pytest
from pathlib import Path
import pandas as pd


class TestNMDCWorkflowBiosampleManagerIntegration:
    """Integration test suite for NMDCWorkflowBiosampleManager mixin."""

    @pytest.mark.network
    def test_get_biosample_attributes_real_api(self, integration_config_file):
        """
        Integration test: Test get_biosample_attributes() with real NMDC API.
        
        This test uses the actual get_biosample_attributes() method with a real
        study ID to verify the API integration still works correctly.
        Uses real study ID from docstrings: nmdc:sty-11-dwsv7q78
        
        Note: Requires network connection. May be slow or fail if API is down.
        """
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        try:
            # Create manager with real study config
            manager = NMDCWorkflowManager(str(integration_config_file))
            result = manager.get_biosample_attributes()
            
            # Verify the method succeeded
            assert result is True, "get_biosample_attributes() returned False - check logs for API errors"
            
            # Verify skip trigger was set
            assert manager.should_skip("biosample_attributes_fetched") is True
            
            # Verify CSV was created (use manager's actual workflow_path)
            csv_path = manager.workflow_path / "metadata" / "biosample_attributes.csv"
            assert csv_path.exists(), f"biosample_attributes.csv not created at {csv_path}"
            
            # Verify CSV contents
            df = pd.read_csv(csv_path)
            assert len(df) > 0, "No biosamples found in CSV"
            assert "id" in df.columns, "Missing 'id' column in CSV"
            
            # Verify all biosamples have NMDC IDs
            assert all(df["id"].str.startswith("nmdc:bsm-")), "Invalid biosample IDs in CSV"
            
            print(f"\n✓ Successfully retrieved {len(df)} biosamples via get_biosample_attributes()")
            print(f"✓ CSV columns: {list(df.columns)}")
            print(f"✓ Sample biosample ID: {df.iloc[0]['id']}")
            
        except Exception as e:
            pytest.fail(f"get_biosample_attributes() failed with real API: {e}\nThis may indicate API changes or network issues.")
