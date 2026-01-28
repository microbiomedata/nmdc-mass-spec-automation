"""
Pytest configuration for NMDC Mass Spec Automation tests.

This file ensures the project root is on sys.path so tests can import nmdc_dp_utils.
Also provides common fixtures used across test modules.
"""

import sys
import json
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

# Add project root to sys.path so nmdc_dp_utils can be imported
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (may be slow, require network)"
    )


@pytest.fixture(autouse=True)
def clean_environment():
    """Clear environment variables for test isolation (applied to all tests automatically)."""
    with patch.dict('os.environ', {}, clear=True):
        yield


@pytest.fixture
def mock_subprocess_run():
    """Mock subprocess.run for Docker/command execution tests."""
    with patch('subprocess.run') as mock_run:
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        yield mock_run


@pytest.fixture
def mock_biosample_search():
    """Mock NMDC API BiosampleSearch for biosample attribute tests."""
    with patch('nmdc_api_utilities.biosample_search.BiosampleSearch') as mock_search:
        yield mock_search


@pytest.fixture
def temp_config_dir():
    """Create a temporary directory for test configs."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def lcms_config(temp_config_dir):
    """Comprehensive LCMS configuration for all LCMS unit tests.
    
    Includes Docker, metadata generation, multiple configurations, and MASSIVE integration.
    Use this for testing raw data inspection, metadata generation, and biosample mapping.
    """
    return {
        "workflow": {
            "name": "test_lcms_workflow",
            "type": "lcms_metabolomics",
            "workflow_type": "LCMS Metabolomics",
            "massive_id": "MSV000094090",
            "file_type": ".raw",
            "file_filters": ["HILICZ"]
        },
        "study": {
            "id": "nmdc:sty-11-test",
            "name": "test_lcms_study",
            "description": "Test LCMS study for unit testing"
        },
        "paths": {
            "base_directory": str(temp_config_dir / "lcms_base"),
            "data_directory": str(temp_config_dir / "lcms_data")
        },
        "docker": {
            "raw_data_inspector_image": "microbiomedata/metams:3.3.3"
        },
        "metadata": {
            "processing_institution_workflow": "EMSL",
            "processing_institution_generation": "EMSL",
            "raw_data_location": "massive",
            "instrument_used": "Thermo Q Exactive",
            "mass_spec_configuration_name": "QE_MSMS",
            "chromat_configuration_name": "HILIC"
        },
        "configurations": [
            {
                "name": "hilic_pos",
                "file_filter": ["HILIC", "POS"],
                "instrument_used": "Thermo Q Exactive",
                "mass_spec_configuration_name": "QE_HILIC_POS",
                "chromat_configuration_name": "HILIC"
            },
            {
                "name": "rp_neg",
                "file_filter": ["RP", "NEG"],
                "instrument_used": "Thermo Q Exactive",
                "mass_spec_configuration_name": "QE_RP_NEG",
                "chromat_configuration_name": "RP"
            }
        ],
        "minio": {
            "endpoint": "localhost:9000",
            "bucket": "test-bucket",
            "secure": False
        },
        "skip_triggers": {
            "protocol_outline_created": True,
        }
    }


@pytest.fixture
def gcms_config(temp_config_dir):
    """Comprehensive GCMS configuration for all GCMS unit tests.
    
    Includes Docker, metadata generation, and MASSIVE integration.
    Use this for testing raw data inspection, metadata generation, and biosample mapping.
    """
    return {
        "workflow": {
            "name": "test_gcms_workflow",
            "type": "gcms_metabolomics",
            "workflow_type": "GCMS Metabolomics",
            "massive_id": "MSV000095000",
            "file_type": ".cdf",
            "file_filters": ["GCMS"]
        },
        "study": {
            "id": "nmdc:sty-11-test",
            "name": "test_gcms_study",
            "description": "Test GCMS study for unit testing"
        },
        "paths": {
            "base_directory": str(temp_config_dir / "gcms_base"),
            "data_directory": str(temp_config_dir / "gcms_data")
        },
        "docker": {
            "raw_data_inspector_image": "microbiomedata/metams:3.3.3"
        },
        "metadata": {
            "processing_institution_workflow": "EMSL",
            "processing_institution_generation": "EMSL",
            "raw_data_location": "massive",
            "instrument_used": "Agilent 5977B",
            "mass_spec_configuration_name": "GCMS_DEFAULT",
            "chromat_configuration_name": "GC"
        },
        "configurations": [
            {
                "name": "gcms_default",
                "file_filter": ["GCMS"],
                "instrument_used": "Agilent 5977B",
                "mass_spec_configuration_name": "GCMS_METAB",
                "chromat_configuration_name": "GC"
            }
        ],
        "minio": {
            "endpoint": "localhost:9000",
            "bucket": "test-bucket",
            "secure": False
        },
        "skip_triggers": {
            "protocol_outline_created": True,
        }
    }


@pytest.fixture
def lcms_config_file(temp_config_dir, lcms_config):
    """Create config file from lcms_config fixture."""
    config_path = temp_config_dir / "lcms_config.json"
    with open(config_path, "w") as f:
        json.dump(lcms_config, f)
    return config_path


@pytest.fixture
def gcms_config_file(temp_config_dir, gcms_config):
    """Create config file from gcms_config fixture."""
    config_path = temp_config_dir / "gcms_config.json"
    with open(config_path, "w") as f:
        json.dump(gcms_config, f)
    return config_path


# Integration test fixtures (keep separate as they use real study IDs and specific paths)

@pytest.fixture
def integration_config(temp_config_dir):
    """Provide configuration for integration tests with real NMDC API and data.
    
    Uses real production study ID: nmdc:sty-11-dwsv7q78
    """
    return {
        "workflow": {
            "name": "integration_test_workflow",
            "type": "lcms_metabolomics",
            "workflow_type": "LCMS Metabolomics",
            "massive_id": "MSV000094090",
            "file_type": ".raw",
            "file_filters": ["HILICZ"]
        },
        "study": {
            "id": "nmdc:sty-11-dwsv7q78",  # Real production study ID
            "name": "kroeger_lcms_metab",
            "description": "Integration test with real study"
        },
        "paths": {
            "base_directory": str(temp_config_dir / "integration_base"),
            "data_directory": str(temp_config_dir / "integration_data")
        },
        "docker": {
            "raw_data_inspector_image": "microbiomedata/metams:3.3.3"
        },
        "skip_triggers": {
            "protocol_outline_created": True,
        }
    }


@pytest.fixture
def integration_config_file(temp_config_dir, integration_config):
    """Create config file for integration tests."""
    config_path = temp_config_dir / "integration_config.json"
    with open(config_path, "w") as f:
        json.dump(integration_config, f)
    return config_path
