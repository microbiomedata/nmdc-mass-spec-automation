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

# Add project root to sys.path so nmdc_dp_utils can be imported
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (may be slow, require network)"
    )


@pytest.fixture
def temp_config_dir():
    """Create a temporary directory for test configs."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def minimal_config(temp_config_dir):
    """Provide a minimal valid configuration for testing."""
    # Use temp_config_dir to make paths unique per test
    return {
        "workflow": {
            "name": "test_workflow",
            "type": "gcms_metabolomics",
            "workflow_type": "GCMS Metabolomics",
            "massive_id": "MSV000012345",
            "file_type": ".raw",
            "file_filters": ["GCMS"]
        },
        "study": {
            "name": "test_study",
            "id": "nmdc:sty-11-test123",
            "description": "Test study for unit testing"
        },
        "paths": {
            "base_directory": str(temp_config_dir / "test_base"),
            "data_directory": str(temp_config_dir / "test_data")
        },
        "minio": {
            "endpoint": "localhost:9000",
            "bucket": "test-bucket",
            "secure": False
        },
        "configurations": [
            {
                "name": "test_config",
                "file_filter": ["TEST"],
                "metadata_overrides": {}
            }
        ]
    }


@pytest.fixture
def integration_config(temp_config_dir):
    """Provide configuration for integration tests with real NMDC API, data etc.
    
    Uses study ID from production: nmdc:sty-11-dwsv7q78
    """
    return {
        "workflow": {
            "name": "integration_test_workflow",
            "type": "lcms_metabolomics",
            "workflow_type": "LCMS Metabolomics",
            "massive_id": "MSV000012345",
            "file_type": ".raw",
            "file_filters": ["LCMS"]
        },
        "study": {
            "id": "nmdc:sty-11-dwsv7q78",  # Real production study ID
            "name": "kroeger_lcms_metab",
            "description": "Integration test with real study"
        },
        "paths": {
            "base_directory": str(temp_config_dir / "test_base"),
            "data_directory": str(temp_config_dir / "test_data")
        }
    }


@pytest.fixture
def config_file(temp_config_dir, minimal_config):
    """Create a temporary config file for testing."""
    config_path = temp_config_dir / "test_config.json"
    with open(config_path, "w") as f:
        json.dump(minimal_config, f)
    return config_path


@pytest.fixture
def config_file_with_docker(temp_config_dir, minimal_config):
    """Create a temporary config file with Docker configuration."""
    config = minimal_config.copy()
    config["docker"] = {
        "raw_data_inspector_image": "microbiomedata/metams:3.3.3"
    }
    config["workflow"]["workflow_type"] = "LCMS Metabolomics"
    config_path = temp_config_dir / "test_config_docker.json"
    with open(config_path, "w") as f:
        json.dump(config, f)
    return config_path


@pytest.fixture
def gcms_config_file_with_docker(temp_config_dir, minimal_config):
    """Create a temporary config file for GCMS workflow with Docker configuration."""
    config = minimal_config.copy()
    config["docker"] = {
        "raw_data_inspector_image": "microbiomedata/metams:3.3.3"
    }
    config["workflow"]["workflow_type"] = "GCMS Metabolomics"
    config["workflow"]["file_type"] = ".cdf"
    config_path = temp_config_dir / "test_gcms_config_docker.json"
    with open(config_path, "w") as f:
        json.dump(config, f)
    return config_path


@pytest.fixture
def integration_config_file(temp_config_dir, integration_config):
    """Create a temporary config file for integration testing with real API."""
    config_path = temp_config_dir / "integration_config.json"
    with open(config_path, "w") as f:
        json.dump(integration_config, f)
    return config_path


@pytest.fixture
def integration_raw_data_config(temp_config_dir):
    """Provide configuration for integration tests with raw data inspection.
    
    Uses LCMS workflow type for .raw file inspection.
    """
    return {
        "workflow": {
            "name": "integration_raw_data_test",
            "type": "lcms_metabolomics",
            "workflow_type": "LCMS Metabolomics",
            "massive_id": "MSV000094090",
            "file_type": ".raw",
            "file_filters": ["HILICZ"]
        },
        "study": {
            "id": "nmdc:sty-11-dwsv7q78",
            "name": "kroeger_lcms_metab_raw_inspection",
            "description": "Integration test for raw data inspection"
        },
        "paths": {
            "base_directory": str(temp_config_dir / "raw_inspection_base"),
            "data_directory": str(temp_config_dir / "raw_inspection_data")
        },
        "docker": {
            "raw_data_inspector_image": "microbiomedata/metams:3.3.3"
        }
    }


@pytest.fixture
def integration_raw_data_config_file(temp_config_dir, integration_raw_data_config):
    """Create a temporary config file for integration testing with raw data."""
    config_path = temp_config_dir / "integration_raw_data_config.json"
    with open(config_path, "w") as f:
        json.dump(integration_raw_data_config, f)
    return config_path
