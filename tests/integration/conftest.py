"""
Pytest configuration for integration tests.

Integration-specific fixtures and configuration.
"""

import pytest
from pathlib import Path
import requests
import hashlib
import os


# Integration tests can be marked with @pytest.mark.slow or @pytest.mark.network
# for finer-grained control in CI/CD

def pytest_configure(config):
    """Register integration-specific markers and ensure Docker is in PATH."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "network: marks tests as requiring network access"
    )
    
    # Ensure /usr/local/bin is in PATH for Docker (macOS/Linux)
    current_path = os.environ.get('PATH', '')
    if '/usr/local/bin' not in current_path:
        os.environ['PATH'] = f"/usr/local/bin:{current_path}"


@pytest.fixture(scope="session")
def integration_test_raw_file():
    """
    Download and cache a real .raw file for integration testing.
    
    Downloads once per test session and caches in tests/integration/test_data/.
    The file is ~95MB and comes from MASSIVE dataset MSV000094090.
    
    Returns:
        Path: Path to the downloaded raw file
    """
    # Define test data directory
    test_data_dir = Path(__file__).parent / "test_data"
    test_data_dir.mkdir(exist_ok=True)
    
    # Define file details
    raw_file_name = "20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490_NEG_MSMS_19_S16-D89_A_Rg70to1050-CE102040-soil-S1_Run84.raw"
    raw_file_path = test_data_dir / raw_file_name
    expected_md5 = "b75d46305e8459bc7c81ba1b2b17d63b"  # From metadata
    
    # Check if file already exists with correct checksum
    if raw_file_path.exists():
        # Verify checksum
        md5_hash = hashlib.md5()
        with open(raw_file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                md5_hash.update(chunk)
        
        if md5_hash.hexdigest() == expected_md5:
            print(f"\n✓ Using cached test file: {raw_file_path.name} ({raw_file_path.stat().st_size / (1024*1024):.1f} MB)")
            return raw_file_path
        else:
            print(f"\n⚠ Cached file has incorrect checksum, re-downloading...")
            raw_file_path.unlink()
    
    # Download the file
    raw_file_url = (
        "https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?"
        "file=f.MSV000094090%2Fraw%2F20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490%2F"
        "rawdata%2F20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490_NEG_MSMS_19_S16-D89_A_Rg70to1050-CE102040-soil-S1_Run84.raw"
        "&forceDownload=true"
    )
    
    print(f"\nDownloading integration test file from MASSIVE...")
    print(f"Target: {raw_file_path}")
    print(f"Size: ~95 MB - this may take a few minutes...")
    
    try:
        response = requests.get(raw_file_url, stream=True, timeout=300)
        response.raise_for_status()
        
        # Write file in chunks with progress
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192
        downloaded = 0
        
        with open(raw_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=block_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        print(f"\rDownload progress: {percent:.1f}%", end='', flush=True)
        
        print(f"\n✓ Download complete: {raw_file_path.stat().st_size / (1024*1024):.1f} MB")
        
        # Verify checksum
        md5_hash = hashlib.md5()
        with open(raw_file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                md5_hash.update(chunk)
        
        if md5_hash.hexdigest() != expected_md5:
            raise ValueError(f"Downloaded file has incorrect MD5 checksum")
        
        print(f"✓ Checksum verified")
        return raw_file_path
        
    except Exception as e:
        if raw_file_path.exists():
            raw_file_path.unlink()
        raise RuntimeError(f"Failed to download test file: {e}") from e


@pytest.fixture(scope="session")
def integration_test_gcms_file():
    """
    Download and cache a real .cdf file for GCMS integration testing.
    
    Downloads once per test session and caches in tests/integration/test_data/.
    The file is ~6.5MB and comes from NMDC example GCMS dataset.
    
    Returns:
        Path: Path to the downloaded CDF file
    """
    # Define test data directory
    test_data_dir = Path(__file__).parent / "test_data"
    test_data_dir.mkdir(exist_ok=True)
    
    # Define file details
    cdf_file_name = "GCMS_FAMEs_01_GCMS01_20180115.cdf"
    cdf_file_path = test_data_dir / cdf_file_name
    expected_md5 = "d27124d36d3db9e19161e7fc81ce176b"  # From metadata
    
    # Check if file already exists with correct checksum
    if cdf_file_path.exists():
        # Verify checksum
        md5_hash = hashlib.md5()
        with open(cdf_file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                md5_hash.update(chunk)
        
        if md5_hash.hexdigest() == expected_md5:
            print(f"\n✓ Using cached GCMS test file: {cdf_file_path.name} ({cdf_file_path.stat().st_size / (1024*1024):.1f} MB)")
            return cdf_file_path
        else:
            print(f"\n⚠ Cached file has incorrect checksum, re-downloading...")
            cdf_file_path.unlink()
    
    # Download the file
    cdf_file_url = "https://nmdcdemo.emsl.pnnl.gov/metabolomics/blanchard_11_8ws97026/raw/GCMS_FAMEs_01_GCMS01_20180115.cdf"
    
    print(f"\nDownloading GCMS integration test file...")
    print(f"Target: {cdf_file_path}")
    print(f"Size: ~6.5 MB")
    
    try:
        response = requests.get(cdf_file_url, stream=True, timeout=120)
        response.raise_for_status()
        
        # Write file in chunks with progress
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192
        downloaded = 0
        
        with open(cdf_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=block_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        print(f"\rDownload progress: {percent:.1f}%", end='', flush=True)
        
        print(f"\n✓ Download complete: {cdf_file_path.stat().st_size / (1024*1024):.1f} MB")
        
        # Verify checksum
        md5_hash = hashlib.md5()
        with open(cdf_file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                md5_hash.update(chunk)
        
        if md5_hash.hexdigest() != expected_md5:
            raise ValueError(f"Downloaded file has incorrect MD5 checksum")
        
        print(f"✓ Checksum verified")
        return cdf_file_path
        
    except Exception as e:
        if cdf_file_path.exists():
            cdf_file_path.unlink()
        raise RuntimeError(f"Failed to download GCMS test file: {e}") from e


@pytest.fixture
def integration_lcms_config(tmp_path):
    """
    Provides a realistic LCMS configuration for integration testing.
    
    This configuration mirrors real NMDC LCMS workflows with:
    - Multiple chromatography configurations (RP positive/negative)
    - Metadata overrides for collision energy patterns
    - MinIO for local testing (no network dependency)
    - Realistic instrument and processing metadata
    - Full metadata package generation support
    
    Args:
        tmp_path: pytest tmp_path fixture
        
    Returns:
        dict: Complete LCMS workflow configuration
    """
    return {
        "study": {
            "name": "test_lcms_study",
            "id": "nmdc:sty-11-test-lcms",
            "description": "Test LCMS study for integration testing"
        },
        "workflow": {
            "name": "test_lcms_workflow",
            "massive_id": "MSV000094090",
            "file_type": ".raw",
            "workflow_type": "LCMS Lipidomics",
            "processed_data_date_tag": "20250108"
        },
        "paths": {
            "base_directory": str(tmp_path),
            "data_directory": str(tmp_path / "data")
        },
        "minio": {
            "endpoint": "admin.nmdcdemo.emsl.pnl.gov",
            "secure": True,
            "bucket": "metabolomics",
            "public_url_base": "https://nmdcdemo.emsl.pnnl.gov"
        },
        "configurations": [
            {
                "name": "rp_pos",
                "file_filter": ["C18", "_POS_"],
                "corems_toml": "workflow_inputs/metams_rp_corems.toml",
                "reference_db": "tests/integration/test_data/test_database.msp",
                "scan_translator": "workflow_inputs/metams_jgi_scan_translator.toml",
                "cores": 1,
                "chromat_configuration_name": "JGI/LBNL Metabolomics Standard LC Method - Nonpolar C18",
                "mass_spec_configuration_name": "JGI/LBNL Standard Metabolomics Method, positive",
                "metadata_overrides": {
                    "mass_spec_configuration_name": {
                        "CE102040": "JGI/LBNL Standard Metabolomics Method, positive @10,20,40CE",
                        "CE205060": "JGI/LBNL Standard Metabolomics Method, positive @20,50,60CE"
                    }
                }
            },
            {
                "name": "rp_neg",
                "file_filter": ["C18", "_NEG_"],
                "corems_toml": "workflow_inputs/metams_rp_corems.toml",
                "reference_db": "tests/integration/test_data/test_database.msp",
                "scan_translator": "workflow_inputs/metams_jgi_scan_translator.toml",
                "cores": 1,
                "chromat_configuration_name": "JGI/LBNL Metabolomics Standard LC Method - Nonpolar C18",
                "mass_spec_configuration_name": "JGI/LBNL Standard Metabolomics Method, negative",
                "metadata_overrides": {
                    "mass_spec_configuration_name": {
                        "CE102040": "JGI/LBNL Standard Metabolomics Method, negative @10,20,40CE",
                        "CE205060": "JGI/LBNL Standard Metabolomics Method, negative @20,50,60CE"
                    }
                }
            }
        ],
        "metadata": {
            "instrument_used": "Thermo Orbitrap Q-Exactive",
            "processing_institution_workflow": "NMDC",
            "processing_institution_generation": "JGI",
            "raw_data_location": "minio",
            "existing_data_objects": []
        },
        "skip_triggers": {
            "metadata_mapping_generated": False,
            "metadata_packages_generated": False
        }
    }


@pytest.fixture
def integration_gcms_config(tmp_path):
    """
    Provides a realistic GCMS configuration for integration testing.
    
    This configuration mirrors real NMDC GCMS workflows with:
    - Single chromatography configuration (typical for GCMS)
    - Calibration file handling
    - MinIO for local testing (no network dependency)
    - Realistic instrument and processing metadata
    - Full metadata package generation support
    
    Args:
        tmp_path: pytest tmp_path fixture
        
    Returns:
        dict: Complete GCMS workflow configuration
    """
    return {
        "study": {
            "name": "test_gcms_study",
            "id": "nmdc:sty-11-test-gcms",
            "description": "Test GCMS study for integration testing"
        },
        "workflow": {
            "name": "test_gcms_workflow",
            "file_type": ".cdf",
            "workflow_type": "GCMS Metabolomics",
            "processed_data_date_tag": "20250108"
        },
        "paths": {
            "base_directory": str(tmp_path),
            "data_directory": str(tmp_path / "data")
        },
        "minio": {
            "endpoint": "admin.nmdcdemo.emsl.pnl.gov",
            "secure": True,
            "bucket": "metabolomics",
            "public_url_base": "https://nmdcdemo.emsl.pnnl.gov"
        },
        "configurations": [
            {
                "name": "gcms",
                "corems_toml": "workflow_inputs/metams_gcms_corems.toml",
                "cores": 1,
                "chromat_configuration_name": "EMSL_Agilent_GC",
                "mass_spec_configuration_name": "Agilent_5977_MSD"
            }
        ],
        "metadata": {
            "instrument_used": "Agilent 7980A GC-MS",
            "processing_institution_workflow": "NMDC",
            "processing_institution_generation": "EMSL",
            "raw_data_location": "minio",
            "configuration_file_name": "test_gcms_corems_params.toml"
        },
        "skip_triggers": {
            "metadata_mapping_generated": False,
            "metadata_packages_generated": False
        }
    }
