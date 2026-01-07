"""
Pytest configuration for integration tests.

Integration-specific fixtures and configuration.
"""

import pytest
from pathlib import Path
import requests
import hashlib


# Integration tests can be marked with @pytest.mark.slow or @pytest.mark.network
# for finer-grained control in CI/CD

def pytest_configure(config):
    """Register integration-specific markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "network: marks tests as requiring network access"
    )


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
