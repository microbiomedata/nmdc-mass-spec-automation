"""
Pytest configuration for integration tests.

Integration-specific fixtures and configuration.
"""

import pytest

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
