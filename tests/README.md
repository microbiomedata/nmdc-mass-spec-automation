# Tests

Test suite for NMDC Mass Spectrometry automation workflows. Tests are organized by scope (unit vs integration) and cover the mixin-based workflow manager architecture.

## Test Organization

```
tests/
├── conftest.py                      # Shared fixtures (configs, managers, mocks)
├── test_*.py                        # Unit tests per mixin/component
└── integration/
    ├── conftest.py                  # Integration-specific fixtures
    ├── test_*_integration.py        # End-to-end workflow tests
    └── test_data/                   # Large test files (auto-downloaded)
```

## Testing Philosophy

**Unit Tests** - Fast, isolated validation of individual methods:
- Mock external dependencies (FTP, APIs, Docker, file I/O)
- Test configuration handling and routing logic
- Verify error handling and edge cases
- Target: <5s total runtime for rapid development feedback

**Integration Tests** - Real-world workflow validation:
- Interact with actual MASSIVE FTP, Docker containers, and test datasets
- Validate end-to-end data movement and processing
- Network-dependent, marked with `@pytest.mark.integration`
- Auto-download required test data on first run

## Quick Start

```bash
# Unit tests only (fast, recommended for development)
make test-unit

# All tests (unit + integration, downloads test data)
make test

# Integration tests only
make test-integration

# Coverage report (generates htmlcov/index.html)
make test-coverage
```

## Running Specific Tests

```bash
# Single test file
pytest tests/test_biosample_manager.py -v

# Specific test class or method
pytest tests/test_biosample_manager.py::TestBiosampleManager::test_fetch_attributes -v

# By keyword pattern
pytest -k "biosample" -v

# Stop on first failure
pytest -x
```

## Writing New Tests

### Unit Test Pattern

Test individual class methods with mocked dependencies:

```python
from unittest.mock import patch, MagicMock

class TestMyMixin:
    def test_method_behavior(self, lcms_config_file):
        """Test that method handles expected input correctly."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        manager = NMDCWorkflowManager(str(lcms_config_file))
        
        with patch.object(manager, 'external_dependency') as mock:
            mock.return_value = expected_result
            result = manager.my_method()
            assert result == expected_output
```

**Guidelines:**
- One test file per class (`test_<mixin_name>.py`)
- Mixin classes tested in separation from full workflow manager
- Use fixtures from `conftest.py` for consistent configs
- Mock at the boundary (FTP connections, API calls, file I/O, Docker)
- Test configuration validation and error handling

### Integration Test Pattern

Test end-to-end workflows with real external services:

```python
@pytest.mark.integration
@pytest.mark.network  # If requires internet
class TestMyWorkflow:
    def test_real_workflow(self, tmp_path, gcms_config):
        """Test complete workflow with real MASSIVE FTP crawl."""
        from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager
        
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(gcms_config))
        
        manager = NMDCWorkflowManager(str(config_file))
        manager.create_workflow_structure()
        
        result = manager.real_external_operation()
        assert result is True
```

**Guidelines:**
- Mark with `@pytest.mark.integration` and `@pytest.mark.network` if applicable
- Use real services but limit to small datasets/operations
- Expect occasional failures due to external service issues
- Provide clear failure messages distinguishing service vs code issues

## Fixtures Reference

Common fixtures from `conftest.py`:
- `lcms_config`, `gcms_config` - Pre-configured study configs
- `lcms_config_file`, `gcms_config_file` - Config files in temp directory
- `tmp_path` - Pytest's temporary directory fixture

See [conftest.py](conftest.py) for complete fixture list.

## CI/CD

GitHub Actions runs on every PR or manual trigger:
- **Unit tests**: Python 3.11 and 3.12, ~5s runtime
- **Integration tests**: Python 3.12 only, pulls Docker images, downloads test data, 15min timeout

See [.github/workflows/tests.yml](../.github/workflows/tests.yml) for configuration.

