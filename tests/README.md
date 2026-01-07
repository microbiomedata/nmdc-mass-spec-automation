# Tests

This directory contains the test suite for the NMDC Mass Spec Automation tools.

## Test Structure

```
tests/
├── conftest.py                                      # Shared fixtures and pytest configuration
├── test_workflow_manager.py                         # Unit tests for core workflow manager
├── test_biosample_manager.py                        # Unit tests for biosample manager mixin
├── test_raw_data_inspection_manager.py              # Unit tests for raw data inspection mixin
└── integration/                                     # Integration tests (separate directory)
    ├── README.md                                    # Integration test documentation
    ├── conftest.py                                  # Integration-specific configuration
    ├── test_biosample_manager_integration.py        # Integration tests for biosample manager
    ├── test_raw_data_inspection_manager_integration.py  # Integration tests for raw data inspection
    └── test_data/                                   # Downloaded test data (gitignored)
        └── README.md                                # Test data documentation
```

## Test Types

### Unit Tests (`tests/*.py`)
- Fast, isolated tests with mocked external dependencies
- Run on every commit via GitHub Actions
- Use mocks for API calls, file I/O, and external services

### Integration Tests (`tests/integration/`)
- Slower tests that interact with real external services
- Run on main branch, PRs to main, and nightly schedule
- Verify end-to-end functionality with real NMDC API, etc.

## Running Tests Locally

### Prerequisites

Ensure pytest is installed:

```bash
pip install pytest pytest-cov
```

### Run All Unit Tests (Recommended for Development)

```bash
pytest tests/ --ignore=tests/integration/ -v
# Or using Makefile:
make test-unit
```

### Run All Tests (Unit + Integration)

```bash
pytest tests/ -v
# Or using Makefile:
make test
```

### Run Integration Tests Only

**Note**: Integration tests require large test data files (~95MB). These are automatically downloaded on first run and cached locally.

```bash
pytest tests/integration/ -v -s
# Or using Makefile (downloads test data first):
make test-integration
```

### Run Specific Test File

```bash
pytest tests/test_workflow_manager.py -v
```

### Run Specific Test

```bash
pytest tests/test_workflow_manager.py::TestNMDCWorkflowManager::test_initialization -v
```

### Run with Coverage Report

```bash
pytest tests/ --ignore=tests/integration/ --cov=nmdc_dp_utils --cov-report=html
```

This will generate a coverage report in `htmlcov/index.html`.

## Common pytest Options

- `-v` or `--verbose`: Verbose output showing each test
- `-s`: Show print statements (don't capture stdout)
- `-x`: Stop on first failure
- `--tb=short`: Shorter traceback format
- `-k EXPRESSION`: Run tests matching the expression (e.g., `pytest -k "biosample"`)
- `-m MARKER`: Run tests with specific marker (e.g., `pytest -m "not slow"`)
- `--lf`: Run last failed tests
- `--ff`: Run failures first, then the rest
- `--ignore=DIR`: Ignore directory (e.g., `--ignore=tests/integration/`)
- `--collect-only`: Show what tests would be run without executing them

## Test Data Management

Integration tests use large data files that are not stored in the repository. The test framework provides several ways to manage this data:

### Automatic Download (Default)
The pytest fixture `integration_test_raw_file` automatically downloads required files on first run and caches them in `tests/integration/test_data/`. Files are verified using MD5 checksums.

### Manual Download via Makefile
```bash
# Download all required test data
make download-test-data

# Clean downloaded test data
make clean-test-data
```

### Direct Download
See `tests/integration/test_data/README.md` for direct download commands.

## CI/CD

GitHub Actions runs tests automatically:
- **Unit tests**: Every PR to main (Python 3.10, 3.11, 3.12)
- **Integration tests**: Main branch, PRs to main, and nightly at 2 AM UTC
  - Test data is downloaded automatically via `make download-test-data`

See [../.github/workflows/tests.yml](../.github/workflows/tests.yml) for configuration.

## Writing Tests

### Unit Tests
- Use fixtures from `conftest.py` (e.g., `config_file`, `minimal_config`)
- Mock external dependencies with `@patch` decorators
- Keep tests fast and isolated
- Place in `tests/` directory

### Integration Tests
- Place in `tests/integration/` directory
- Use `integration_config_file` fixture for real study data
- Mark with `@pytest.mark.network` if requiring internet
- Include clear error messages distinguishing API vs code issues

Example:
```python
@pytest.mark.network
def test_real_api_call(integration_config_file):
    """Test description."""
    try:
        # test logic
        assert result is True
    except Exception as e:
        pytest.fail(f"Test failed: {e}")
```

