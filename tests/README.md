# Tests

This directory contains the test suite for the NMDC Mass Spec Automation tools.

## Test Structure

```
tests/
├── conftest.py                    # Shared fixtures and pytest configuration
├── test_workflow_manager.py       # Unit tests for core workflow manager
├── test_biosample_manager.py      # Unit tests for biosample manager mixin
└── integration/                   # Integration tests (separate directory)
    ├── README.md                  # Integration test documentation
    ├── conftest.py                # Integration-specific configuration
    └── test_biosample_manager_integration.py
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
```

### Run All Tests (Unit + Integration)

```bash
pytest tests/ -v
```

### Run Integration Tests Only

```bash
pytest tests/integration/ -v -s
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
- `--lf`: Run last failed tests
- `--ff`: Run failures first, then the rest
- `--ignore=DIR`: Ignore directory (e.g., `--ignore=tests/integration/`)
- `--collect-only`: Show what tests would be run without executing them

## CI/CD

GitHub Actions runs tests automatically:
- **Unit tests**: Every push to any branch (Python 3.10, 3.11, 3.12)
- **Integration tests**: Main branch, PRs to main, and nightly at 2 AM UTC

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

