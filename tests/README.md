# Tests

This directory contains unit tests for the NMDC Mass Spec Automation project.

## Running Tests Locally

### Prerequisites

Ensure pytest is installed:

```bash
pip install pytest
```

### Run All Tests

From the project root directory:

```bash
pytest tests/
```

### Run Specific Test File

```bash
pytest tests/test_basic.py
```

### Run with Verbose Output

```bash
pytest tests/ -v
```

### Run with Coverage Report

First install pytest-cov:

```bash
pip install pytest-cov
```

Then run tests with coverage:

```bash
pytest tests/ --cov=nmdc_dp_utils --cov-report=html
```

This will generate a coverage report in `htmlcov/index.html`.

### Common pytest Options

- `-v` or `--verbose`: Verbose output showing each test
- `-s`: Show print statements (don't capture stdout)
- `-x`: Stop on first failure
- `-k EXPRESSION`: Run tests matching the expression (e.g., `pytest -k "test_simple"`)
- `--collect-only`: Show what tests would be run without executing them

## Test Organization

- `test_basic.py`: Basic tests to verify pytest is working
- Additional test files should follow the naming pattern `test_*.py`
