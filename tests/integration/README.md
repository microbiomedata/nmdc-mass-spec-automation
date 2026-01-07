# Integration Tests

This directory contains integration tests that interact with real external services (NMDC API, MinIO, etc.).

## Running Integration Tests

### Run all integration tests:
```bash
pytest tests/integration/ -v
```

### Run specific integration test file:
```bash
pytest tests/integration/test_biosample_manager_integration.py -v -s
```

### Run in CI/CD:
- **Unit tests**: Run on every push to any branch
- **Integration tests**: Run on:
  - Pushes to `main` branch
  - Pull requests to `main`
  - Scheduled nightly runs (2 AM UTC)

## Characteristics

- **Slower**: These tests make real network calls and may take several seconds
- **Network-dependent**: Require internet connection to reach external APIs
- **May be flaky**: External service downtime can cause test failures
- **Use real data**: Tests interact with production NMDC study data

## Markers

Integration tests can use these pytest markers:
- `@pytest.mark.network` - Requires network access
- `@pytest.mark.slow` - Takes significant time to run

## Adding New Integration Tests

When adding integration tests:
1. Use the `integration_config_file` fixture for consistent test configuration
2. Add `@pytest.mark.network` to tests requiring external APIs
3. Include clear error messages that distinguish API failures from code bugs
4. Use try/except with `pytest.fail()` for meaningful error reporting

Example:
```python
@pytest.mark.network
def test_my_integration(integration_config_file):
    """Test description with real API."""
    try:
        # Test logic
        assert result is True
    except Exception as e:
        pytest.fail(f"Integration failed: {e}")
```
