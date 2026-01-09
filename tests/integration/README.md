# Integration Tests

End-to-end tests validating workflows with real external services and data files.

## Purpose

Integration tests verify:
- **MASSIVE FTP operations** - Crawling, parsing, file discovery on real FTP server
- **Docker-based processing** - Raw data inspection with actual MS files
- **Workflow routing** - Configuration-driven data source detection
- **Data movement** - Complete fetch/download workflows

## Characteristics

- **Network-dependent**: Require internet for MASSIVE FTP, NMDC API access
- **Data-dependent**: Auto-download test files (~100MB total) on first run
- **Slower execution**: 20-60s runtime due to FTP crawling and Docker operations
- **Potentially flaky**: External service downtime may cause transient failures

## Running

```bash
# All integration tests (auto-downloads test data)
make test-integration

# Specific test file
pytest tests/integration/test_data_movement_manager_integration.py -v -s

# Skip slow tests
pytest tests/integration/ -m "not slow" -v
```

## Writing Integration Tests

**Use real services but limit scope:**

```python
@pytest.mark.integration
@pytest.mark.network
class TestMyWorkflow:
    def test_small_real_operation(self, tmp_path, gcms_config):
        """Test with real FTP but limited to small dataset."""
        # Use known small datasets or limit file counts
        # Expect occasional failures - include helpful messages
```

**Mark appropriately:**
- `@pytest.mark.integration` - All tests in this directory
- `@pytest.mark.network` - Requires internet connection
- `@pytest.mark.slow` - Takes >10s (allows selective skipping)

## Test Data

Large MS files are auto-downloaded on first run and cached in `test_data/`:
- LCMS .raw file (~95MB) for LCMS workflows
- GCMS .cdf file (~6.5MB) for GCMS workflows

See [test_data/README.md](test_data/README.md) for details.
