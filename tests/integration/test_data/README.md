# Integration Test Data

Large MS data files for integration testing. Auto-downloaded on first test run, cached locally, gitignored.

## Files

| File | Size | Source | Purpose |
|------|------|--------|----------|
| `20210819_JGI-AK_..._Run84.raw` | ~95 MB | MASSIVE MSV000094090 | LCMS raw data inspection |
| `GCMS_FAMEs_01_GCMS01_20180115.cdf` | ~6.5 MB | NMDC blanchard_11_8ws97026 | GCMS data inspection |

## Usage

**Automatic** (recommended): Files download automatically on first `make test-integration` run.

**Manual download**:
```bash
make download-test-data  # Download all
make clean-test-data     # Remove all
```

**Direct URLs** (see [Makefile](../../../Makefile) for curl commands).

## Directory Structure

```
test_data/
├── *.raw, *.cdf           # Large MS data files (downloaded)
├── test_database.msp      # Reference spectral database
├── metadata/              # Biosample mapping CSVs for tests
│   ├── gcms_biosample_mapping.csv
│   ├── lcms_biosample_mapping.csv
│   └── massive_biosample_mapping.csv
└── raw_file_info/         # Expected inspection outputs for validation
    ├── gcms_inspection_results.csv
    ├── lcms_inspection_results.csv
    ├── massive_inspection_results.csv
    └── massive_ftp_*.csv
```

**metadata/** - Pre-generated biosample mappings used by integration tests to validate mapping logic without NMDC API calls.

**raw_file_info/** - Expected outputs from raw data inspection, used to verify Docker-based inspection produces correct results.

## Adding New Test Files

1. Add download logic to `Makefile` `download-test-data` target
2. Update the files table with details
3. Reference in integration test fixtures (see `conftest.py`)
4. Keep files small (<100MB) when possible

```yaml
- name: Download test data
  run: make download-test-data

- name: Run integration tests
  run: pytest tests/integration/ -v -s
```

## File Verification

The pytest fixture automatically verifies downloaded files using MD5 checksums to ensure data integrity.
