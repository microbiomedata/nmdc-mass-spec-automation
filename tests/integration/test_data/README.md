# Integration Test Data

This directory contains test data files used by integration tests. These files are **not** stored in the repository due to their large size.

## Test Files

### LCMS Raw MS Data File
- **File**: `20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490_NEG_MSMS_19_S16-D89_A_Rg70to1050-CE102040-soil-S1_Run84.raw`
- **Size**: ~95 MB
- **Source**: MASSIVE dataset MSV000094090
- **MD5**: `b75d46305e8459bc7c81ba1b2b17d63b`
- **Used by**: `test_raw_data_inspection_manager_integration.py` (LCMS tests)

### GCMS Data File
- **File**: `GCMS_FAMEs_01_GCMS01_20180115.cdf`
- **Size**: ~6.5 MB
- **Source**: NMDC example GCMS dataset (blanchard_11_8ws97026)
- **MD5**: `d27124d36d3db9e19161e7fc81ce176b`
- **Used by**: `test_raw_data_inspection_manager_integration.py` (GCMS tests)

## Downloading Test Data

### Automatic Download (Recommended)
Integration tests will automatically download required files on first run using a session-scoped pytest fixture. The files are cached here for subsequent test runs.

### Manual Download via Makefile
```bash
# Download all required test data
make download-test-data

# Clean downloaded test data
make clean-test-data
```

### Direct Download
```bash
mkdir -p tests/integration/test_data

# LCMS .raw file
curl -L -o tests/integration/test_data/20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490_NEG_MSMS_19_S16-D89_A_Rg70to1050-CE102040-soil-S1_Run84.raw \
  "https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f.MSV000094090%2Fraw%2F20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490%2Frawdata%2F20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490_NEG_MSMS_19_S16-D89_A_Rg70to1050-CE102040-soil-S1_Run84.raw&forceDownload=true"

# GCMS .cdf file
curl -L -o tests/integration/test_data/GCMS_FAMEs_01_GCMS01_20180115.cdf \
  "https://nmdcdemo.emsl.pnnl.gov/metabolomics/blanchard_11_8ws97026/raw/GCMS_FAMEs_01_GCMS01_20180115.cdf"
```

## CI/CD Integration

In GitHub Actions, test data is downloaded before running integration tests:

```yaml
- name: Download test data
  run: make download-test-data

- name: Run integration tests
  run: pytest tests/integration/ -v -s
```

## File Verification

The pytest fixture automatically verifies downloaded files using MD5 checksums to ensure data integrity.
