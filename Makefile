.PHONY: help test test-unit test-integration download-test-data clean-test-data

# Python and pytest commands (use venv if available, otherwise system)
PYTHON := $(shell if [ -f venv/bin/python ]; then echo venv/bin/python; else echo python; fi)
PYTEST := $(PYTHON) -m pytest

# Default target
help:
	@echo "Available targets:"
	@echo "  test                 - Run all tests (unit + integration)"
	@echo "  test-unit            - Run unit tests only"
	@echo "  test-integration     - Run integration tests (requires Docker and network)"
	@echo "  download-test-data   - Download required test data files"
	@echo "  clean-test-data      - Remove downloaded test data"
	@echo "  clean                - Clean all generated files"

# Run all tests
test: test-unit test-integration

# Run unit tests only
test-unit:
	$(PYTEST) tests/ --ignore=tests/integration/ -v

# Run integration tests (downloads test data if needed)
test-integration: download-test-data
	$(PYTEST) tests/integration/ -v -s

# Download required test data for integration tests
download-test-data:
	@echo "Downloading test data for integration tests..."
	@mkdir -p tests/integration/test_data
	@if [ ! -f tests/integration/test_data/20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490_NEG_MSMS_19_S16-D89_A_Rg70to1050-CE102040-soil-S1_Run84.raw ]; then \
		echo "Downloading LCMS .raw file from MASSIVE (~95MB)..."; \
		curl -L -o tests/integration/test_data/20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490_NEG_MSMS_19_S16-D89_A_Rg70to1050-CE102040-soil-S1_Run84.raw \
		"https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f.MSV000094090%2Fraw%2F20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490%2Frawdata%2F20210819_JGI-AK_MK_506588_SoilWaterRep_final_QE-139_HILICZ_USHXG01490_NEG_MSMS_19_S16-D89_A_Rg70to1050-CE102040-soil-S1_Run84.raw&forceDownload=true"; \
		echo "LCMS file download complete!"; \
	else \
		echo "LCMS test data already exists."; \
	fi
	@if [ ! -f tests/integration/test_data/GCMS_FAMEs_01_GCMS01_20180115.cdf ]; then \
		echo "Downloading GCMS .cdf file from NMDC (~6.5MB)..."; \
		curl -L -o tests/integration/test_data/GCMS_FAMEs_01_GCMS01_20180115.cdf \
		"https://nmdcdemo.emsl.pnnl.gov/metabolomics/blanchard_11_8ws97026/raw/GCMS_FAMEs_01_GCMS01_20180115.cdf"; \
		echo "GCMS file download complete!"; \
	else \
		echo "GCMS test data already exists."; \
	fi

# Remove downloaded test data
clean-test-data:
	@echo "Removing test data..."
	rm -rf tests/integration/test_data/
	@echo "Test data removed."

# Clean all generated files
clean: clean-test-data
	@echo "Cleaning generated files..."
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf **/__pycache__/
	rm -f .coverage
	@echo "Clean complete."
