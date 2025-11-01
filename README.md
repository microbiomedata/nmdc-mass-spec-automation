# NMDC Data Processing Utilities

This repository contains utilities for managing NMDC (National Microbiome Data Collaborative) metabolomics studies, including automated data discovery, processing workflows, and quality control analysis.

## Features

### Core Workflow Management
- **Study Structure Setup**: Automated directory structure creation
- **Raw Data Discovery**: MASSIVE dataset integration and download
- **Biosample Mapping**: Intelligent mapping of raw files to NMDC biosamples
- **Self-Contained WDL Workflows**: GitHub-integrated execution within study directories
- **MinIO Integration**: Cloud storage management for processed data

### Quality Control & Analysis
- **Raw Data Inspector**: Comprehensive metadata extraction from MS files (NEW!)
- **File Filtering**: Process only biosample-mapped files for efficiency
- **Error Tracking**: Detailed logging and error management
- **Progress Monitoring**: Real-time processing status and statistics

## Quick Start

### 1. Basic Study Setup
```python
from nmdc_dp_utils.study_manager import NMDCStudyManager

# Initialize with your study configuration
study_manager = NMDCStudyManager("path/to/your/config.json")

# Run complete workflow
study_manager.run_biosample_mapping_script()
study_manager.generate_wdl_jsons()
```

### 2. Raw Data Quality Control (NEW!)
```python
# Extract comprehensive metadata from raw files
inspection_results = study_manager.raw_data_inspector(
    cores=4,     # Parallel processing
    limit=None   # Process all files
)

# Results include instrument info, scan parameters, data ranges, etc.
```

### 3. Filtered Processing
```python
# Generate list of only biosample-mapped files
study_manager._generate_mapped_files_list()

# Process only the mapped files (saves compute time)
study_manager.run_wdl_workflows()
```

## Directory Structure

```
data_processing/
├── nmdc_dp_utils/              # Core utilities
│   ├── study_manager.py        # Main workflow manager
│   ├── raw_data_inspector.py   # Raw file metadata extraction (NEW!)
│   ├── raw_data_inspector.md   # Raw inspector documentation (NEW!)
│   └── ...
├── configurations/             # Study configuration templates
├── example_scripts/            # Usage examples
├── _study_folders/            # Individual study data
│   ├── config.json           # Study configuration
│   ├── metadata/            # Biosample mappings, file lists
│   ├── raw_file_info/       # Raw file inspection results (NEW!)
│   ├── wdl_execution/       # Self-contained WDL workflow execution (NEW!)
│   └── ...
└── mappings/                  # Biosample mapping templates
```

## New Raw Data Inspector

The raw data inspector extracts comprehensive metadata from MS files including:

- 🔬 **Instrument Information**: Model, serial number, name
- 📊 **Scan Parameters**: MS levels, scan types, collision energies
- 📈 **Data Ranges**: m/z ranges, retention time windows
- ⚡ **Polarity Information**: Positive/negative mode detection
- 📅 **Timestamps**: File creation and modification times
- ❌ **Error Tracking**: Failed file analysis with detailed logs

### Setup Requirements
1. Add CoreMS virtual environment path to your config:
```json
{
    "virtual_environments": {
        "corems_env": "/path/to/corems/venv"
    }
}
```

2. Install CoreMS in separate environment:
```bash
python -m venv /path/to/corems/venv
source /path/to/corems/venv/bin/activate
pip install corems pandas tqdm
```

See `nmdc_dp_utils/raw_data_inspector.md` for complete documentation.

## Configuration

Study configurations are JSON files containing:

```json
{
    "study": {
        "name": "study_name",
        "id": "nmdc:sty-11-xxxxxxxx",
        "massive_id": "MSV000000000"
    },
    "paths": {
        "base_directory": "/path/to/data_processing",
        "raw_data_directory": "/path/to/raw/data",
        "processed_data_directory": "/path/to/processed/data"
    },
    "virtual_environments": {
        "corems_env": "/path/to/corems/venv"
    },
    "minio": {
        "enabled": true,
        "bucket": "nmdc-processed-data",
        "processed_data_folder": "study_name/LC-MS/{config_name}/"
    },
    "configurations": [
        {
            "name": "hilic_pos",
            "file_filter": ["HILIC", "_POS_"],
            "cores": 4
        }
    ]
}
```

## Example Studies

- `_bioscales_lcms_metabolomics/`: Large-scale LC-MS metabolomics
- `_emp_500_gcms_metabolomics/`: GC-MS batch processing example
- `_kroeger_11_dwsv7q78/`: Complete workflow with raw inspection

## Workflow Integration

```python
# Complete workflow with quality control and self-contained WDL execution
study_manager = NMDCStudyManager("config.json")

# 1. Map files to biosamples
study_manager.run_biosample_mapping_script()

# 2. Generate filtered file list
study_manager._generate_mapped_files_list()

# 3. NEW: Inspect raw files for QC
inspection_results = study_manager.raw_data_inspector(cores=4)

# 4. Process only mapped files with self-contained WDL workflows
study_manager.generate_wdl_jsons()
study_manager.run_wdl_script()  # Downloads WDL from GitHub, executes in study directory

# 5. Upload processed data to MinIO (if configured)
study_manager.upload_to_minio()
```

## Recent Updates

### Version 2024.12 (NEW!)
- ✨ **Self-Contained WDL Workflows**: GitHub-integrated execution within study directories
- 🔄 **Raw Data Inspector**: Comprehensive MS file metadata extraction
- � **Virtual Environment Support**: Multi-environment workflow management
- 📊 **Enhanced Error Tracking**: Detailed logging and error analysis
- 🚀 **Parallel Processing**: Multi-core raw file analysis
- 📋 **Filtered Processing**: Process only biosample-mapped files
- 🔧 **Template System**: Maintainable biosample mapping scripts
- ☁️ **MinIO Integration**: Configurable cloud storage with processed data upload

### Previous Updates
- Biosample mapping simplification (4-column output)
- Automated processed file management
- WDL workflow optimization
- Configuration simplification (removed external workspace dependencies)

## Requirements

- Python 3.8+
- pandas, pathlib, subprocess
- miniwdl (for WDL workflow execution)
- docker (for workflow containerization)
- CoreMS (separate environment for raw inspection)
- MinIO client for cloud storage

## Documentation

- `nmdc_dp_utils/raw_data_inspector.md` - Raw file inspection guide
- `example_scripts/` - Usage examples and integration patterns
- Individual study folders contain specific README files

## Support

For issues or questions:
1. Check the documentation in `docs/`
2. Review example scripts in `example_scripts/`
3. Examine working configurations in study folders