# NMDC Metabolomics Data Processing System

A standardized workflow system for processing NMDC (National Microbiome Data Collaborative) mass-spec 'omics studies from raw data retrieval through NMDC metadata submission.

## Overview

This system provides automated workflows for mass-spec 'omics data processing, including:

- Consistent and configurable study setup
- Automated data discovery and download from MASSIVE repositories
- Docker-based raw data inspection
- Biosample mapping with confidence scoring
- WDL workflow generation and data processing using [MetaMS](https://github.com/microbiomedata/metaMS) or [EnviroMS](https://github.com/microbiomedata/enviroms)
- MinIO object storage integration
- NMDC metadata package generation and submission

## Prerequisites

**Required Software:**
- Python 3.8 or higher
- Docker Desktop
- Git

**System Requirements:**
- Adequate storage for raw data (roughly 50-500 GB per study, but highly variable)
- Internet connectivity for MASSIVE downloads, Docker operations, and MinIO access
- MinIO credentials (for cloud storage operations)

## Installation

### 1. Clone Repository and Install Dependencies

```bash
git clone <repository-url>
cd data_processing
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Set Up Environment Variables

Create a `.env` file in the project root:

```bash
MINIO_ACCESS_KEY="your_access_key"
MINIO_SECRET_KEY="your_secret_key"
```

## Quick Start

### Complete Workflow Example

```python
from nmdc_dp_utils.study_manager import NMDCWorkflowManager

# Initialize study manager
study = NMDCWorkflowManager("studies/your_study/config.json")

# Step 1: Create directory structure
study.create_study_structure()

# Step 2: Discover and download raw data
ftp_df = study.get_massive_ftp_urls()
downloaded_files = study.download_from_massive()

# Step 3: Map files to biosamples
biosample_csv = study.get_biosample_attributes()
mapping_script = study.generate_biosample_mapping_script()
study.run_biosample_mapping_script()

# Step 4: Inspect raw data
inspection_results = study.raw_data_inspector(cores=4)

# Step 5: Generate WDL configurations
json_count = study.generate_wdl_jsons(batch_size=25)

# Step 6: Run processing workflows
script_path = study.generate_wdl_runner_script()
study.run_wdl_script(script_path)

# Step 7: Upload to MinIO (if configured)
study.upload_processed_data_to_minio()
```

### Using Automated Workflow Scripts

Each study can include a `run_workflow.py` script for automated execution:

```bash
cd data_processing
python studies/kroeger_11_dwsv7q78/run_workflow.py
```

## Repository Structure

```
data_processing/
├── nmdc_dp_utils/              # Core system modules
│   ├── study_manager.py        # Main workflow orchestration
│   ├── raw_data_inspector.py   # Docker-based raw file inspection
│   ├── README.md               # Detailed system documentation
│   ├── raw_data_inspector.md   # Raw inspection documentation
│   └── templates/              # Biosample mapping templates
├── studies/                    # Individual study directories
│   ├── kroeger_11_dwsv7q78/   # Example: complete LC-MS workflow
│   ├── singer_11_46aje659/    # Example: lipidomics study
│   └── ...
├── requirements.txt            # Python dependencies
├── checklist.md               # Data processing evaluation checklist
└── README.md                  # This file
```

### Study Directory Structure

Each study creates the following structure:

```
studies/your_study/
├── config.json                     # Study configuration
├── run_workflow.py                 # Automated workflow runner
├── scripts/                        # Generated and custom scripts
│   └── map_raw_files_to_biosamples.py
├── metadata/                       # Biosample and mapping data
│   ├── biosample_attributes.csv
│   ├── mapped_raw_files.csv
│   └── downloaded_files.csv
├── raw_file_info/                 # Raw data inspection results
│   └── raw_file_inspection_results.csv
├── wdl_jsons/                     # Generated WDL configurations
│   ├── hilic_pos/
│   └── hilic_neg/
└── wdl_execution/                 # Temporary WDL execution directory
```

### For More Information
Refer to the individual study README files and the detailed documentation in `nmdc_dp_utils/README.md` and `nmdc_dp_utils/metadata_overrides_examples.md`.
