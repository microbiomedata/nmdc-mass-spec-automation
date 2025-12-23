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

### Workflow Example

```python
from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager

# Initialize workflow manager
manager = NMDCWorkflowManager("studies/your_study/config.json")

# Step 1: Create directory structure
manager.create_workflow_structure()

# Step 2: Fetch raw data (automatically uses MASSIVE or MinIO based on config)
manager.fetch_raw_data()

# Step 3: Map files to biosamples
manager.get_biosample_attributes()
manager.generate_biosample_mapping_script()
## Manually edit and run the mapping script as instructed in the generated script
manager.run_biosample_mapping_script()

# Step 4: Inspect raw data
manager.raw_data_inspector(cores=4)

# Step 5: Process data (generate WDL configs and execute workflows)
manager.process_data(execute=True)

# Step 6: Upload to MinIO
manager.upload_processed_data_to_minio()

# Step 7: Generate NMDC metadata packages
manager.generate_nmdc_metadata_for_workflow()
```

### Configuration-Based Operation

The workflow manager uses configuration files to determine:
- **Data source**: Presence of `massive_id` in config → MASSIVE; otherwise → MinIO
- **Batch size**: Configured in `config['workflow']['batch_size']`
- **File filtering**: Configured in `config['workflow']['file_filters']`
- **Processing parameters**: Read from `config['configurations']`

All methods use configuration parameters automatically—no need to pass arguments manually.

## Repository Structure

```
nmdc_mass_spec_automation/
├── nmdc_dp_utils/                  # Core system modules
│   ├── workflow_manager.py         # Main workflow orchestration class
│   ├── raw_data_inspector.py       # Docker-based raw file inspection
│   ├── example_config.json         # Example configuration file
│   ├── README.md                   # Detailed system documentation
│   ├── metadata_overrides_examples.md  # Metadata override examples
│   └── templates/                  # Script templates
│       ├── biosample_mapping_script_template.py
│       └── README.md
├── studies/                        # Individual study/workflow directories
│   └── kroeger_11_dwsv7q78_lcms_metab/  # Example: complete LC-MS Metabolomics workflow
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

### Study Directory Structure

Each workflow creates the following structure:

```
studies/workflow_name/
├── workflow_config.json            # Workflow configuration
├── run_workflow.py                 # Workflow runner script
├── scripts/                        # Generated and custom scripts
│   ├── map_raw_files_to_biosamples_TEMPLATE.py
│   ├── map_raw_files_to_biosamples.py
│   └── workflow_name_wdl_runner.sh
├── metadata/                       # Biosample and mapping data
│   ├── biosample_attributes.csv
│   ├── mapped_raw_files.csv
│   ├── downloaded_files.csv
│   └── metadata_gen_input_csvs/    # Metadata generation input files
├── raw_file_info/                 # Raw data inspection results
│   ├── raw_file_inspection_results.csv
│   └── raw_file_inspection_errors.csv
├── wdl_jsons/                     # Generated WDL configurations
│   ├── hilic_pos/
│   ├── hilic_neg/
│   ├── rp_pos/
│   └── rp_neg/
└── wdl_execution/                 # Temporary WDL execution directory
```

### For More Information
Refer to the individual study README files and the detailed documentation in `nmdc_dp_utils/README.md` and `nmdc_dp_utils/metadata_overrides_examples.md`.
