# NMDC Study Management System

This reusable system provides a standardized way to manage NMDC metabolomics studies, including:
- Downloading files from MASSIVE datasets using dataset IDs
- Docker-based raw data inspection for metadata extraction
- Biosample mapping with confidence scoring
- Uploading/downloading files to/from MinIO
- Generating WDL JSON files for processing
- Managing consistent directory structures

## Setup

1. **Install dependencies**:
   ```bash
   pip install nmdc-api-utilities minio
   ```

2. **Install Docker**:
   ```bash
   # Pull the MetaMS Docker image for raw data inspection
   docker pull microbiomedata/metams:3.3.3
   ```

3. **Set MinIO environment variables** (optional, for data upload/download):
   ```bash
   export MINIO_ACCESS_KEY="your_access_key"
   export MINIO_SECRET_KEY="your_secret_key"
   ```

## Configuration

Each study needs a `config.json` file. Use `nmdc_dp_utils/example_config.json` as a starting point.

### Key Configuration Fields:

- **`study.massive_id`**: MASSIVE dataset ID with version (e.g., "v07/MSV000094090")
- **`study.file_filters`**: List of keywords to filter files (e.g., ["pos", "neg", "hilic"])
- **`paths.raw_data_directory`**: Where to download raw files
- **`paths.processed_data_directory`**: Where processed files are stored
- **`configurations`**: List of processing configurations (pos/neg, different methods)

### Skip Triggers

The system includes skip triggers to avoid repeating completed workflow steps when rerunning:

- **`skip_triggers.study_structure_created`**: Skip directory creation if already done
- **`skip_triggers.raw_data_downloaded`**: Skip FTP discovery and download if raw data is ready
- **`skip_triggers.wdls_generated`**: Skip WDL JSON generation if files already exist
- **`skip_triggers.data_processed`**: Skip WDL workflow execution if data is already processed

These triggers are automatically set to `true` when steps complete successfully. To rerun a step, manually set its trigger to `false` in the config file.

## Usage

### Option 1: Use the workflow script
```bash
cd _ecofab_lcms_11_ev70y104_new
python run_workflow.py
```

### Option 2: Use programmatically
```python
from nmdc_dp_utils.study_manager import NMDCStudyManager

# Initialize with your config file
study = NMDCStudyManager("path/to/config.json")

# Create directory structure
study.create_study_structure()

# Get FTP URLs from MASSIVE
ftp_df = study.get_massive_ftp_urls()

# Download files
downloaded_files = study.download_from_massive()

# Generate WDL JSON files
json_count = study.generate_wdl_jsons(batch_size=25)

# Upload processed results to MinIO
study.upload_to_minio(
    local_directory="/path/to/processed/data",
    folder_name="study_id/processed_20241027"
)

# Download results from MinIO
study.download_from_minio(
    folder_name="study_id/processed_20241027",
    local_directory="/path/to/download"
)
```

## Directory Structure

The system creates this standard structure for each study:
```
_study_name/
├── scripts/
├── metadata/
├── wdl_jsons/
│   ├── hilic_pos/
│   └── hilic_neg/
├── raw_file_info/
└── study_name_massive_ftp_locs.csv
```

## Key Methods

### `get_massive_ftp_urls(massive_id)`
- Gets FTP URLs for a MASSIVE dataset
- Applies file filters from config
- Saves results to CSV file
- Returns pandas DataFrame

### `download_from_massive()`
- Downloads files using FTP URLs
- Skips existing files
- Shows progress with tqdm
- Returns list of downloaded files

### `generate_wdl_jsons(batch_size)`
- Creates WDL JSON files for each configuration
- Splits files into batches
- Handles multiple processing configurations
- Returns number of JSON files created

### `upload_to_minio(local_directory, folder_name)`
- Uploads files to MinIO bucket
- Preserves directory structure
- Skips existing objects
- Returns number of uploaded files

### `download_from_minio(folder_name, local_directory)`
- Downloads files from MinIO bucket
- Recreates directory structure locally
- Skips existing files with same size
- Returns number of downloaded files

## Creating a New Study

1. **Copy the template**:
   ```bash
   cp nmdc_dp_utils/config_template.json _new_study/config.json
   ```

2. **Edit the configuration**:
   - Update study name, ID, and description
   - Set the MASSIVE dataset ID
   - Update file paths
   - Configure processing parameters

3. **Run the workflow**:
   ```bash
   cd _new_study
   python -c "
   import sys
   sys.path.append('../nmdc_dp_utils')
   from study_manager import NMDCStudyManager
   study = NMDCStudyManager('config.json')
   study.create_study_structure()
   study.get_massive_ftp_urls()
   "
   ```

## Example Study: EcoFab LCMS

The `_ecofab_lcms_11_ev70y104_new` directory demonstrates the new system with:
- MASSIVE dataset MSV000083559
- Two processing configurations (HILIC pos/neg)
- File filters for pos, neg, and hilic keywords
- Batch size of 25 files per WDL job

## Troubleshooting

- **FTP listing fails**: The system will create a template CSV file for manual editing
- **Import errors**: Make sure `nmdc_dp_utils` is in your Python path
- **MinIO errors**: Check environment variables and network connectivity
- **No files found**: Verify MASSIVE ID and file filters in config