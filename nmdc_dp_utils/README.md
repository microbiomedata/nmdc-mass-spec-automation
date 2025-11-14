# NMDC Study Management System

This reusable system provides a standardized way to manage and process NMDC LCMS metabolomics studies, including:
- Downloading files from MASSIVE datasets using dataset IDs
- Docker-based raw data inspection for metadata extraction
- Biosample mapping with confidence scoring
- Generating WDL JSON files for processing
- Processing workflows using MetaMS Docker images
- Uploading/downloading files to/from MinIO
- Generating NMDC metadata packages
- Validating and submitting metadata to NMDC dev and production environments

## Setup
You need Python 3.8+ and Docker installed prior to using this system.

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Install Docker**:
   ```bash
   # Pull the MetaMS Docker image for raw data inspection
   docker pull microbiomedata/metams:3.3.3
   ```

3. **Set up environment variables** :
   **Create a `.env` file in the project root directory and add the environment variables you'll need:**

   **For uploading data to MinIO:**
   ```bash
   MINIO_ACCESS_KEY="your_access_key"
   MINIO_SECRET_KEY="your_secret_key"
   ```

   **For submitting metadata to NMDC:**
   coming soon!

## Configuration

Each study needs a configuration json. Use `nmdc_dp_utils/example_config.json` as a starting point.

### Key Configuration Fields:

- **`study.name`**: File safe name for the study that will be used in local and MinIO file paths (e.g., "kroeger_11_dwsv7q78")
- **`study.id`**: Unique NMDC study identifier (e.g., "nmdc:sty-11-xxxxxxxx")
- **`study.description`**: Brief description of the study
- **`workflow.name`**: Name for this specific workflow run (e.g., "kroeger_11_dwsv7q78_lcms_metab")
- **`workflow.massive_id`**: MASSIVE dataset ID with version (e.g., "v07/MSV000094090")
- **`workflow.file_type`**: Type of raw files to download (e.g., ".raw" or ".mzML")
- **`workflow.file_filters`**: List of keywords to filter files before downloading (e.g., ["msms"])
- **`workflow.processed_data_date_tag`**: Date tag to append to processed data folder for the workflow batch (e.g., "20251027")
- **`workflow.workflow_type`**: Type of workflow (currently only "LCMS Metabolomics" is supported)
- **`workflow.batch_size`**: Number of files to process per WDL batch (e.g., 25)
- **`paths.base_directory`**: Path to base data processing directory
- **`paths.data_directory`**: Path where raw and processed files are stored, the system will create a study-specific subdirectory here
- **`minio.bucket`**: Bucket name for MinIO uploads/downloads
- **`configurations`**: List of processing configurations, each with:
  - **`name`**: Configuration name (e.g., "hilic_pos")
  - **`file_filter`**: List of keywords to filter files for this configuration (e.g., ["HILIC", "_POS_"])
  - **`cores`**: Number of CPU cores to allocate per WDL job
  - **`corems_toml`**: Path to CoreMS parameter TOML file
  - **`reference_db`**: Path to reference MS/MS database MSP file or SQLIte database for annotation
  - **`scan_translator`**: Path to scan translator TOML file
  - **`chromat_configuration_name`**: Name of the chromatographic configuration (e.g., "JGI/LBNL Metabolomics Standard LC Method - Polar HILIC-Z"). See Metadata Overrides Examples ([metadata_overrides_examples.md](./metadata_overrides_examples.md)) for more details.
  - **`mass_spec_configuration_name`**: Name of the mass spectrometry configuration (e.g., "JGI/LBNL Standard Metabolomics Method, positive @10,20,40CE"). See Metadata Overrides Examples ([metadata_overrides_examples.md](./metadata_overrides_examples.md)) for more details.
  - **`metadata`**: NMDC metadata generation settings to be applied, with:
    - **`instrument_used`**: Name of the instrument used (e.g., "Thermo Orbitrap Q-Exactive")
    - **`processing_institution_workflow`**: Institution name where workflow was run (e.g. "NMDC")
    - **`processing_institution_generation`**: Institution name where raw data were generated (e.g. "JGI")
   - **`chromat_configuration_name`**: Name of the chromatographic configuration (e.g., "JGI/LBNL Metabolomics Standard LC Method - Polar HILIC-Z"). Note that this is overwritten if specified in a configuration. See Metadata Overrides Examples ([metadata_overrides_examples.md](./metadata_overrides_examples.md)) for more details.
   - **`mass_spec_configuration_name`**: Name of the mass spectrometry configuration (e.g., "JGI/LBNL Standard Metabolomics Method, positive @10,20,40CE"). Note that this is overwritten if specified in a configuration. See Metadata Overrides Examples ([metadata_overrides_examples.md](./metadata_overrides_examples.md)) for more details.
   - **`use_massive_urls`**: Boolean to use MASSIVE URLs directly when generating metadata packages
   - **`serial_numbers_to_remove`**: List of instrument serial numbers to exclude from metadata generation (e.g., ["Unknown", "Exactive Series slot #1"])

### Skip Triggers

The system includes skip triggers to avoid repeating completed workflow steps when rerunning:

- **`skip_triggers.study_structure_created`**: Skip directory creation if already done
- **`skip_triggers.raw_data_downloaded`**: Skip FTP discovery and download if raw data is ready
- **`skip_triggers.biosample_attributes_fetched`**: Skip biosample attribute fetching if already done
- **`skip_triggers.biosample_mapping_script_generated`**: Skip biosample mapping script generation if already done
- **`skip_triggers.biosample_mapping_completed`**: Skip biosample mapping if already done
- **`skip_triggers.raw_data_inspected`**: Skip raw data inspection if already done
- **`skip_triggers.metadata_mapping_generated`**: Skip metadata mapping if already done
- **`skip_triggers.data_processed`**: Skip WDL workflow execution if data is already processed
- **`skip_triggers.processed_data_uploaded_to_minio`**: Skip MinIO upload if already done
- **`skip_triggers.metadata_packages_generated`**: Skip metadata package generation if already done
- **`skip_triggers.metadata_submitted_dev`**: Skip metadata submission to NMDC dev if already done
- **`skip_triggers.metadata_submitted_prod`**: Skip metadata submission to NMDC production if already done

These triggers are automatically set to `true` when steps complete successfully. To rerun a step, manually set its trigger to `false` in the config file or use the `reset_skip_triggers()` method on the workflow manager object.

## Directory Structure

The system creates this standard structure for each workflow:
```
workflow_name/
├── scripts/
├── metadata/
├── wdl_jsons/
│   ├── hilic_pos/
│   └── hilic_neg/
├── raw_file_info/
```