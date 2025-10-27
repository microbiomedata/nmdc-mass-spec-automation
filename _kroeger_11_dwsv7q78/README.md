# Kroeger Study (nmdc:sty-11-dwsv7q78)

**MASSIVE ID**: MSV000094090  
**Description**: Microbial regulation of soil water repellency to control soil degradation  
**Data**: `/Volumes/LaCie/nmdc_data/_kroeger_11_dwsv7q78/`

## Quick Start

```bash
cd /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing
python _kroeger_11_dwsv7q78/run_workflow.py
```

This will:
1. Discover .raw files from MASSIVE (filters: "msms")
2. Download to local storage
3. Generate WDL JSON files for processing

## Processing Configurations

Four analytical methods for comprehensive metabolomics:

- **hilic_pos**: HILIC + ESI positive (files: "HILIC" + "POSITIVE")
- **hilic_neg**: HILIC + ESI negative (files: "HILIC" + "NEGATIVE")  
- **rp_pos**: Reverse phase + ESI positive (files: "RP" + "POSITIVE")
- **rp_neg**: Reverse phase + ESI negative (files: "RP" + "NEGATIVE")

## Workflow Execution

### Automated Workflow Runner
Use the Python workflow runner for complete automation:

```bash
cd /Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing
python _kroeger_11_dwsv7q78/run_workflow.py
```

### Manual Step-by-Step Process

1. **Initialize Study Manager**:
   ```python
   from nmdc_dp_utils.study_manager import NMDCStudyManager
   study = NMDCStudyManager('_kroeger_11_dwsv7q78/config.json')
   ```

2. **Create Directory Structure**:
   ```python
   study.create_study_structure()
   ```

3. **Discover and Download Data**:
   ```python
   # Get FTP URLs from MASSIVE
   ftp_df = study.get_massive_ftp_urls()
   
   # Download raw files
   downloaded_files = study.download_from_massive()
   ```

4. **Generate WDL Processing Configurations**:
   ```python
   json_count = study.generate_wdl_jsons(batch_size=25)
   ```

## File Organization

```
## Files

```
_kroeger_11_dwsv7q78/
├── config.json              # Study configuration
├── run_workflow.py          # Main script
├── README.md               # This file
└── wdl_jsons/              # Generated WDL configs
```

## Troubleshooting

- **FTP issues**: Check connectivity and MASSIVE server status
- **Download failures**: Verify disk space and permissions
- **Config errors**: Validate JSON syntax in config.json
- **Missing files**: Check MASSIVE ID and file filters
```

## Processing Parameters

- **Batch Size**: 25 files per WDL JSON
- **Processing Cores**: 5 cores per configuration
- **Database**: 20250407 MS/MS database
- **Scan Translation**: EcoFAB scan translator parameters

## Data Storage

### Raw Data
- **Location**: `/Volumes/LaCie/nmdc_data/_kroeger_11_dwsv7q78/raw/`
- **Format**: Thermo .raw files
- **Size**: ~500 files (estimated after filtering)

### Processed Data
- **Location**: `/Volumes/LaCie/nmdc_data/_kroeger_11_dwsv7q78/processed_20251027/`
- **MinIO Integration**: Configured for upload to metabolomics bucket

## Configuration Files

The study uses configuration files from other NMDC studies:
- **HILIC Parameters**: From EcoFAB study (`_ecofab_lcms_11_ev70y104_new`)
- **RP Parameters**: From Bioscales study (`_bioscales_lcms_metabolomics`)

## MinIO Integration

The study is configured for MinIO object storage:
- **Endpoint**: `admin.nmdcdemo.emsl.pnl.gov`
- **Bucket**: `metabolomics`
- **Security**: TLS enabled

Set environment variables for MinIO access:
```bash
export MINIO_ACCESS_KEY="your_access_key"
export MINIO_SECRET_KEY="your_secret_key"
```

## Troubleshooting

### Common Issues

1. **No files found**: Check that MASSIVE FTP is accessible and MSV000094090 exists
2. **Download failures**: Verify network connectivity and disk space
3. **WDL generation issues**: Ensure raw data directory contains .raw files
4. **Configuration errors**: Verify all TOML and MSP file paths exist

### Debug Commands

```python
# Check study configuration
study = NMDCStudyManager('_kroeger_11_dwsv7q78/config.json')
info = study.get_study_info()
print(info)

# Test FTP connection
ftp_df = study.get_massive_ftp_urls()
print(f"Found {len(ftp_df)} files")
```

## Study Integration

This study uses the reusable NMDC Study Management System, which provides:
- Automated MASSIVE dataset discovery
- Configurable file filtering
- Standardized directory structures
- WDL workflow generation
- MinIO integration
- Progress tracking and logging

For more information on the study management system, see `/nmdc_dp_utils/study_manager.py`.