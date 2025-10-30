# Raw Data Inspector

The NMDC Study Manager now includes a `raw_data_inspector` method that extracts comprehensive metadata from raw MS data files using CoreMS in a separate virtual environment.

## Overview

The raw data inspector provides detailed information about raw MS files including:
- Instrument information (model, serial number, name)
- Scan parameters (levels, types, collision energies)
- Data range information (m/z, retention time ranges)
- Polarity information
- File creation times and sizes
- Error tracking for failed files

## Setup Requirements

### 1. Virtual Environment Configuration

Add the CoreMS virtual environment path to your study configuration:

```json
{
    "virtual_environments": {
        "corems_env": "/path/to/your/corems/venv"
    }
}
```

### 2. CoreMS Environment

The inspector requires a separate Python environment with CoreMS installed:

```bash
# Create and activate virtual environment
python -m venv /path/to/corems/venv
source /path/to/corems/venv/bin/activate  # On macOS/Linux
# or
/path/to/corems/venv/Scripts/activate     # On Windows

# Install CoreMS and dependencies
pip install corems pandas tqdm
```

## Usage

### Basic Usage

```python
from nmdc_dp_utils.study_manager import NMDCStudyManager

# Initialize study manager
study_manager = NMDCStudyManager("path/to/config.json")

# Inspect all mapped raw files
result_file = study_manager.raw_data_inspector()
```

### Advanced Usage

```python
# Inspect specific files
file_paths = ["/path/to/file1.mzML", "/path/to/file2.mzML"]
result_file = study_manager.raw_data_inspector(
    file_paths=file_paths,
    cores=4,     # Use 4 cores for parallel processing
    limit=100    # Limit to first 100 files
)
```

### Parameters

- `file_paths` (List[str], optional): Specific file paths to inspect. If None, uses mapped raw files from metadata
- `cores` (int, default=1): Number of cores for parallel processing
- `limit` (int, optional): Maximum number of files to process (useful for testing)

## Output

The inspector creates several output files in the `raw_file_info` directory with simple, consistent names:

### Main Results File
`raw_file_inspection_results.csv` containing:

| Column | Description |
|--------|-------------|
| file_name | Original filename |
| file_path | Full path to file |
| file_size_bytes | File size in bytes |
| file_extension | File extension (.mzML, .raw) |
| instrument_model | MS instrument model |
| instrument_name | Instrument name |
| instrument_serial_number | Serial number |
| scan_types | Available scan types |
| scan_levels | MS levels present |
| collision_energies | Collision energies used |
| polarity | Ionization polarities |
| mz_min | Minimum m/z value |
| mz_max | Maximum m/z value |
| rt_min | Minimum retention time |
| rt_max | Maximum retention time |
| write_time | File creation time from metadata |
| total_scans | Total number of scans |
| creation_time | File system creation time |
| error | Error message if processing failed |

### Error Log
`raw_file_inspection_errors.csv` containing details of failed files.

### Processing Log
`raw_inspection_log.log` with detailed processing information.

## Integration with Workflow

The raw data inspector can be integrated at different points in the NMDC workflow:

### After Biosample Mapping
```python
# Run biosample mapping first
study_manager.run_biosample_mapping_script()

# Generate filtered file list
study_manager._generate_mapped_files_list()

# Inspect the mapped files
inspection_result = study_manager.raw_data_inspector()

# Continue with WDL processing
study_manager.generate_wdl_jsons()
```

### Quality Control Analysis
```python
# Run inspection on all files for QC
qc_result = study_manager.raw_data_inspector(cores=8)

# Analyze results
import pandas as pd
results_df = pd.read_csv(qc_result)

# Check for instrument consistency
instruments = results_df['instrument_model'].value_counts()
print(f"Instruments found: {dict(instruments)}")

# Check for failed files
failed_files = results_df[results_df['error'].notna()]
print(f"Failed files: {len(failed_files)}")
```

## Error Handling

The inspector handles various error conditions:

1. **Missing CoreMS**: Falls back to basic file information
2. **Unsupported formats**: Currently supports .mzML files
3. **Corrupted files**: Logs errors and continues processing
4. **Virtual environment issues**: Clear error messages for setup problems

## Performance

- **Parallel Processing**: Use multiple cores for faster processing
- **Memory Efficient**: Processes files individually to minimize memory usage  
- **Progress Tracking**: Shows progress bar during processing
- **Incremental Output**: Results written immediately to prevent data loss

## Examples

See the example scripts:
- `test_raw_data_inspector.py` - Basic testing
- `example_scripts/workflow_with_raw_inspection.py` - Complete workflow integration

## Troubleshooting

### Common Issues

1. **"CoreMS virtual environment path not found"**
   - Add `virtual_environments.corems_env` to your config.json

2. **"Virtual environment not found"**
   - Verify the path exists and contains a valid Python installation

3. **File format not supported**
   - Ensure files are in .raw (Thermo) or .mzML format

4. **Import errors in CoreMS environment**
   - Ensure CoreMS and dependencies are properly installed in the virtual environment

### Validation

Test your setup with a small number of files first:

```python
# Test with just 5 files
result = study_manager.raw_data_inspector(limit=5, cores=1)
```

## File Type Support

Currently supported formats:
- ✅ `.raw` files (Thermo Fisher - full metadata extraction)
- ✅ `.mzML` files (vendor-neutral format - full metadata extraction)

Future support planned for:
- Additional vendor formats (Agilent, Waters, etc.)
- Cloud-based file processing
- Batch conversion utilities