#!/usr/bin/env python3
"""
GCMS CDF Data Inspector for NMDC Studies

Extracts metadata from GCMS CDF data files including:
- Instrument information
- Data range information (m/z, retention time)
- Polarity information
- File creation times
- Scan counts

This script uses CoreMS to read AndiNetCDF (CDF) files.

USAGE:
    This script is designed to be run within a Docker container as part of the
    NMDC workflow manager. It requires CoreMS to be installed in the container.
    
    Docker example:
        docker run --rm -v /data:/mnt/data \\
            microbiomedata/metams:3.3.3 \\
            python gcms_data_inspector.py \\
            --files /mnt/data/*.cdf \\
            --output-dir /mnt/data/results \\
            --cores 4
"""

import argparse
import csv
import logging
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from tqdm import tqdm

try:
    from corems.mass_spectra.input.andiNetCDF import ReadAndiNetCDF
    COREMS_AVAILABLE = True
except ImportError:
    COREMS_AVAILABLE = False
    print("⚠️  CoreMS not available - install with: pip install corems")


def setup_logging(out_dir: Path) -> Path:
    """Set up logging for error tracking"""
    log_file = out_dir / "gcms_inspection_log.log"
    
    # Remove existing log file to start fresh
    if log_file.exists():
        log_file.unlink()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return log_file


def get_cdf_file_info_single(file_path: Path, max_retries: int = 3, retry_delay: float = 5.0) -> Optional[Dict]:
    """
    Extract metadata from a single GCMS CDF file using CoreMS with retry logic.

    Args:
        file_path: Path to the CDF file
        max_retries: Maximum number of retry attempts for transient errors
        retry_delay: Delay in seconds between retry attempts

    Returns:
        Dictionary containing file metadata, or None if processing failed
    """
    import time
    
    for attempt in range(max_retries + 1):
        try:
            return _extract_cdf_metadata(file_path)
        except Exception as e:
            error_msg = str(e).lower()
            
            # Check if this is a retryable error (file access issues)
            retryable_errors = [
                "file is locked",
                "sharing violation", 
                "access denied",
                "permission denied",
                "device or resource busy",
                "file in use",
                "temporarily unavailable"
            ]
            
            is_retryable = any(retryable_error in error_msg for retryable_error in retryable_errors)
            
            if is_retryable and attempt < max_retries:
                logging.warning(f"Retryable error processing {file_path.name} (attempt {attempt + 1}/{max_retries + 1}): {str(e)}")
                logging.info(f"Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)
                continue
            else:
                # Final attempt failed or non-retryable error
                final_error_msg = f"Error processing {file_path.name} after {attempt + 1} attempts: {str(e)}"
                logging.error(final_error_msg)
                return {
                    "file_name": file_path.name,
                    "file_path": str(file_path),
                    "file_size_bytes": file_path.stat().st_size if file_path.exists() else 0,
                    "file_extension": file_path.suffix.lower(),
                    "error": final_error_msg,
                    "creation_time": datetime.fromtimestamp(file_path.stat().st_ctime).isoformat() if file_path.exists() else None,
                    # Set required fields to None to indicate failure
                    "write_time": None,
                    "instrument_model": None,
                    "instrument_name": None,
                    "instrument_serial_number": None,
                }
    
    # Should never reach here, but just in case
    return None


def _extract_cdf_metadata(file_path: Path) -> Dict:
    """
    Internal function to extract metadata from a CDF file (no retry logic).
    
    Args:
        file_path: Path to the CDF file
        
    Returns:
        Dictionary containing file metadata
        
    Raises:
        Exception: If file processing fails
    """
    if not COREMS_AVAILABLE:
        raise ImportError("CoreMS not available - cannot process CDF files")
    
    if file_path.suffix.lower() not in [".cdf", ".nc"]:
        raise ValueError(f"Unsupported file format: {file_path.suffix}. Expected .cdf or .nc")
    
    # Read the CDF file using CoreMS
    reader = ReadAndiNetCDF(file_path, auto_process=False)
    
    # Get GCMS object
    reader.run()
    gcms_obj = reader.get_gcms_obj()
    
    # Get instrument metadata from the CDF reader
    instrument_label = reader.instrument_label
    analyzer = reader.analyzer
    ionization_type = reader.ionization_type
    experiment_type = reader.experiment_type
    polarity = reader.polarity
    
    # Get data ranges from the NetCDF object
    net_cdf = reader.net_cdf_obj
    
    # Get m/z range from mass_values
    mass_values = net_cdf.variables.get('mass_values')[:]
    mz_min = float(mass_values.min())
    mz_max = float(mass_values.max())
    
    # Get retention time range (convert from seconds to minutes)
    rt_values = net_cdf.variables.get('scan_acquisition_time')[:] / 60
    rt_min = float(rt_values.min())
    rt_max = float(rt_values.max())
    
    # Get scan information
    total_scans = len(reader.list_scans)
    
    # Get TIC information
    tic = net_cdf.variables.get('total_intensity')[:]
    tic_min = float(tic.min())
    tic_max = float(tic.max())
    
    # Extract instrument information from NetCDF variables
    serial_number = "Unknown"
    instrument_model = "Unknown"
    instrument_name = "Unknown"
    instrument_mfr = "Unknown"
    
    def get_netcdf_string_var(nc_obj, var_name):
        """Helper to extract string from NetCDF variable"""
        if var_name in nc_obj.variables:
            var = nc_obj.variables[var_name]
            data = var[:]
            if hasattr(data, 'tobytes'):
                value = data.tobytes().decode('utf-8', errors='ignore').strip('\x00').strip()
                return value if value else "Unknown"
        return "Unknown"
    
    # Get instrument information from variables
    instrument_name = get_netcdf_string_var(net_cdf, 'instrument_name')
    instrument_model = get_netcdf_string_var(net_cdf, 'instrument_model')
    serial_number = get_netcdf_string_var(net_cdf, 'instrument_serial_no')
    instrument_mfr = get_netcdf_string_var(net_cdf, 'instrument_mfr')
    
    # If model is unknown but we have manufacturer, use that
    if instrument_model == "Unknown" and instrument_mfr != "Unknown":
        instrument_model = instrument_mfr
    
    # Try to get operator name from attributes as additional context
    operator_name = "Unknown"
    if hasattr(net_cdf, 'operator_name'):
        operator_name = str(net_cdf.operator_name).strip()
    
    # If we have operator name, add it to instrument_name for context
    if instrument_name != "Unknown" and operator_name != "Unknown":
        instrument_name = f"{instrument_name} (Operator: {operator_name})"
    
    # Get file write time from NetCDF attributes or file stats
    write_time = None
    if hasattr(net_cdf, 'netcdf_file_date_time_stamp'):
        write_time_raw = str(net_cdf.netcdf_file_date_time_stamp)
        # Parse format like "20180115081400-0800" to ISO format
        try:
            # Extract the datetime part (before timezone)
            dt_str = write_time_raw.split('-')[0] if '-' in write_time_raw else write_time_raw.split('+')[0]
            # Parse YYYYMMDDHHMMSS format
            dt = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
            write_time = dt.isoformat()
        except:
            write_time = write_time_raw
    elif hasattr(net_cdf, 'dataset_date_time_stamp'):
        write_time_raw = str(net_cdf.dataset_date_time_stamp)
        try:
            dt_str = write_time_raw.split('-')[0] if '-' in write_time_raw else write_time_raw.split('+')[0]
            dt = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
            write_time = dt.isoformat()
        except:
            write_time = write_time_raw
    else:
        # Fall back to file modification time
        write_time = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
    
    # Close the NetCDF file
    net_cdf.close()
    
    # Compile metadata
    file_info = {
        "file_name": file_path.name,
        "file_path": str(file_path),
        "file_size_bytes": file_path.stat().st_size,
        "file_extension": file_path.suffix.lower(),
        "instrument_model": instrument_model,
        "instrument_name": instrument_name,
        "instrument_serial_number": serial_number,
        "analyzer": analyzer,
        "ionization_type": ionization_type,
        "experiment_type": experiment_type,
        "polarity": str(polarity),
        "mz_min": mz_min,
        "mz_max": mz_max,
        "rt_min": rt_min,
        "rt_max": rt_max,
        "tic_min": tic_min,
        "tic_max": tic_max,
        "write_time": write_time,
        "total_scans": total_scans,
        "creation_time": datetime.fromtimestamp(file_path.stat().st_ctime).isoformat(),
    }
    
    return file_info


def process_file_wrapper(args) -> Optional[Dict]:
    """Wrapper function for parallel processing"""
    file_path, output_file, error_file, max_retries, retry_delay = args
    
    if not file_path.is_file():
        print(f"⚠️  File not found: {file_path.name}")
        return None
    
    print(f"🔍 Processing: {file_path.name} ({file_path.stat().st_size / (1024*1024):.1f} MB)")
    
    result = get_cdf_file_info_single(file_path, max_retries=max_retries, retry_delay=retry_delay)
    
    if result is not None:
        # Write successful result immediately
        write_result_to_csv(result, output_file)
        if result.get('error'):
            print(f"❌ {file_path.name}: {result.get('error')}")
        else:
            print(f"✅ {file_path.name}: {result.get('total_scans', 'N/A')} scans, RT: {result.get('rt_min', 0):.1f}-{result.get('rt_max', 0):.1f} min")
        return result
    else:
        print(f"❌ {file_path.name}: Processing failed completely")
        # Log error
        with open(error_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().isoformat(),
                file_path.name,
                str(file_path),
                "Processing failed - returned None"
            ])
        return None


def write_result_to_csv(result: Dict, output_file: Path):
    """Write a single result to CSV file with thread safety"""
    file_exists = output_file.exists()
    
    with open(output_file, 'a', newline='') as f:
        fieldnames = [
            "file_name", "file_path", "file_size_bytes", "file_extension",
            "instrument_model", "instrument_name", "instrument_serial_number",
            "analyzer", "ionization_type", "experiment_type", "polarity",
            "mz_min", "mz_max", "rt_min", "rt_max", 
            "tic_min", "tic_max", "write_time", "total_scans", "creation_time"
        ]
        
        # Add error field if present
        if 'error' in result:
            if 'error' not in fieldnames:
                fieldnames.append('error')
        
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(result)


def inspect_cdf_files(
    file_paths: List[Path],
    output_dir: Path,
    cores: int = 1,
    max_retries: int = 3,
    retry_delay: float = 5.0
) -> tuple[Path, Path]:
    """
    Inspect multiple CDF files and extract metadata.

    Args:
        file_paths: List of paths to CDF files
        output_dir: Directory for output files
        cores: Number of CPU cores for parallel processing
        max_retries: Maximum retry attempts for transient errors
        retry_delay: Delay between retry attempts

    Returns:
        Tuple of (output_file_path, error_file_path)
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Set up logging
    log_file = setup_logging(output_dir)
    
    # Output files
    output_file = output_dir / "raw_file_inspection_results.csv"
    error_file = output_dir / "raw_file_inspection_errors.csv"
    
    # Clear existing output files
    if output_file.exists():
        output_file.unlink()
    
    # Create error file with header
    with open(error_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'file_name', 'file_path', 'error'])
    
    logging.info(f"Starting inspection of {len(file_paths)} CDF files")
    logging.info(f"Using {cores} cores for processing")
    
    # Prepare arguments for parallel processing
    args_list = [(fp, output_file, error_file, max_retries, retry_delay) for fp in file_paths]
    
    # Process files
    if cores == 1:
        # Sequential processing
        results = []
        for args in tqdm(args_list, desc="Inspecting CDF files"):
            result = process_file_wrapper(args)
            if result:
                results.append(result)
    else:
        # Parallel processing
        with Pool(cores) as pool:
            results = []
            for result in tqdm(
                pool.imap_unordered(process_file_wrapper, args_list),
                total=len(args_list),
                desc="Inspecting CDF files"
            ):
                if result:
                    results.append(result)
    
    # Summary
    successful = len([r for r in results if not r.get('error')])
    failed = len([r for r in results if r.get('error')])
    
    logging.info(f"✅ Successfully processed: {successful}/{len(file_paths)} files")
    if failed > 0:
        logging.info(f"❌ Failed: {failed} files (see {error_file})")
    
    return output_file, error_file


def main():
    """Main entry point for CLI"""
    parser = argparse.ArgumentParser(
        description="Extract metadata from GCMS CDF files"
    )
    parser.add_argument(
        "files",
        nargs="+",
        type=Path,
        help="CDF files to inspect"
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=Path("raw_file_info"),
        help="Output directory for results (default: raw_file_info)"
    )
    parser.add_argument(
        "-c", "--cores",
        type=int,
        default=1,
        help="Number of CPU cores for parallel processing (default: 1)"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retry attempts for transient errors (default: 3)"
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=5.0,
        help="Delay in seconds between retries (default: 5.0)"
    )
    
    args = parser.parse_args()
    
    # Validate files
    valid_files = []
    for file_path in args.files:
        if not file_path.exists():
            print(f"⚠️  File not found: {file_path}")
        elif file_path.suffix.lower() not in ['.cdf', '.nc']:
            print(f"⚠️  Not a CDF file: {file_path}")
        else:
            valid_files.append(file_path)
    
    if not valid_files:
        print("❌ No valid CDF files to process")
        return 1
    
    # Run inspection
    output_file, error_file = inspect_cdf_files(
        valid_files,
        args.output_dir,
        cores=args.cores,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay
    )
    
    print(f"\n📊 Results saved to: {output_file}")
    if error_file.stat().st_size > 50:  # Has content beyond header
        print(f"⚠️  Errors logged to: {error_file}")
    
    return 0


if __name__ == "__main__":
    exit(main())
