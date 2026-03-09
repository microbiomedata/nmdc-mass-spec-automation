#!/usr/bin/env python3
"""
Raw Data Inspector for NMDC Studies

Extracts metadata from raw MS data files (mzML or raw formats) including:
- Instrument information (model, serial number, name)
- Scan parameters (levels, types, collision energies)
- Data range information (m/z, retention time)
- Polarity information
- File creation times

This script is designed to run in a CoreMS environment with proper dependencies.
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
    from corems.mass_spectra.input.mzml import MZMLSpectraParser
    from corems.mass_spectra.input.rawFileReader import ImportMassSpectraThermoMSFileReader
    COREMS_AVAILABLE = True
except ImportError:
    COREMS_AVAILABLE = False
    print("⚠️  CoreMS not available - raw file parsing will be limited")


def setup_logging(out_dir: Path) -> Path:
    """Set up logging for error tracking"""
    log_file = out_dir / "raw_inspection_log.log"
    
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


def get_raw_file_info_single(file_path: Path, max_retries: int = 3, retry_delay: float = 5.0) -> Optional[Dict]:
    """
    Extract metadata from a single mass spectrometry file using CoreMS with retry logic.

    Args:
        file_path: Path to the mass spectrometry file (.raw or .mzML)
        max_retries: Maximum number of retry attempts for transient errors
        retry_delay: Delay in seconds between retry attempts

    Returns:
        Dictionary containing file metadata, or None if processing failed
    """
    import time
    
    for attempt in range(max_retries + 1):
        try:
            return _extract_file_metadata(file_path)
        except Exception as e:
            error_msg = str(e).lower()
            
            # Check if this is a retryable error (file access issues)
            retryable_errors = [
                "unable to access the raw file using the rawfile reader",
                "unable to access the raw file using the rawfilereader class",
                "rawfilereader",
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


def _extract_file_metadata(file_path: Path) -> Dict:
    """
    Internal function to extract metadata from a file (no retry logic).
    
    Args:
        file_path: Path to the mass spectrometry file
        
    Returns:
        Dictionary containing file metadata
        
    Raises:
        Exception: If file processing fails
    """
    if not COREMS_AVAILABLE:
        # Basic file info only
        return {
            "file_name": file_path.name,
            "file_path": str(file_path),
            "file_size_bytes": file_path.stat().st_size,
            "file_extension": file_path.suffix.lower(),
            "creation_time": datetime.fromtimestamp(file_path.stat().st_ctime).isoformat(),
            "error": "CoreMS not available - limited metadata only"
        }
    
    if file_path.suffix.lower() == ".raw":
        parser = ImportMassSpectraThermoMSFileReader(file_path)
    elif file_path.suffix.lower() == ".mzml":
        parser = MZMLSpectraParser(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    # Get LCMS object
    lcms_obj = parser.get_lcms_obj(spectra='none')

    # Get instrument metadata
    instrument_info = parser.get_instrument_info()
    serial_number = instrument_info.get("serial_number", "Unknown")
    instrument_model = instrument_info.get("model", "Unknown")
    instrument_name = instrument_info.get("name", "Unknown")
    
    # Get scan information
    scan_types = list(set(lcms_obj.scan_df['ms_format'].to_list()))
    scan_levels = list(set(lcms_obj.scan_df['ms_level'].to_list()))
    
    # Get data ranges
    mz_min = lcms_obj.scan_df['scan_window_lower'].min()
    mz_max = lcms_obj.scan_df['scan_window_upper'].max()
    rt_min = lcms_obj.scan_df['scan_time'].min()
    rt_max = lcms_obj.scan_df['scan_time'].max()
    
    # Extract collision energies from scan text (if available)
    collision_energies = []
    if 'scan_text' in lcms_obj.scan_df.columns:
        ce_series = lcms_obj.scan_df['scan_text'].str.extract(r'@hcd(\d+)\.')[0].dropna()
        collision_energies = list(set(ce_series.unique().tolist()))
    else:
        # For .raw files, collision energies might not be in scan_text
        collision_energies = ["Unknown"]

    # Extract MS2 type from scan text (if available)
    # For example, if the scan_text contains @hcd20.35, we can extract "hcd" as the MS2 type
    ms2_types = []
    if 'scan_text' in lcms_obj.scan_df.columns:
        ms2_series = lcms_obj.scan_df['scan_text'].str.extract(r'@([a-zA-Z]+)\d+\.')[0].dropna()
        ms2_types = list(set(ms2_series.unique().tolist()))
    else:
        ms2_types = ["Unknown"]
    
    # Get polarity information
    polarity = list(set(lcms_obj.scan_df['polarity'].tolist()))
    
    # Get file creation time
    write_time = parser.get_creation_time()
    
    # Validate that we got a write_time - this is critical for metadata generation
    if write_time is None:
        error_msg = f"Failed to extract write_time from {file_path.name} - file may be corrupted or unsupported format"
        logging.error(error_msg)
        raise ValueError(error_msg)
    
    # Compile metadata
    file_info = {
        "file_name": file_path.name,
        "file_path": str(file_path),
        "file_size_bytes": file_path.stat().st_size,
        "file_extension": file_path.suffix.lower(),
        "instrument_model": instrument_model,
        "instrument_name": instrument_name,
        "instrument_serial_number": serial_number,
        "scan_types": str(scan_types),
        "scan_levels": str(scan_levels),
        "collision_energies": str(collision_energies),
        "ms2_types": str(ms2_types),
        "polarity": str(polarity),
        "mz_min": float(mz_min) if pd.notna(mz_min) else None,
        "mz_max": float(mz_max) if pd.notna(mz_max) else None,
        "rt_min": float(rt_min) if pd.notna(rt_min) else None,
        "rt_max": float(rt_max) if pd.notna(rt_max) else None,
        "write_time": write_time,
        "total_scans": len(lcms_obj.scan_df),
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
    
    result = get_raw_file_info_single(file_path, max_retries=max_retries, retry_delay=retry_delay)
    
    if result is not None:
        # Write successful result immediately
        write_result_to_csv(result, output_file)
        if result.get('error'):
            print(f"❌ {file_path.name}: {result.get('error')}")
        else:
            print(f"✅ {file_path.name}: {result.get('total_scans', 'N/A')} scans, {result.get('instrument_model', 'Unknown')} instrument")
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
    
    try:
        with open(output_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=result.keys())
            
            # Write header if file doesn't exist
            if not file_exists:
                writer.writeheader()
            
            writer.writerow(result)
    except Exception as e:
        logging.error(f"Error writing to CSV: {e}")


def initialize_error_log(error_file: Path):
    """Initialize the error log file with headers"""
    with open(error_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'file_name', 'file_path', 'error_type'])


def inspect_raw_files(
    file_paths: List[str],
    output_dir: str,
    cores: int = 1,
    limit: Optional[int] = None,
    max_retries: int = 3,
    retry_delay: float = 5.0
) -> str:
    """
    Process multiple raw files and extract metadata with retry logic.
    
    Args:
        file_paths: List of file paths to process
        output_dir: Output directory for results
        cores: Number of cores for parallel processing
        limit: Optional limit on number of files to process
        max_retries: Maximum number of retry attempts for transient errors
        retry_delay: Delay in seconds between retry attempts
    
    Returns:
        Path to the output CSV file
    """
    # Set up paths
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Set up logging
    log_file = setup_logging(output_dir)
    
    # Set up output files with simple names
    output_file = output_dir / "raw_file_inspection_results.csv"
    error_file = output_dir / "raw_file_inspection_errors.csv"
    
    # Remove existing files to start fresh
    if output_file.exists():
        output_file.unlink()
    if error_file.exists():
        error_file.unlink()
    
    # Initialize error log
    initialize_error_log(error_file)
    
    # Process file paths and filter by supported extensions
    file_list = []
    for fp in file_paths:
        path = Path(fp)
        if path.exists() and path.suffix.lower() in ['.raw', '.mzml']:
            file_list.append(path)
    
    if limit is not None:
        file_list = file_list[:limit]
    
    print("=" * 70)
    print("🔬 RAW DATA INSPECTOR - STARTING ANALYSIS")
    print("=" * 70)
    print(f"📊 Processing Configuration:")
    print(f"   Files to inspect: {len(file_list)}")
    print(f"   Processing cores: {cores}")
    print(f"   Max retries per file: {max_retries}")
    print(f"   Retry delay: {retry_delay}s")
    print(f"   CoreMS available: {COREMS_AVAILABLE}")
    if limit:
        print(f"   File limit: {limit}")
    
    print(f"\n📁 Output Configuration:")
    print(f"   Results file: {output_file}")
    print(f"   Error log: {error_file}")
    print(f"   Processing log: {log_file}")
    
    logging.info(f"Found {len(file_list)} files to inspect")
    logging.info(f"Results will be written to: {output_file}")
    logging.info(f"Errors will be logged to: {error_file}")
    logging.info(f"CoreMS available: {COREMS_AVAILABLE}")

    print("\n🔍 File List Preview:")
    for i, file_path in enumerate(file_list[:5], 1):
        size_mb = file_path.stat().st_size / (1024*1024)
        print(f"   {i}. {file_path.name} ({size_mb:.1f} MB)")
    if len(file_list) > 5:
        print(f"   ... and {len(file_list) - 5} more files")
    
    print("\n🚀 Starting file processing...")
    print("=" * 70)

    # Prepare arguments for processing
    process_args = [(file_path, output_file, error_file, max_retries, retry_delay) for file_path in file_list]
    
    successful_count = 0
    start_time = datetime.now()
    
    if cores == 1:
        print("🔄 Sequential processing mode")
        # Sequential processing without tqdm to avoid conflicts with our custom output
        for i, args in enumerate(process_args, 1):
            print(f"\n📋 [{i}/{len(process_args)}] Processing batch...")
            result = process_file_wrapper(args)
            if result and not result.get('error'):
                successful_count += 1
            
            # Show progress every 5 files or at key milestones
            if i % 5 == 0 or i in [1, len(process_args)]:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(process_args) - i) / rate if rate > 0 else 0
                print(f"📈 Progress: {i}/{len(process_args)} ({i/len(process_args)*100:.1f}%) | Rate: {rate:.1f} files/sec | ETA: {eta/60:.1f}min")
    else:
        print(f"⚡ Parallel processing mode with {cores} cores")
        # Parallel processing
        with Pool(processes=cores) as pool:
            results = list(tqdm(
                pool.imap(process_file_wrapper, process_args),
                total=len(process_args),
                desc=f"Inspecting files ({cores} cores)",
                unit="file"
            ))
        
        successful_count = sum(1 for result in results if result and not result.get('error'))
    
    # Final summary
    elapsed_total = (datetime.now() - start_time).total_seconds()
    failed_count = len(file_list) - successful_count
    
    print("=" * 70)
    print("📋 PROCESSING COMPLETE - FINAL SUMMARY")
    print("=" * 70)
    logging.info("Inspection complete!")
    logging.info(f"Successfully processed: {successful_count} files")
    logging.info(f"Failed to process: {failed_count} files")
    
    print(f"✅ Successfully processed: {successful_count} files")
    print(f"❌ Failed to process: {failed_count} files")
    print(f"⏱️  Total processing time: {elapsed_total/60:.1f} minutes")
    print(f"📊 Average rate: {len(file_list)/elapsed_total:.2f} files/sec")
    
    if successful_count > 0:
        logging.info(f"Results saved to: {output_file}")
    if failed_count > 0:
        logging.info(f"Error details saved to: {error_file}")
    
    logging.info(f"Processing log saved to: {log_file}")
    
    return str(output_file)


def main():
    """Command line interface for raw data inspection"""
    parser = argparse.ArgumentParser(description="Inspect raw MS data files and extract metadata")
    parser.add_argument("--files", nargs="+", required=True, help="Raw data file paths to inspect (.raw or .mzML)")
    parser.add_argument("--output-dir", required=True, help="Output directory for results")
    parser.add_argument("--cores", type=int, default=1, help="Number of cores for parallel processing")
    parser.add_argument("--limit", type=int, help="Limit number of files to process (for testing)")
    parser.add_argument("--max-retries", type=int, default=3, help="Maximum number of retry attempts for transient errors")
    parser.add_argument("--retry-delay", type=float, default=5.0, help="Delay in seconds between retry attempts")
    
    args = parser.parse_args()
    
    output_file = inspect_raw_files(
        file_paths=args.files,
        output_dir=args.output_dir,
        cores=args.cores,
        limit=args.limit,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay
    )
    
    print(f"✅ Raw data inspection completed. Results: {output_file}")


if __name__ == "__main__":
    main()