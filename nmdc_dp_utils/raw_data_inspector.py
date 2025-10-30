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


def get_raw_file_info_single(file_path: Path) -> Optional[Dict]:
    """
    Extract metadata from a single raw or mzML file.

    Args:
        file_path: Path to the raw or mzML file

    Returns:
        Dictionary containing file metadata, or None if processing failed
    """
    try:
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
        
        # Get polarity information
        polarity = list(set(lcms_obj.scan_df['polarity'].tolist()))
        
        # Get file creation time
        write_time = parser.get_creation_time()
        
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
        
    except Exception as e:
        error_msg = f"Error processing {file_path.name}: {str(e)}"
        logging.error(error_msg)
        return {
            "file_name": file_path.name,
            "file_path": str(file_path),
            "file_size_bytes": file_path.stat().st_size if file_path.exists() else 0,
            "file_extension": file_path.suffix.lower(),
            "error": str(e),
            "creation_time": datetime.fromtimestamp(file_path.stat().st_ctime).isoformat() if file_path.exists() else None,
        }


def process_file_wrapper(args) -> Optional[Dict]:
    """Wrapper function for parallel processing"""
    file_path, output_file, error_file = args
    
    if not file_path.is_file():
        return None
    
    result = get_raw_file_info_single(file_path)
    
    if result is not None:
        # Write successful result immediately
        write_result_to_csv(result, output_file)
        return result
    else:
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
    limit: Optional[int] = None
) -> str:
    """
    Process multiple raw files and extract metadata.
    
    Args:
        file_paths: List of file paths to process
        output_dir: Output directory for results
        cores: Number of cores for parallel processing
        limit: Optional limit on number of files to process
    
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
    
    logging.info(f"Found {len(file_list)} files to inspect")
    logging.info(f"Results will be written to: {output_file}")
    logging.info(f"Errors will be logged to: {error_file}")
    logging.info(f"CoreMS available: {COREMS_AVAILABLE}")

    # Prepare arguments for processing
    process_args = [(file_path, output_file, error_file) for file_path in file_list]
    
    successful_count = 0
    
    if cores == 1:
        # Sequential processing
        for args in tqdm(process_args, desc="Inspecting files", unit="file"):
            result = process_file_wrapper(args)
            if result and not result.get('error'):
                successful_count += 1
    else:
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
    failed_count = len(file_list) - successful_count
    logging.info(f"Inspection complete!")
    logging.info(f"Successfully processed: {successful_count} files")
    logging.info(f"Failed to process: {failed_count} files")
    
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
    
    args = parser.parse_args()
    
    output_file = inspect_raw_files(
        file_paths=args.files,
        output_dir=args.output_dir,
        cores=args.cores,
        limit=args.limit
    )
    
    print(f"✅ Raw data inspection completed. Results: {output_file}")


if __name__ == "__main__":
    main()