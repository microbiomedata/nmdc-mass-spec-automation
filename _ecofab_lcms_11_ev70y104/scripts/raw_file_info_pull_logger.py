from pathlib import Path
import pandas as pd     
from multiprocessing import Pool
from tqdm import tqdm
import logging
from datetime import datetime
import csv

from corems.mass_spectra.input.mzml import MZMLSpectraParser
from corems.mass_spectra.input.rawFileReader import ImportMassSpectraThermoMSFileReader


def setup_logging(out_dir: Path):
    """Set up logging for error tracking"""
    log_file = out_dir / f"processing_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return log_file

def get_raw_file_info_single(
    file_in
) -> dict:
    """
    Extracts metadata from a raw or mzML file.

    Parameters
    ----------
    file_in : Path
        Path to the raw or mzML file.

    Returns
    -------
    dict
        A dictionary containing metadata about the file.
    """
    try:
        if file_in.suffix.lower() == ".raw":
            parser = ImportMassSpectraThermoMSFileReader(file_in)
        elif file_in.suffix.lower() == ".mzml":
            parser = MZMLSpectraParser(file_in)
        myLCMSobj = parser.get_lcms_obj(spectra='none')

        # Get file metadata
        instrument_info = parser.get_instrument_info()
        serial_number = instrument_info.get("serial_number", "Unknown")
        instrument_model = instrument_info.get("model", "Unknown")
        instrument_name = instrument_info.get("name", "Unknown")
        scan_types = list(set(myLCMSobj.scan_df['ms_format'].to_list()))
        scan_levels = list(set(myLCMSobj.scan_df['ms_level'].to_list()))
        mz_min = myLCMSobj.scan_df['scan_window_lower'].min()
        mz_max = myLCMSobj.scan_df['scan_window_upper'].max()
        rt_min = myLCMSobj.scan_df['scan_time'].min()
        rt_max = myLCMSobj.scan_df['scan_time'].max()
        # pull the digits between @hcd and before the next period in the column "scan_text"
        collision_energies = list(set(myLCMSobj.scan_df['scan_text'].str.extract(r'@hcd(\d+)\.')[0].dropna().unique().tolist()))
        polarity = list(set(myLCMSobj.scan_df['polarity'].tolist()))
        write_time = parser.get_creation_time()
        file_name = file_in.name

        # convert to dict
        file_info = {
            "file_name": file_name,
            "file_path": str(file_in),
            "instrument_model": instrument_model,
            "instrument_name": instrument_name,
            "instrument_serial_number": serial_number,
            "scan_types": scan_types,
            "scan_levels": scan_levels,
            "collision_energies": collision_energies,
            "polarity": polarity,
            "mz_min": mz_min,
            "mz_max": mz_max,
            "rt_min": rt_min,
            "rt_max": rt_max,
            "write_time": write_time,
        }
        return file_info
    except Exception as e:
        error_msg = f"Error processing {file_in.name}: {str(e)}"
        logging.error(error_msg)
        return None

def process_file_wrapper(args):
    """Wrapper function for parallel processing with file writing"""
    file_in, output_file, error_file = args
    
    if not file_in.is_file():
        return None
    
    result = get_raw_file_info_single(file_in)
    
    if result is not None:
        # Write successful result immediately
        write_result_to_csv(result, output_file)
        return True
    else:
        # Log error
        with open(error_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().isoformat(),
                file_in.name,
                str(file_in),
                "Processing failed"
            ])
        return False

def write_result_to_csv(result: dict, output_file: Path):
    """Write a single result to CSV file"""
    file_exists = output_file.exists()
    
    with open(output_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=result.keys())
        
        # Write header if file doesn't exist
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(result)

def initialize_error_log(error_file: Path):
    """Initialize the error log file with headers"""
    with open(error_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'file_name', 'file_path', 'error_type'])

def get_all_raw_file_info(
    file_dir: Path,
    out_dir: Path,
    cores: int = 1,
    n_limit: int = None
):
    """
    Process multiple raw files in parallel and extract metadata.
    Writes results incrementally and logs errors.
    
    Parameters
    ----------
    file_dir : Path
        Directory containing raw files to process.
    out_dir : Path
        Output directory for results.
    cores : int
        Number of cores to use for parallel processing.
    n_limit : int, optional
        Limit the number of files to process. If None, process all files.
        Useful for testing with a subset of files.
    """
    # Make output dir and set up logging
    out_dir.mkdir(parents=True, exist_ok=True)
    log_file = setup_logging(out_dir)
    
    # Set up output files
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = out_dir / f"raw_file_info_{timestamp}.csv"
    error_file = out_dir / f"processing_errors_{timestamp}.csv"
    
    # Initialize error log
    initialize_error_log(error_file)
    
    # Get list of files to process
    files_list = [
        f for f in file_dir.iterdir() if f.suffix.lower() in {".raw", ".mzml"}
    ]
    if n_limit is not None:
        files_list = files_list[:n_limit]
    
    logging.info(f"Found {len(files_list)} files to process")
    logging.info(f"Results will be written to: {output_file}")
    logging.info(f"Errors will be logged to: {error_file}")

    # Prepare arguments for processing
    process_args = [(file_in, output_file, error_file) for file_in in files_list]
    
    successful_count = 0
    
    if cores == 1:
        # Sequential processing
        for args in tqdm(process_args, desc="Processing files", unit="file"):
            result = process_file_wrapper(args)
            if result:
                successful_count += 1
    else:
        # Parallel processing
        with Pool(processes=cores) as pool:
            results = list(tqdm(
                pool.imap(process_file_wrapper, process_args),
                total=len(process_args),
                desc=f"Processing files ({cores} cores)",
                unit="file"
            ))
        
        successful_count = sum(1 for result in results if result)
    
    # Final summary
    failed_count = len(files_list) - successful_count
    logging.info(f"Processing complete!")
    logging.info(f"Successfully processed: {successful_count} files")
    logging.info(f"Failed to process: {failed_count} files")
    
    if successful_count > 0:
        logging.info(f"Results saved to: {output_file}")
    if failed_count > 0:
        logging.info(f"Error details saved to: {error_file}")
        logging.info(f"Processing log saved to: {log_file}")

if __name__ == "__main__":
    file_dir = Path(
        "/Users/heal742/LOCAL/staging"
    )
    out_dir = Path("/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_ecofab_lcms_11_ev70y104/raw_file_info")
    cores = 7

    get_all_raw_file_info(
        file_dir=file_dir,
        out_dir=out_dir,
        cores=cores,
    )