import os
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional
from functools import wraps
import asyncio
import inspect

import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from minio.error import S3Error

from nmdc_dp_utils.llm.llm_pipeline import get_llm_yaml_outline
from nmdc_dp_utils.llm.llm_conversation_manager import ConversationManager
from nmdc_dp_utils.llm.llm_client import LLMClient

# Import workflow mapping defined in workflow_manager (defined before mixins import)
from nmdc_ms_metadata_gen.lcms_metab_metadata_generator import (
    LCMSMetabolomicsMetadataGenerator,
)
from nmdc_ms_metadata_gen.lcms_lipid_metadata_generator import (
    LCMSLipidomicsMetadataGenerator,
)
from nmdc_ms_metadata_gen.gcms_metab_metadata_generator import (
    GCMSMetabolomicsMetadataGenerator,
)
from nmdc_ms_metadata_gen.material_processing_generator import (
    MaterialProcessingMetadataGenerator,
)



# Workflow configuration mapping used across manager and mixins
WORKFLOW_DICT = {
    "LCMS Metabolomics": {
        "wdl_workflow_name": "metaMS_lcms_metabolomics",
        "wdl_download_location": "https://raw.githubusercontent.com/microbiomedata/metaMS/master/wdl/metaMS_lcms_metabolomics.wdl",
        "generator_method": "_generate_lcms_metab_wdl",
        "workflow_metadata_input_generator": "_generate_lcms_workflow_metadata_inputs",
        "metadata_generator_class": LCMSMetabolomicsMetadataGenerator,
        "raw_data_inspector": "raw_data_inspector",
    },
    "LCMS Lipidomics": {
        "wdl_workflow_name": "metaMS_lcms_lipidomics",
        "wdl_download_location": "https://raw.githubusercontent.com/microbiomedata/metaMS/master/wdl/metaMS_lcmslipidomics.wdl",
        "generator_method": "_generate_lcms_lipid_wdl",
        "workflow_metadata_input_generator": "_generate_lcms_workflow_metadata_inputs",
        "metadata_generator_class": LCMSLipidomicsMetadataGenerator,
        "raw_data_inspector": "raw_data_inspector",
    },
    "GCMS Metabolomics": {
        "wdl_workflow_name": "metaMS_gcms",
        "wdl_download_location": "https://raw.githubusercontent.com/microbiomedata/metaMS/master/wdl/metaMS_gcms.wdl",
        "generator_method": "_generate_gcms_metab_wdl",
        "workflow_metadata_input_generator": "_generate_gcms_workflow_metadata_inputs",
        "metadata_generator_class": GCMSMetabolomicsMetadataGenerator,
        "raw_data_inspector": "gcms_data_inspector",
    },
}

# Load environment variables from .env file
load_dotenv()


def skip_if_complete(trigger_name: str, return_value=None):
    """
    Decorator to skip a method if the specified trigger is set to True.
    
    Compatible with both sync and async functions.

    Args:
        trigger_name: Name of the skip trigger to check
        return_value: Value to return if skipping (default: None)

    Usage:
        @skip_if_complete('data_processed', return_value=0)
        def some_method(self):
            # method implementation
            
        @skip_if_complete('protocol_outline_created', return_value=True)
        async def async_method(self):
            # async method implementation
    """

    def decorator(func):
        # Check if the function is async
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(self, *args, **kwargs):
                if self.should_skip(trigger_name):
                    self.logger.info(
                        f"Skipping {func.__name__} ({trigger_name} already complete)"
                    )
                    return return_value
                return await func(self, *args, **kwargs)
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(self, *args, **kwargs):
                if self.should_skip(trigger_name):
                    self.logger.info(
                        f"Skipping {func.__name__} ({trigger_name} already complete)"
                    )
                    return return_value
                return func(self, *args, **kwargs)
            return sync_wrapper

    return decorator


class WorkflowDataMovementManager:
    """
    Mixin class providing data movement utilities for NMDC workflows.
    """

    def _crawl_massive_ftp(self, massive_id: str) -> str:
        """
        Crawl MASSIVE FTP directory to discover all data files recursively.

        Uses Python's ftplib to connect to massive-ftp.ucsd.edu and recursively
        traverse the dataset directory structure, collecting URLs for files
        matching the configured file type extension.

        Args:
            massive_id: MASSIVE dataset identifier including version path
                       (e.g., 'v07/MSV000094090')

        Returns:
            Path to the log file containing discovered FTP URLs

        Note:
            This method can take several minutes for large datasets.
            Progress is reported every 100 files discovered.
            File type is determined by config['study']['file_type'] (e.g., '.raw', '.mzml', '.d')
        """
        import ftplib

        log_file = self.workflow_path / "raw_file_info" / "massive_ftp_locs.txt"

        self.logger.info(f"Crawling MASSIVE FTP directory for dataset: {massive_id}")

        ftp_urls = []

        try:
            # Connect to MASSIVE FTP server
            ftp = ftplib.FTP("massive-ftp.ucsd.edu")
            ftp.login()  # Anonymous login

            # Navigate to the study directory (massive_id should include version path)
            try:
                ftp.cwd(massive_id)
            except ftplib.error_perm:
                self.logger.error(
                    f"Could not access {massive_id} - check that the path includes version (e.g., 'v07/MSV000094090')"
                )
                return []

            def collect_files(relative_path=""):
                """Recursively collect files from FTP directory."""
                try:
                    # Get list of items in current directory
                    items = []
                    ftp.retrlines("LIST", items.append)

                    for item in items:
                        # Parse the LIST output (Unix format)
                        parts = item.split()
                        if len(parts) >= 9:
                            permissions = parts[0]
                            filename = " ".join(
                                parts[8:]
                            )  # Handle filenames with spaces

                            if permissions.startswith("d"):
                                # It's a directory, recurse into it
                                current_dir = ftp.pwd()  # Save current directory
                                try:
                                    ftp.cwd(filename)  # Change to subdirectory
                                    new_relative_path = (
                                        f"{relative_path}/{filename}"
                                        if relative_path
                                        else filename
                                    )
                                    collect_files(new_relative_path)
                                    ftp.cwd(current_dir)  # Go back to parent directory
                                except ftplib.error_perm as e:
                                    self.logger.debug(
                                        f"Cannot access directory {filename}: {e}"
                                    )
                            else:
                                # It's a file, check if it matches the configured file type
                                file_type = (
                                    self.config["study"]
                                    .get("file_type", ".raw")
                                    .lower()
                                )
                                if filename.lower().endswith(file_type):
                                    current_path = (
                                        f"{massive_id}/{relative_path}"
                                        if relative_path
                                        else massive_id
                                    )
                                    full_url = f"ftp://massive-ftp.ucsd.edu/{current_path}/{filename}"
                                    ftp_urls.append(full_url)
                                    if len(ftp_urls) % 100 == 0:
                                        self.logger.info(
                                            f"Found {len(ftp_urls)} {file_type} files..."
                                        )

                except ftplib.error_perm as e:
                    # Permission denied or directory doesn't exist
                    self.logger.error(f"Cannot access current directory: {e}")
                except Exception as e:
                    self.logger.error(f"Error processing current directory: {e}")

            # Start crawling from the dataset root
            collect_files()
            ftp.quit()

            # Write URLs to log file
            with open(log_file, "w") as f:
                for url in ftp_urls:
                    f.write(f"{url}\n")

            file_type = self.config["study"].get("file_type", ".raw").lower()
            self.logger.info(f"Found {len(ftp_urls)} {file_type} files")

            return str(log_file)

        except Exception as e:
            self.logger.error(f"Error crawling FTP: {e}")
            # Create empty log file
            with open(log_file, "w") as f:
                f.write("# No files found - FTP crawling failed\n")
            return str(log_file)

    def parse_massive_ftp_log(
        self, log_file: Optional[str] = None, output_file: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Parse FTP crawl log file to extract URLs and create a structured DataFrame.

        Processes the text file created by _crawl_massive_ftp() to extract FTP URLs
        and create a pandas DataFrame with location and filename information.
        Applies workflow-specific file filters if configured.

        Args:
            log_file: Path to FTP crawl log file (uses default if not provided)
            output_file: Optional filename to save CSV results (defaults to study name)

        Returns:
            DataFrame with columns:
            - ftp_location: Full FTP URL for each file
            - raw_data_file_short: Just the filename portion

        File Type and Filtering Details:
            First filters by config['workflow']['file_type'] (e.g., '.raw', '.mzml', '.d')
            to collect only files of the specified type. Then uses
            config['workflow']['file_filters'] list to filter filenames. Files are
            KEPT if their filename contains ANY of the filter keywords (OR logic).

            Example: file_type = '.raw', file_filters = ['pos', 'neg', 'hilic']
            - 'sample_hilic_pos.raw' → KEPT (is .raw AND matches 'pos'/'hilic')
            - 'sample_hilic_pos.mzml' → EXCLUDED (wrong file type)
            - 'blank_rp_neutral.raw' → EXCLUDED (is .raw but no filter matches)
            - 'qc_neg_check.raw' → KEPT (is .raw AND matches 'neg')

            Filter matching is case-insensitive. If no file_filters are specified,
            ALL files of the configured type are returned (potentially thousands).
        """
        if log_file is None:
            log_file = self.workflow_path / "raw_file_info" / "massive_ftp_locs.txt"

        if output_file is None:
            output_file = "raw_file_info/massive_ftp_locs.csv"

        output_path = self.workflow_path / output_file

        self.logger.info(f"Parsing FTP log file: {log_file}")

        ftp_locs = []

        try:
            # Get the configured file type
            file_type = self.config["study"].get("file_type", ".raw").lower()

            with open(log_file, "r") as f:
                for line in f:
                    # Look for lines ending with the configured file extension
                    if line.rstrip().lower().endswith(file_type):
                        # Extract the FTP URL (should be the entire line for our format)
                        ftp_url = line.strip()
                        if ftp_url.startswith("ftp://"):
                            ftp_locs.append(ftp_url)

            # Remove duplicates and create DataFrame
            ftp_locs = list(set(ftp_locs))
            ftp_df = pd.DataFrame(ftp_locs, columns=["ftp_location"])

            # Extract filename from URL - use configured file type for pattern
            file_type = (
                self.config["study"].get("file_type", ".raw").lower().lstrip(".")
            )
            pattern = rf"([^/]+\.{file_type})$"
            ftp_df["raw_data_file_short"] = ftp_df["ftp_location"].str.extract(
                pattern, flags=re.IGNORECASE
            )[0]

            # Apply file filters if specified
            if "file_filters" in self.config["workflow"] and len(ftp_df) > 0:
                filter_pattern = "|".join(self.config["workflow"]["file_filters"])
                ftp_df = ftp_df[
                    ftp_df["raw_data_file_short"].str.contains(
                        filter_pattern, na=False, case=False
                    )
                ]
            elif len(ftp_df) > 0:
                self.logger.warning(
                    f"No file_filters configured - returning ALL {len(ftp_df)} files"
                )
                self.logger.warning(
                    "Consider adding 'file_filters' to config to avoid downloading unnecessary files"
                )

            # Save to CSV
            ftp_df.to_csv(output_path, index=False)

            return ftp_df

        except FileNotFoundError:
            self.logger.error(
                f"Log file not found: {log_file}, run crawl_massive_ftp() first"
            )
            return pd.DataFrame(columns=["ftp_location", "raw_data_file_short"])
        except Exception as e:
            self.logger.error(f"Error parsing log file: {e}")
            return pd.DataFrame(columns=["ftp_location", "raw_data_file_short"])

    @skip_if_complete("raw_data_downloaded", return_value=True)
    def get_massive_ftp_urls(self, massive_id: Optional[str] = None) -> bool:
        """
        Complete workflow to discover and catalog MASSIVE dataset files with filtering.

        Performs a three-step process:
        1. Crawls the MASSIVE FTP server to discover files of the configured type
        2. Filters by file type first (config['workflow']['file_type'])
        3. Applies workflow-specific filters to reduce the dataset to relevant files only

        IMPORTANT - File Type Selection:
        Uses config['workflow']['file_type'] to specify which file extension to collect
        (e.g., '.raw', '.mzml', '.d'). Only files of this type are discovered.

        IMPORTANT - File Filtering:
        The filtering step uses config['workflow']['file_filters'] to dramatically reduce
        the number of files from potentially thousands to only those matching your
        workflow's specific criteria. Files are kept if their filename contains ANY of
        the filter keywords (case-insensitive).

        Example configuration and filtering:
        - config['workflow']['file_type'] = '.raw'  # Only collect .raw files
        - config['workflow']['file_filters'] = ['pos', 'neg', 'hilic']
        - File 'sample_hilic_pos_01.raw' → KEPT (is .raw AND contains 'pos' and 'hilic')
        - File 'sample_rp_neutral_01.mzml' → EXCLUDED (wrong file type)
        - File 'sample_rp_neutral_01.raw' → EXCLUDED (right type but no matching keywords)
        - File 'blank_neg_02.raw' → KEPT (is .raw AND contains 'neg')

        Args:
            massive_id: MASSIVE dataset ID with version path (e.g., 'v07/MSV000094090').
                       Uses config['workflow']['massive_id'] if not provided.

        Returns:
            True if discovery and cataloging completed successfully, False otherwise

        Note:
            Results are saved to CSV file at workflow_path/raw_file_info/massive_ftp_locs.csv.
            This method is automatically skipped if raw_data_downloaded trigger is set.

        Warning:
            Without proper file_filters configuration, this method could discover
            thousands of files. Always verify your file_filters are configured
            correctly for your specific study needs.

        Example:
            >>> manager = NMDCWorkflowManager('config.json')
            >>> # Ensure config has: "file_filters": ["pos", "neg", "hilic"]
            >>> success = manager.get_massive_ftp_urls()
            >>> # Results saved to: workflow_path/raw_file_info/massive_ftp_locs.csv
        """
        if massive_id is None:
            massive_id = self.config["workflow"]["massive_id"]

        # Step 1: Crawl FTP
        try:
            log_file = self._crawl_massive_ftp(massive_id)
            # Step 2: Parse log and get filtered results
            filtered_df = self.parse_massive_ftp_log(log_file)

            # Step 3: Report filtering results with sample files
            file_type = self.config["study"].get("file_type", ".raw")
            file_filters = self.config["study"].get("file_filters", [])

            if len(filtered_df) > 0:
                self.logger.info(
                    f"Kept {len(filtered_df)} files for downloading after applying both file type and filter criteria"
                )

                # Show 4 random sample files
                sample_size = min(4, len(filtered_df))
                sample_files = filtered_df["raw_data_file_short"].sample(
                    n=sample_size, random_state=42
                )
                self.logger.info("  Sample of kept files:")
                for i, filename in enumerate(sample_files, 1):
                    self.logger.info(f"    {i}. {filename}")

                if len(filtered_df) > 4:
                    self.logger.info(f"    ... and {len(filtered_df) - 4} more files")
            else:
                self.logger.error("No files matched the criteria!")
                self.logger.error(f"  File type: {file_type}")
                self.logger.error(f"  File filters: {file_filters}")
                self.logger.error(
                    "  Check your file_type and file_filters configuration"
                )

            return True

        except Exception as e:
            self.logger.error(f"Error in MASSIVE FTP process: {e}")
            return False

    @skip_if_complete("raw_data_downloaded", return_value=True)
    def download_from_massive(
        self,
        ftp_file: Optional[str] = None,
        download_dir: Optional[str] = None,
        massive_id: Optional[str] = None,
    ) -> bool:
        """
        Download raw data files from MASSIVE dataset via FTP.

        Downloads files using FTP URLs either from a pre-generated file or by
        directly querying the MASSIVE dataset. Provides progress tracking and
        skips files that already exist locally.

        Args:
            ftp_file: Path to CSV/text file containing FTP locations. Can be
                     either CSV format (with 'ftp_location' column) or plain text
                     format (one URL per line). Optional if massive_id provided.
            download_dir: Local directory to download files to. Uses
                         self.raw_data_directory if not provided.
            massive_id: MASSIVE dataset ID to query directly. Uses
                       config['study']['massive_id'] if not provided.

        Returns:
            True if download completed successfully, False otherwise

        Note:
            Files are downloaded using urllib.request.urlretrieve for reliability.
            Existing files with matching names are skipped to avoid re-downloading.
            Downloaded file list is saved to metadata/downloaded_files.csv.
            This method is automatically skipped if raw_data_downloaded trigger is set.
        """
        if download_dir is None:
            download_dir = self.raw_data_directory

        # Get FTP URLs either from file or by querying MASSIVE
        if massive_id:
            # Call to discover and save URLs to CSV
            self.get_massive_ftp_urls(massive_id)
            # Read the saved CSV
            ftp_csv = self.workflow_path / "raw_file_info" / "massive_ftp_locs.csv"
            ftp_df = (
                pd.read_csv(ftp_csv)
                if ftp_csv.exists()
                else pd.DataFrame(columns=["ftp_location", "raw_data_file_short"])
            )
        elif ftp_file:
            ftp_path = self.workflow_path / ftp_file
            if ftp_path.suffix == ".csv":
                ftp_df = pd.read_csv(ftp_path)
            else:
                # Handle text file format
                with open(ftp_path, "r") as f:
                    lines = f.readlines()
                ftp_df = self._parse_ftp_file(lines)
        else:
            # Try to use MASSIVE ID from config
            if "massive_id" in self.config["workflow"]:
                # Call to discover and save URLs to CSV
                if not self.get_massive_ftp_urls():
                    self.logger.error("Failed to discover MASSIVE files")
                    return False
                # Read the saved CSV
                ftp_csv = self.workflow_path / "raw_file_info" / "massive_ftp_locs.csv"
                ftp_df = (
                    pd.read_csv(ftp_csv)
                    if ftp_csv.exists()
                    else pd.DataFrame(columns=["ftp_location", "raw_data_file_short"])
                )
            else:
                self.logger.error("Either ftp_file or massive_id must be provided")
                return False

        if len(ftp_df) == 0:
            self.logger.error("No files to download")
            return True  # Not an error, just nothing to do

        os.makedirs(download_dir, exist_ok=True)
        downloaded_files = []

        self.logger.info(f"Starting download of {len(ftp_df)} files...")
        for index, row in tqdm(
            ftp_df.iterrows(), total=len(ftp_df), desc="Downloading files"
        ):
            ftp_location = row["ftp_location"]
            file_name = row["raw_data_file_short"]
            download_path = os.path.join(download_dir, file_name)

            # Check if file already exists
            if os.path.exists(download_path):
                tqdm.write(f"File {file_name} already exists. Skipping download.")
                downloaded_files.append(download_path)
                continue

            try:
                self._download_file_wget(ftp_location, download_path)
                downloaded_files.append(download_path)
                tqdm.write(f"Downloaded {file_name}")
            except Exception as e:
                tqdm.write(f"Error downloading {file_name}: {e}")

        self.logger.info(
            f"Downloaded {len([f for f in downloaded_files if os.path.exists(f)])} files successfully"
        )

        # Write CSV of downloaded file names for biosample mapping
        if len(downloaded_files) > 0:
            downloaded_files_csv = (
                self.workflow_path / "metadata" / "downloaded_files.csv"
            )
            os.makedirs(downloaded_files_csv.parent, exist_ok=True)

            # Create DataFrame with downloaded file information
            file_data = []
            for file_path in downloaded_files:
                if os.path.exists(file_path):
                    file_data.append(
                        {
                            "file_path": file_path,
                            "file_name": os.path.basename(file_path),
                            "file_size_bytes": os.path.getsize(file_path)
                            if os.path.exists(file_path)
                            else 0,
                        }
                    )

            if file_data:
                download_df = pd.DataFrame(file_data)
                download_df.to_csv(downloaded_files_csv, index=False)

            self.set_skip_trigger("raw_data_downloaded", True)

        return True

    def _download_file_wget(self, ftp_location: str, download_path: str):
        """
        Download a single file using Python's urllib.

        Args:
            ftp_location: FTP URL of the file to download
            download_path: Local path where the file should be saved

        Raises:
            RuntimeError: If the download fails for any reason
        """
        import urllib.request
        import urllib.error

        try:
            # Download the file using urllib
            urllib.request.urlretrieve(ftp_location, download_path)
        except urllib.error.URLError as e:
            raise RuntimeError(f"Failed to download {ftp_location}: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error downloading {ftp_location}: {e}")

    def _parse_ftp_file(self, lines: List[str]) -> pd.DataFrame:
        """
        Parse text-format FTP file into DataFrame.

        Handles legacy text format FTP files with tab-separated values.

        Args:
            lines: List of lines from the FTP file

        Returns:
            DataFrame with ftp_location and raw_data_file_short columns
        """
        # Implement based on existing format
        data = []
        for line in lines:
            if line.strip() and not line.startswith("#"):
                # Parse line based on format
                parts = line.strip().split("\t")  # Adjust based on actual format
                if len(parts) >= 2:
                    data.append(
                        {"ftp_location": parts[0], "raw_data_file_short": parts[1]}
                    )
        return pd.DataFrame(data)

    def upload_to_minio(
        self,
        local_directory: str,
        bucket_name: str,
        folder_name: str,
        file_pattern: str = "*",
    ) -> int:
        """
        Upload files from local directory to MinIO object storage.

        Recursively uploads files matching the specified pattern to MinIO,
        preserving directory structure within the target folder.

        Args:
            local_directory: Local directory containing files to upload
            bucket_name: MinIO bucket name (must already exist)
            folder_name: Folder name within bucket (will be created if needed)
            file_pattern: Glob pattern to match files (default: "*" for all files)

        Returns:
            Number of files successfully uploaded

        Raises:
            ValueError: If MinIO client is not initialized or local directory doesn't exist
            S3Error: If MinIO operations fail

        Example:
            >>> manager.upload_to_minio('/path/to/processed', 'metabolomics', 'study_data')
        """
        if not self.minio_client:
            raise ValueError(
                "MinIO client not available. Please set MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables."
            )

        local_path = Path(local_directory)
        if not local_path.exists():
            raise ValueError(f"Local directory {local_directory} does not exist")

        # Collect files to upload
        files_to_upload = list(local_path.rglob(file_pattern))
        files_to_upload = [f for f in files_to_upload if f.is_file()]

        uploaded_count = 0

        self.logger.info(
            f"Uploading {len(files_to_upload)} files to {bucket_name}/{folder_name}"
        )

        for file_path in tqdm(files_to_upload, desc="Uploading files"):
            # Create object name preserving directory structure
            relative_path = file_path.relative_to(local_path)
            object_name = f"{folder_name}/{relative_path}"

            try:
                # Check if file already exists with same size
                try:
                    stat = self.minio_client.stat_object(bucket_name, object_name)
                    if stat.size == file_path.stat().st_size:
                        continue  # Skip if same size
                except S3Error:
                    pass  # File doesn't exist, proceed with upload

                self.minio_client.fput_object(bucket_name, object_name, str(file_path))
                uploaded_count += 1

            except S3Error as e:
                self.logger.error(f"Failed to upload {file_path}: {e}")

        self.logger.info(f"Successfully uploaded {uploaded_count} files")
        return uploaded_count

    def download_from_minio(
        self, bucket_name: str, folder_name: str, local_directory: str
    ) -> int:
        """
        Download files from MinIO object storage to local directory.

        Downloads all files from the specified bucket/folder combination,
        recreating the directory structure locally. Skips files that already
        exist locally with the same size.

        Args:
            bucket_name: MinIO bucket name
            folder_name: Folder name within bucket
            local_directory: Local directory to download files to (created if needed)

        Returns:
            Number of new files downloaded (excludes skipped existing files)

        Raises:
            ValueError: If MinIO client is not initialized
            S3Error: If MinIO operations fail

        Example:
            >>> count = manager.download_from_minio('metabolomics', 'study_data', '/local/path')
            >>> print(f"Downloaded {count} files")
        """
        if not self.minio_client:
            raise ValueError(
                "MinIO client not available. Please set MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables."
            )

        # Create local directory
        Path(local_directory).mkdir(parents=True, exist_ok=True)

        # List objects in folder
        objects = self.minio_client.list_objects(
            bucket_name, prefix=folder_name, recursive=True
        )

        all_objects = [obj for obj in objects if not obj.object_name.endswith("/")]
        downloaded_count = 0

        for obj in tqdm(all_objects, desc="Downloading files"):
            # Create local file path
            relative_path = obj.object_name[len(folder_name) :].lstrip("/")
            local_file_path = os.path.join(local_directory, relative_path)

            # Create subdirectories if needed
            Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)

            # Check if file exists and has same size
            if os.path.exists(local_file_path):
                local_size = os.path.getsize(local_file_path)
                if local_size == obj.size:
                    continue  # Skip existing files

            try:
                self.minio_client.fget_object(
                    bucket_name, obj.object_name, local_file_path
                )
                downloaded_count += 1
            except S3Error as e:
                self.logger.error(f"Error downloading {obj.object_name}: {e}")

        self.logger.info(f"Downloaded {downloaded_count} new files")
        return downloaded_count

    @skip_if_complete("raw_data_downloaded", return_value=True)
    def download_raw_data_from_minio(
        self, bucket_name: Optional[str] = None, folder_name: Optional[str] = None
    ) -> bool:
        """
        Download raw data files from MinIO object storage.

        Downloads files from MinIO bucket/folder using configuration settings,
        similar to download_from_massive() for consistency. Provides progress tracking
        and skips files that already exist locally.

        Args:
            bucket_name: MinIO bucket name. Uses config['minio']['bucket'] if not provided.
            folder_name: Folder path within bucket. Uses config['study']['name'] + '/raw'
                        if not provided.

        Returns:
            True if download completed successfully, False otherwise

        Note:
            Files are downloaded to self.raw_data_directory. Existing files with
            matching sizes are skipped to avoid re-downloading.
            Downloaded file list is saved to metadata/downloaded_files.csv.
            This method is automatically skipped if raw_data_downloaded trigger is set.

        Example:
            >>> manager = NMDCWorkflowManager('config.json')
            >>> success = manager.download_raw_data_from_minio()
        """
        if not self.minio_client:
            self.logger.error(
                "MinIO client not available. Please set MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables."
            )
            return False

        # Use config values if not provided
        if bucket_name is None:
            bucket_name = self.config.get("minio", {}).get("bucket")
            if not bucket_name:
                self.logger.error(
                    "bucket_name not provided and not found in config['minio']['bucket']"
                )
                return False

        if folder_name is None:
            folder_name = f"{self.config['study']['name']}/raw"

        # Download files using the core download_from_minio method
        _ = self.download_from_minio(
            bucket_name=bucket_name,
            folder_name=folder_name,
            local_directory=str(self.raw_data_directory),
        )

        # Get list of downloaded files
        file_type = self.config["workflow"].get("file_type")
        downloaded_files = [
            os.path.join(self.raw_data_directory, f)
            for f in os.listdir(self.raw_data_directory)
            if f.endswith(file_type)
        ]

        # Write CSV of downloaded file names for biosample mapping
        if len(downloaded_files) > 0:
            downloaded_files_csv = (
                self.workflow_path / "metadata" / "downloaded_files.csv"
            )
            downloaded_files_csv.parent.mkdir(parents=True, exist_ok=True)

            # Create DataFrame with just filenames
            df = pd.DataFrame(
                {"raw_data_file_short": [os.path.basename(f) for f in downloaded_files]}
            )
            df.to_csv(downloaded_files_csv, index=False)
            self.logger.info(
                f"Saved list of downloaded files to {downloaded_files_csv}"
            )

            # Set skip trigger for raw_data_downloaded
            self.set_skip_trigger("raw_data_downloaded", True)

        return True

    @skip_if_complete("raw_data_downloaded", return_value=True)
    def fetch_raw_data(self) -> bool:
        """
        Fetch raw data from configured source (MASSIVE or MinIO).

        This is a consolidated method that determines the data source from configuration
        and calls the appropriate download method. The source is determined by:
        - If 'massive_id' exists in config['workflow']: downloads from MASSIVE
        - Otherwise: downloads from MinIO

        Returns:
            True if data fetching completed successfully, False otherwise

        Note:
            This method is automatically skipped if raw_data_downloaded trigger is set.

        Example:
            >>> manager = NMDCWorkflowManager('config.json')
            >>> success = manager.fetch_raw_data()
        """
        # Determine source from configuration
        if "massive_id" in self.config.get("workflow", {}):
            # Get FTP URLs and download
            if not self.get_massive_ftp_urls():
                return False
            return self.download_from_massive()
        else:
            return self.download_raw_data_from_minio()

    def _move_processed_files(self, working_dir: str, clean_up: bool = True) -> None:
        """
        Move processed output files from working directory to designated processed data location.

        Handles both LCMS and GCMS workflow outputs:
        - LCMS: Searches for .corems directories and moves them to processed directory
        - GCMS: Searches for CSV files in out/output_files/ structure and copies them directly to processed directory

        After attempting to move files, optionally cleans up the WDL execution directory
        to keep the study directory clean and prevent accumulation of large temporary files.
        Cleanup occurs only if clean_up is True.

        Args:
            working_dir: Directory where WDL workflows were executed and output files created
            clean_up: Whether to remove the WDL execution directory after attempting file moves (default: True)

        Note:
            Uses self.processed_data_directory as the destination.
            Creates the destination directory if it doesn't exist.
            Validates output files belong to this study by matching filenames with raw data files.
        """
        import shutil

        working_path = Path(working_dir)
        processed_data_dir = self.processed_data_directory

        if not processed_data_dir:
            self.logger.warning(
                "processed_data_directory not configured - skipping file move"
            )
            return

        processed_path = Path(processed_data_dir)

        # Create processed data directory if it doesn't exist
        if not processed_path.exists():
            processed_path.mkdir(parents=True, exist_ok=True)

        # Get list of raw files for this study to validate outputs belong to this study
        raw_data_dir = self.raw_data_directory
        study_raw_files = set()
        if raw_data_dir and Path(raw_data_dir).exists():
            file_type = self.config["study"].get("file_type", ".raw")
            raw_files = list(Path(raw_data_dir).rglob(f"*{file_type}"))
            study_raw_files = {
                f.stem for f in raw_files
            }  # Get filenames without extension

        moved_count = 0

        # Determine workflow type to use appropriate file moving strategy
        workflow_type = self.config["workflow"]["workflow_type"]

        if workflow_type in ["LCMS Metabolomics", "LCMS Lipidomics"]:
            # LCMS: Search for .corems directories
            for dirpath in working_path.rglob("*"):
                if dirpath.is_dir() and dirpath.name.endswith(".corems"):
                    # Check that there is a .csv within the directory (indicates successful processing)
                    csv_files = list(dirpath.glob("*.csv"))
                    if not csv_files:
                        self.logger.warning(
                            f"No .csv files found in {dirpath.name}, skipping."
                        )
                        continue

                    # Validate this .corems directory belongs to our study by checking the filename
                    corems_filename = dirpath.name.replace(".corems", "")
                    if study_raw_files and corems_filename not in study_raw_files:
                        self.logger.warning(
                            f"{dirpath.name} does not match any raw files for study {self.study_name}, skipping."
                        )
                        continue

                    # Move the entire .corems directory to processed location
                    destination = processed_path / dirpath.name

                    # Handle case where destination already exists (silent skip)
                    if destination.exists():
                        continue

                    try:
                        shutil.move(str(dirpath), str(destination))
                        moved_count += 1
                    except Exception as e:
                        self.logger.error(f"Failed to move {dirpath.name}: {e}")

        elif workflow_type == "GCMS Metabolomics":
            # GCMS: Search for CSV files in out/output_files/ structure
            # Pattern: <timestamp>_gcmsMetabolomics/out/output_files/<number>/<filename>.csv
            for csv_file in working_path.rglob("out/output_files/*/*.csv"):
                # Get the base filename (without extension)
                base_filename = csv_file.stem

                # Validate this file belongs to our study
                if study_raw_files and base_filename not in study_raw_files:
                    self.logger.warning(
                        f"{csv_file.name} does not match any raw files for study {self.study_name}, skipping."
                    )
                    continue

                # Move CSV file directly to processed data directory
                destination_file = processed_path / csv_file.name

                # Handle case where destination file already exists (silent skip)
                if destination_file.exists():
                    continue

                try:
                    shutil.copy2(str(csv_file), str(destination_file))
                    moved_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to copy {csv_file.name}: {e}")

        if moved_count > 0:
            # Report total processed files in destination
            total_corems = len(list(processed_path.glob("*.corems")))
            self.logger.info(f"Total processed files in destination: {total_corems}")
        else:
            self.logger.info("No processed files were moved")

        # Optionally clean up the WDL execution directory after attempting to move files
        if clean_up:
            self._cleanup_wdl_execution_dir(working_dir)
        else:
            self.logger.debug(
                "Skipping cleanup of WDL execution directory (clean_up=False)"
            )


class NMDCWorkflowDataProcessManager:
    """
    Mixin class providing WDL workflow data processing utilities for NMDC workflows.
    """

    @skip_if_complete("data_processed", return_value=True)
    def process_data(self, execute: bool = True, cleanup: bool = True) -> bool:
        """
        Generate WDL configurations and optionally execute workflow processing.

        This is a consolidated method that handles the complete WDL workflow execution:
        1. Generates WDL JSON configuration files from mapped raw data
        2. Generates the shell script to run the workflows
        3. Optionally executes the workflows and processes the data

        Args:
            execute: Whether to execute the WDL workflows after generating configs (default: True)
            cleanup: Whether to clean up temporary execution directory after success (default: True)

        Returns:
            True if all steps completed successfully, False otherwise

        Note:
            This method is automatically skipped if data_processed trigger is set.
            Batch size and other parameters are read from configuration.

        Example:
            >>> manager = NMDCWorkflowManager('config.json')
            >>> # Generate configs only (don't execute)
            >>> success = manager.process_data(execute=False)
            >>>
            >>> # Generate and execute
            >>> success = manager.process_data(execute=True)
        """

        # Step 1: Generate WDL JSON files
        if not self.generate_wdl_jsons():
            self.logger.error("Failed to generate WDL JSON files")
            return False

        # Step 2: Generate runner script
        if not self.generate_wdl_runner_script():
            self.logger.error("Failed to generate WDL runner script")
            return False

        # Step 3: Execute workflows (optional)
        if execute:
            if not self.run_wdl_script():
                self.logger.error("WDL workflow execution failed")
                return False
        else:
            self.logger.info("Skipping WDL execution (execute=False)")

        return True

    @skip_if_complete("data_processed", return_value=True)
    def generate_wdl_jsons(self, batch_size: int = 50) -> bool:
        """
        Generate WDL workflow JSON configuration files for batch processing.

        Returns:
            True if WDL generation completed successfully, False otherwise

        First moves any processed data from previous WDL execution attempts to ensure
        the processed data directory is up-to-date, then creates JSON files for each
        processing configuration defined in the config, organizing raw data files into
        batches of the specified size. Files are filtered for each configuration based
        on the configuration name (e.g., 'hilic_pos' will only include files with
        'hilic' and 'pos' in the filename).

        Args:
            batch_size: Maximum number of files per batch (default: 50)

        Raises:
            ValueError: If workflow_type is not set in config
            NotImplementedError: If workflow_type is not yet supported

        File Filtering per Configuration:
            Each configuration can specify its own file_filter list to determine
            which files should be processed. Files must contain ALL filter terms
            specified in the configuration's file_filter.

            Example: Configuration with file_filter: ['hilic', 'pos'] will only
            include files containing both 'hilic' AND 'pos' in their filename
            (case-insensitive). If no file_filter is specified, all files are included.

        Note:
            JSON files are created in the study's wdl_jsons directory, organized
            by configuration name (e.g., wdl_jsons/hilic_pos/batch1.json).
            Each JSON follows the MetaMS WDL workflow input specification.
            Automatically moves processed data from previous runs and cleans up
            WDL execution directories before generating new configurations.

        Example:
            >>> manager.generate_wdl_jsons(batch_size=25)
        """

        # First, move any processed data from previous WDL execution attempts
        # This ensures the processed data directory is up-to-date before we check for already-processed files
        wdl_execution_dir = self.workflow_path / "wdl_execution"
        if wdl_execution_dir.exists():
            self._move_processed_files(str(wdl_execution_dir))

        # Always empty the wdl_jsons directory first
        wdl_jsons_path = self.workflow_path / "wdl_jsons"
        if wdl_jsons_path.exists():
            shutil.rmtree(wdl_jsons_path)
        wdl_jsons_path.mkdir(parents=True, exist_ok=True)

        # Use mapped files list if available (only files that map to biosamples)
        mapped_files_csv = self.workflow_path / "metadata" / "mapped_raw_files.csv"

        if mapped_files_csv.exists():
            import pandas as pd

            mapped_df = pd.read_csv(mapped_files_csv)
            raw_files = [
                Path(file_path)
                for file_path in mapped_df["raw_file_path"]
                if Path(file_path).exists()
            ]
        else:
            raise FileNotFoundError(f"Mapped files list not found: {mapped_files_csv}")

        # Remove any problem_files (from config) from list of raw_files
        problem_files = self.config.get("problem_files", [])
        if problem_files:
            initial_count = len(raw_files)
            raw_files = [f for f in raw_files if f.name not in problem_files]

        # Filter out already-processed files by checking for processed outputs
        # IMPORTANT: Calibration files should never be filtered out as they are reference files, not samples
        processed_data_dir = self.processed_data_directory
        workflow_type = self.config["workflow"]["workflow_type"]

        # Load biosample mapping to identify calibration files (for GCMS workflow)
        calibration_files_set = set()
        if workflow_type == "GCMS Metabolomics":
            mapping_file = (
                self.workflow_path
                / "metadata"
                / "mapped_raw_file_biosample_mapping.csv"
            )
            if mapping_file.exists():
                mapping_df = pd.read_csv(mapping_file)
                calibration_files_set = set(
                    mapping_df[mapping_df["raw_file_type"] == "calibration"][
                        "raw_file_name"
                    ].tolist()
                )

        if processed_data_dir:
            processed_path = Path(processed_data_dir)
            if processed_path.exists():
                initial_count = len(raw_files)
                unprocessed_files = []

                for raw_file in raw_files:
                    # Get the base name without extension (e.g., sample1.raw -> sample1)
                    base_name = raw_file.stem

                    # ALWAYS include calibration files (they are reference files, not samples to be processed)
                    if raw_file.name in calibration_files_set:
                        unprocessed_files.append(raw_file)
                        continue

                    # Check for processed output based on workflow type
                    if workflow_type in ["LCMS Metabolomics", "LCMS Lipidomics"]:
                        # LCMS: Check if corresponding .corems directory exists
                        corems_dir = processed_path / f"{base_name}.corems"

                        if corems_dir.exists() and corems_dir.is_dir():
                            # Check if the .corems directory contains CSV files (indicates successful processing)
                            csv_files = list(corems_dir.glob("*.csv"))
                            if csv_files:
                                continue  # Skip this file - already processed

                    elif workflow_type == "GCMS Metabolomics":
                        # GCMS: Check if corresponding CSV file exists directly in processed directory
                        csv_file = processed_path / f"{base_name}.csv"

                        if csv_file.exists() and csv_file.is_file():
                            continue  # Skip this file - already processed

                    # File is not processed or processing incomplete
                    unprocessed_files.append(raw_file)

                excluded_count = initial_count - len(unprocessed_files)
                raw_files = unprocessed_files

                if not raw_files:
                    # set skip trigger for data_processed
                    self.set_skip_trigger("data_processed", True)
                    return
                if excluded_count > 0:
                    self.logger.info(
                        f"Generating wdl JSON files for {len(raw_files)} remaining unprocessed files"
                    )
                else:
                    self.logger.info(
                        f"Generating wdl JSON files for all {len(raw_files)} files (none processed yet)"
                    )
            else:
                self.logger.info(
                    f"Generating wdl JSON files for all {len(raw_files)} files (none processed yet)"
                )
        else:
            raise ValueError(
                "Processed data directory not configured correctly, check input configuration"
            )

        # Create batches for each configuration
        json_count = 0
        for config in self.config.get("configurations", []):
            config_name = config["name"]
            config_dir = self.workflow_path / "wdl_jsons" / config_name
            config_dir.mkdir(parents=True, exist_ok=True)

            # Filter files for this specific configuration
            # Use the configuration's file_filter if specified, otherwise include all files
            config_filters = config.get("file_filter", [])

            filtered_files = []
            for file_path in raw_files:
                filename = file_path.name.lower()
                # If no file_filter specified, include all files
                if not config_filters:
                    filtered_files.append(file_path)
                else:
                    # Check if ALL configuration filters are present in the filename
                    if all(
                        filter_term.lower() in filename
                        for filter_term in config_filters
                    ):
                        filtered_files.append(file_path)

            filter_info = (
                f"filters {config_filters}"
                if config_filters
                else "no filters (all files)"
            )
            self.logger.info(
                f"Configuration '{config_name}': {len(filtered_files)} files match {filter_info}"
            )

            if len(filtered_files) == 0:
                self.logger.warning(
                    f"No files found for configuration '{config_name}' - skipping"
                )
                continue

            # Show sample of filtered files for verification
            sample_size = min(3, len(filtered_files))
            self.logger.debug(f"Sample files for '{config_name}':")
            for i, sample_file in enumerate(filtered_files[:sample_size], 1):
                self.logger.debug(f"  {i}. {sample_file.name}")
            if len(filtered_files) > sample_size:
                self.logger.debug(f"  ... and {len(filtered_files) - sample_size} more")

            # Split files into batches
            batches = [
                filtered_files[i : i + batch_size]
                for i in range(0, len(filtered_files), batch_size)
            ]

            # Get the workflow type
            for batch_num, batch_files in enumerate(batches, 1):
                num_jsons = self._generate_single_wdl_json(
                    config, batch_files, batch_num
                )
                json_count += num_jsons

        # If no JSONs were created, all files are already processed
        if json_count == 0:
            self.set_skip_trigger("data_processed", True)
            return True

        return True

    def _generate_single_wdl_json(
        self, config: dict, batch_files: List[Path], batch_num: int
    ) -> int:
        """
        Generate WDL JSON file(s) based on workflow type.

        Args:
            config: Configuration dictionary for the workflow
            batch_files: List of raw data file paths for this batch
            batch_num: Batch number for naming the output file

        Returns:
            Number of JSON files created (may be >1 if batch is split into sub-batches)
        """
        workflow_type = self.config["workflow"]["workflow_type"]

        if workflow_type not in WORKFLOW_DICT:
            raise ValueError(
                f"Unsupported workflow type: {workflow_type}. Supported types: {list(WORKFLOW_DICT.keys())}"
            )

        # Get the generator method name from WORKFLOW_DICT and call it
        generator_method_name = WORKFLOW_DICT[workflow_type]["generator_method"]
        generator_method = getattr(self, generator_method_name)
        return generator_method(config, batch_files, batch_num)

    def _generate_lcms_metab_wdl(
        self, config: dict, batch_files: List[Path], batch_num: int
    ) -> int:
        """
        Generate a WDL JSON file for LCMS Metabolomics workflow.

        Args:
            config: Configuration dictionary for the workflow
            batch_files: List of raw data file paths for this batch
            batch_num: Batch number for naming the output file

        Returns:
            Number of JSON files created (always 1 for LCMS)
        """
        config_dir = self.workflow_path / "wdl_jsons" / config["name"]
        json_obj = {
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.file_paths": [
                str(f) for f in batch_files
            ],
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.output_directory": "output",
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.corems_toml_path": config[
                "corems_toml"
            ],
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.msp_file_path": config[
                "reference_db"
            ],
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.scan_translator_path": config[
                "scan_translator"
            ],
            "lcmsMetabolomics.runMetaMSLCMSMetabolomics.cores": config.get("cores", 1),
        }

        output_file = (
            config_dir
            / f"run_metaMS_lcms_metabolomics_{config['name']}_batch{batch_num}.json"
        )

        with open(output_file, "w") as f:
            json.dump(json_obj, f, indent=4)

        return 1

    def _generate_lcms_lipid_wdl(
        self, config: dict, batch_files: List[Path], batch_num: int
    ) -> int:
        """
        Generate a WDL JSON file for LCMS Lipidomics workflow.

        Args:
            config: Configuration dictionary for the workflow
            batch_files: List of raw data file paths for this batch
            batch_num: Batch number for naming the output file

        Returns:
            Number of JSON files created (always 1 for LCMS)
        """
        config_dir = self.workflow_path / "wdl_jsons" / config["name"]
        json_obj = {
            "lcmsLipidomics.runMetaMSLCMSLipidomics.file_paths": [
                str(f) for f in batch_files
            ],
            "lcmsLipidomics.runMetaMSLCMSLipidomics.output_directory": "output",
            "lcmsLipidomics.runMetaMSLCMSLipidomics.corems_toml_path": config[
                "corems_toml"
            ],
            "lcmsLipidomics.runMetaMSLCMSLipidomics.db_location": config[
                "reference_db"
            ],
            "lcmsLipidomics.runMetaMSLCMSLipidomics.scan_translator_path": config[
                "scan_translator"
            ],
            "lcmsLipidomics.runMetaMSLCMSLipidomics.cores": config.get("cores", 1),
        }

        output_file = (
            config_dir
            / f"run_metaMS_lcms_lipidomics_{config['name']}_batch{batch_num}.json"
        )

        with open(output_file, "w") as f:
            json.dump(json_obj, f, indent=4)

        return 1

    def _generate_gcms_metab_wdl(
        self, config: dict, batch_files: List[Path], batch_num: int
    ) -> int:
        """
        Generate WDL JSON file(s) for GCMS Metabolomics workflow.

        Matches sample files to calibration files based on chronological order (write_time).
        For each batch, finds the appropriate calibration file based on when files were run.
        May create multiple JSON files if samples exceed configured batch_size.

        Calibration Matching Logic:
        - Samples are matched to the most recent calibration file before them
        - If a sample was run before any calibration, it uses the first available calibration (with warning)
        - Example: [Cal1, S1, S2, Cal2, S3] -> S1,S2 use Cal1; S3 uses Cal2

        Args:
            config: Configuration dictionary for the workflow
            batch_files: List of raw data file paths for this batch (includes both samples and calibrations)
            batch_num: Batch number for naming the output file

        Returns:
            Number of JSON files created (may be >1 if batch is split into sub-batches)
        """
        config_dir = self.workflow_path / "wdl_jsons" / config["name"]

        # Get inspection results path
        inspection_results_path = self.get_raw_inspection_results_path()
        if not inspection_results_path:
            raise FileNotFoundError(
                "Raw file inspection results not found. Run raw_data_inspector() first."
            )

        # Load biosample mapping to identify file types
        mapping_file = (
            self.workflow_path / "metadata" / "mapped_raw_file_biosample_mapping.csv"
        )
        if not mapping_file.exists():
            raise FileNotFoundError(
                f"Biosample mapping not found: {mapping_file}. Run biosample mapping first."
            )

        mapping_df = pd.read_csv(mapping_file)
        inspection_df = pd.read_csv(inspection_results_path)

        # Build DataFrame for batch files with their metadata
        batch_df = pd.DataFrame(
            [
                {
                    "raw_data_file_short": f.name,
                    "file_path": str(f),
                    "raw_file_type": mapping_df[
                        mapping_df["raw_file_name"] == f.name
                    ].iloc[0]["raw_file_type"],
                    "write_time": inspection_df[
                        inspection_df["file_name"] == f.name
                    ].iloc[0]["write_time"],
                }
                for f in batch_files
            ]
        )

        # Separate calibration and sample files
        sample_files_df = batch_df[batch_df["raw_file_type"] != "calibration"].copy()
        calibration_count = len(batch_df[batch_df["raw_file_type"] == "calibration"])

        if calibration_count == 0:
            raise ValueError(
                f"No calibration files found in batch {batch_num}. At least one calibration file is required."
            )

        if len(sample_files_df) == 0:
            self.logger.warning(f"No sample files in batch {batch_num} - skipping")
            return 0

        # Use helper function to assign calibration files to samples
        sample_files_df = self._assign_calibration_files_to_samples(
            sample_files_df, inspection_results_path
        )

        # Get unique calibration file (for this batch, all samples should use same calibration)
        calibration_file = sample_files_df["calibration_file"].iloc[0]
        sample_file_paths = sample_files_df["file_path"].tolist()

        # Get batch size from workflow config (default to no limit if not specified)
        max_batch_size = self.config["workflow"].get(
            "batch_size", len(sample_file_paths)
        )

        # Helper function to create WDL JSON
        def create_wdl_json(samples, batch_id):
            json_obj = {
                "gcmsMetabolomics.runMetaMSGCMS.file_paths": samples,
                "gcmsMetabolomics.runMetaMSGCMS.calibration_file_path": calibration_file,
                "gcmsMetabolomics.runMetaMSGCMS.output_directory": f"output_batch_{batch_id}",
                "gcmsMetabolomics.runMetaMSGCMS.output_type": config.get(
                    "output_type", "csv"
                ),
                "gcmsMetabolomics.runMetaMSGCMS.corems_toml_path": config[
                    "corems_toml"
                ],
                "gcmsMetabolomics.runMetaMSGCMS.jobs_count": config.get("cores", 5),
                "gcmsMetabolomics.runMetaMSGCMS.output_filename": f"{config['name']}_batch{batch_id}",
            }
            output_file = (
                config_dir
                / f"run_metaMS_gcms_metabolomics_{config['name']}_batch{batch_id}.json"
            )
            with open(output_file, "w") as f:
                json.dump(json_obj, f, indent=4)
            return output_file

        # If sample files exceed batch size, split into sub-batches
        if len(sample_file_paths) > max_batch_size:
            num_sub_batches = (
                len(sample_file_paths) + max_batch_size - 1
            ) // max_batch_size

            for sub_batch_idx in range(num_sub_batches):
                start_idx = sub_batch_idx * max_batch_size
                end_idx = min(start_idx + max_batch_size, len(sample_file_paths))
                sub_batch_samples = sample_file_paths[start_idx:end_idx]
                sub_batch_num = f"{batch_num}.{sub_batch_idx + 1}"

                create_wdl_json(sub_batch_samples, sub_batch_num)

            return num_sub_batches
        else:
            # Generate single WDL JSON (batch size not exceeded)
            create_wdl_json(sample_file_paths, batch_num)
            return 1

    @skip_if_complete("data_processed", return_value=True)
    def generate_wdl_runner_script(self, script_name: Optional[str] = None) -> bool:
        """
        Generate a shell script to run all WDL JSON files using miniwdl.

        Creates a bash script that discovers all JSON files in the study's wdl_jsons
        directory and runs them sequentially using miniwdl. The script includes
        progress reporting and error handling.

        Args:
            script_name: Name for the generated script file. Defaults to
                        '{study_name}_wdl_runner.sh'

        Returns:
            True if script generation completed successfully, False otherwise

        Example:
            >>> manager.generate_wdl_runner_script()

        Note:
            The generated script expects to be run from a directory containing
            a 'wdl/' subdirectory with the workflow file. Use run_wdl_script()
            to execute from the appropriate location.
        """

        workflow_type = self.config["workflow"]["workflow_type"]
        if workflow_type not in WORKFLOW_DICT.keys():
            raise NotImplementedError(
                f"WDL runner script generation not implemented for workflow type: {workflow_type}"
            )
        wdl_workflow_name = WORKFLOW_DICT[workflow_type]["wdl_workflow_name"]

        if script_name is None:
            script_name = f"{self.workflow_name}_wdl_runner.sh"

        script_path = self.workflow_path / "scripts" / script_name

        # Get absolute path to the wdl_jsons directory
        wdl_jsons_dir = self.workflow_path / "wdl_jsons"

        # Check if wdl_jsons directory exists
        if not wdl_jsons_dir.exists():
            self.logger.error(
                f"WDL JSON directory not found: {wdl_jsons_dir}, run generate_wdl_jsons() first"
            )
            raise FileNotFoundError(f"WDL JSON directory not found: {wdl_jsons_dir}")

        # Find all JSON files
        json_files = list(wdl_jsons_dir.rglob("*.json"))
        if not json_files:
            self.logger.error(
                f"No JSON files found in: {wdl_jsons_dir}, run generate_wdl_jsons() first"
            )
            raise FileNotFoundError(f"No JSON files found in: {wdl_jsons_dir}")

        self.logger.info(f"Found {len(json_files)} JSON files")

        # Validate each JSON file and check referenced files
        missing_files = []
        corrupted_jsons = []

        for json_file in json_files:
            try:
                with open(json_file, "r") as f:
                    json_data = json.load(f)

                # Find all keys that end with 'file_paths' (raw data files)
                file_paths_keys = [
                    key for key in json_data.keys() if key.endswith(".file_paths")
                ]
                for file_paths_key in file_paths_keys:
                    file_paths = json_data[file_paths_key]
                    if isinstance(file_paths, list):
                        for file_path in file_paths:
                            if not Path(file_path).exists():
                                missing_files.append(file_path)

                # Find all keys that reference file paths (configuration files)
                # These typically end with '_path', 'toml_path', 'msp_file_path', 'db_location'
                config_file_patterns = [
                    "_path",
                    "toml_path",
                    "msp_file_path",
                    "db_location",
                ]
                config_keys = []
                for key in json_data.keys():
                    if any(key.endswith(pattern) for pattern in config_file_patterns):
                        config_keys.append(key)

                for config_key in config_keys:
                    config_path = json_data[config_key]
                    if isinstance(config_path, str) and not Path(config_path).exists():
                        missing_files.append(config_path)

            except json.JSONDecodeError as e:
                corrupted_jsons.append(f"{json_file}: {e}")
            except Exception as e:
                corrupted_jsons.append(f"{json_file}: {e}")

        # Report any issues found
        if corrupted_jsons:
            self.logger.error("Corrupted JSON files found:")
            for error in corrupted_jsons:
                self.logger.error(f"  {error}")
            raise ValueError("Corrupted JSON files detected")

        if missing_files:
            self.logger.error("Missing referenced files:")
            unique_missing = list(set(missing_files))
            for missing_file in unique_missing[:10]:  # Show first 10
                self.logger.error(f"  {missing_file}")
            if len(unique_missing) > 10:
                self.logger.error(f"  ... and {len(unique_missing) - 10} more files")
            self.logger.error(
                "Please ensure all raw data files and configuration files exist"
            )
            raise FileNotFoundError(f"Missing {len(unique_missing)} referenced files")

        self.logger.info("All JSON files and referenced files validated")

        script_content = f"""#!/bin/bash

# WDL Runner Script for {self.study_name}
# Generated automatically by NMDC Study Manager

# Base directory for the JSON files
BASE_DIR="{wdl_jsons_dir}"

# Count total batch files
NUM_BATCHES=$(find "${{BASE_DIR}}" -type f -name '*.json' | wc -l)

echo "Found $NUM_BATCHES JSON files to process for study: {self.study_name}"
echo "Study ID: {self.study_id}"
echo "========================"

# Check if WDL file exists in current directory
WDL_FILE="wdl/{wdl_workflow_name}.wdl"
if [ ! -f "$WDL_FILE" ]; then
    echo "ERROR: WDL file not found: $WDL_FILE"
    echo "Please run this script from a directory containing the wdl/ subdirectory"
    exit 1
fi

echo "Using WDL workflow: $WDL_FILE"
echo "========================"

# Initialize counters
SUCCESS_COUNT=0
FAILED_COUNT=0

# Iterate over all JSON files, sorted by name
for JSON_FILE in $(find "${{BASE_DIR}}" -type f -name '*.json' | sort); do
    BATCH_NAME=$(basename "$JSON_FILE")
    echo "Processing batch: $BATCH_NAME"
    echo "File: $JSON_FILE"
    
    # Run miniwdl with the JSON file
    if miniwdl run "$WDL_FILE" -i "$JSON_FILE" --verbose --no-cache --copy-input-files; then
        echo "✓ SUCCESS: Completed batch $BATCH_NAME"
        ((SUCCESS_COUNT++))
    else
        echo "✗ FAILED: Batch $BATCH_NAME failed with exit code $?"
        ((FAILED_COUNT++))
        echo "Continuing with next batch..."
    fi
    
    echo "------------------------"
done

echo "========================"
echo "WORKFLOW SUMMARY:"
echo "  Total batches: $NUM_BATCHES"
echo "  Successful: $SUCCESS_COUNT"
echo "  Failed: $FAILED_COUNT"
echo "  Study: {self.study_name} ({self.study_id})"

if [ $FAILED_COUNT -eq 0 ]; then
    echo "🎉 All batches completed successfully!"
    exit 0
else
    echo "⚠️  Some batches failed. Check logs above for details."
    exit 1
fi
"""

        # Write the script file
        with open(script_path, "w") as f:
            f.write(script_content)

        # Make the script executable
        os.chmod(script_path, 0o755)

        self.logger.info(f"Generated WDL runner script: {script_path}")
        return True

    # print(f"Script expects to find WDL file at: wdl/{workflow_name}.wdl")

    @skip_if_complete("data_processed", return_value=True)
    def run_wdl_script(
        self, script_path: Optional[str] = None, working_directory: Optional[str] = None
    ) -> bool:
        """
        Execute WDL workflows by downloading the workflow file from GitHub and running
        it from a study-level workflow directory.

        This method creates a workflow execution directory within the study, downloads
        the WDL file from GitHub, sets up a Python virtual environment, and executes
        the workflows directly without needing an external workspace.

        Args:
            script_path: Path to the shell script to execute. If not provided,
                        looks for '{workflow_name}_wdl_runner.sh' in scripts directory.
            working_directory: Optional override for execution directory. If not provided,
                             creates 'wdl_execution' directory within the study.

        Returns:
            True if WDL execution completed successfully, False otherwise

        Note:
            - Downloads WDL file
            - Creates study-level execution environment
            - No file moving required - processed data goes directly to configured location
        """
        import subprocess
        import urllib.request
        import ssl

        # Find script if not provided
        if script_path is None:
            script_path = (
                self.workflow_path / "scripts" / f"{self.workflow_name}_wdl_runner.sh"
            )
            if not script_path.exists():
                self.logger.error(
                    f"WDL runner script not found: {script_path}. Run generate_wdl_runner_script() first."
                )
                return False
        else:
            script_path = Path(script_path)

        # Set up working directory within study
        if working_directory is None:
            working_directory = self.workflow_path / "wdl_execution"
        else:
            working_directory = Path(working_directory)
        working_dir = Path(working_directory)

        # Validate script exists
        if not script_path.exists():
            self.logger.error(f"Script not found: {script_path}")
            return False

        # Create working directory structure
        working_dir.mkdir(parents=True, exist_ok=True)
        wdl_dir = working_dir / "wdl"
        wdl_dir.mkdir(parents=True, exist_ok=True)
        # Download WDL file from GitHub
        workflow_type = self.config["workflow"]["workflow_type"]
        if workflow_type not in WORKFLOW_DICT:
            self.logger.error(f"Unsupported workflow type: {workflow_type}")
            return False
        wdl_url = WORKFLOW_DICT[workflow_type]["wdl_download_location"]
        wdl_file = wdl_dir / f"{WORKFLOW_DICT[workflow_type]['wdl_workflow_name']}.wdl"

        if not wdl_file.exists():
            try:
                # Create SSL context that handles certificate issues on macOS
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                # Try with urllib first
                try:
                    with urllib.request.urlopen(
                        wdl_url, context=ssl_context
                    ) as response:
                        wdl_content = response.read().decode("utf-8")
                except Exception:
                    # Fallback: try using subprocess with curl (often works better on macOS)
                    result = subprocess.run(
                        ["curl", "-L", "-k", "--silent", "--show-error", wdl_url],
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )

                    if result.returncode != 0:
                        raise Exception(f"curl failed: {result.stderr}")

                    wdl_content = result.stdout

                # Validate we got actual WDL content
                if not wdl_content.strip() or "workflow" not in wdl_content.lower():
                    raise Exception(
                        "Downloaded content doesn't appear to be a valid WDL file"
                    )

                with open(wdl_file, "w") as f:
                    f.write(wdl_content)

                self.logger.info("WDL file downloaded successfully")
            except Exception as e:
                self.logger.error(f"Failed to download WDL file: {e}")
                self.logger.error("You can manually download the file with:")
                self.logger.error(f"  curl -L -k '{wdl_url}' > '{wdl_file}'")
                return 1

        # Check if Docker is running
        self.logger.info("Checking Docker availability...")
        try:
            docker_cmd = WorkflowRawDataInspectionManager._find_docker_command()
            docker_check = subprocess.run(
                [docker_cmd, "info"], capture_output=True, text=True, timeout=10
            )
            if docker_check.returncode != 0:
                self.logger.error("Docker is not running or not available")
                return 1
        except subprocess.TimeoutExpired:
            self.logger.error("Docker check timed out - Docker may not be running")
            return 1
        except FileNotFoundError as e:
            self.logger.error(
                f"Docker command not found - please install Docker Desktop: {e}"
            )
            return 1
        except Exception as e:
            self.logger.error(f"Error checking Docker: {e}")
            return 1

        # Use the base directory virtual environment
        base_venv_dir = self.base_path / "venv"
        venv_python = base_venv_dir / "bin" / "python"

        if not base_venv_dir.exists():
            self.logger.error(f"Virtual environment not found at: {base_venv_dir}")
            self.logger.error(
                "Please ensure you have a virtual environment set up in the base directory"
            )
            self.logger.error(
                "Run: python -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
            )
            return 1

        if not venv_python.exists():
            self.logger.error(f"Python executable not found in venv: {venv_python}")
            return 1

        self.logger.info(f"Using existing virtual environment: {base_venv_dir}")

        # Check if required WDL packages are installed
        self.logger.info("Checking WDL dependencies...")
        try:
            result = subprocess.run(
                [str(venv_python), "-c", "import WDL; import docker; print('OK')"],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode == 0:
                pass
            else:
                raise subprocess.CalledProcessError(result.returncode, "import check")

        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            self.logger.warning("WDL dependencies missing or corrupted. Installing...")
            try:
                # Force reinstall the WDL packages
                subprocess.run(
                    [
                        str(venv_python),
                        "-m",
                        "pip",
                        "install",
                        "--force-reinstall",
                        "miniwdl",
                        "docker",
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=60,
                )

                # Verify the installation worked (miniwdl installs as WDL package)
                verify_result = subprocess.run(
                    [
                        str(venv_python),
                        "-c",
                        "import WDL; import docker; print('Installation verified')",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )

                if verify_result.returncode == 0:
                    pass
                else:
                    self.logger.error(
                        f"Installation verification failed: {verify_result.stderr}"
                    )
                    return False

            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to install dependencies: {e}")
                if hasattr(e, "stderr") and e.stderr:
                    self.logger.error(f"Error details: {e}")
                return False

        self.logger.info(f"Running WDL workflows from: {working_dir}")

        # Create symbolic link to workflow_inputs directory so relative paths work
        workflow_inputs_source = self.base_path / "workflow_inputs"
        workflow_inputs_link = working_dir / "workflow_inputs"

        if workflow_inputs_source.exists() and not workflow_inputs_link.exists():
            try:
                workflow_inputs_link.symlink_to(workflow_inputs_source)
            except Exception as e:
                raise Exception(
                    f"Failed to create symbolic link for workflow inputs: {e}"
                )

        # Store current directory
        original_dir = os.getcwd()

        try:
            os.chdir(working_dir)

            # Create a command that activates the base venv and runs the script
            activate_and_run = f"source {base_venv_dir}/bin/activate && {script_path}"

            # Run the script with bash to handle source command
            result = subprocess.run(
                ["bash", "-c", activate_and_run],
                capture_output=False,  # Let output go to console
                text=True,
            )

            self.logger.info("=" * 50)
            self.logger.info(
                f"Script execution completed with exit code: {result.returncode}"
            )

            if result.returncode == 0:
                self.logger.info("All WDL workflows completed successfully!")

                # Move processed output files from working directory to designated processed data location
                self._move_processed_files(str(working_dir))

                self.set_skip_trigger(
                    trigger_name="data_processed", value=True, save=True
                )
                return True
            else:
                self.logger.warning(
                    f"Some workflows failed (exit code: {result.returncode})"
                )

                # Still move any processed files that were completed successfully
                self._move_processed_files(str(working_dir), clean_up=False)

                return False

        except Exception as e:
            self.logger.error(f"Error executing script: {e}")

            # Still try to move any completed processed files
            self._move_processed_files(str(working_dir))

            return False

        finally:
            # Always return to original directory
            os.chdir(original_dir)

    def _cleanup_wdl_execution_dir(self, working_dir: str) -> bool:
        """
        Clean up the current study's WDL execution directory after successful file moves.

        Removes the temporary WDL execution directory and all its contents after
        processed files have been moved to their final destination. This helps
        keep the study directory clean and prevents large temporary files from
        accumulating.

        Args:
            working_dir: Path to the WDL execution directory to clean up

        Returns:
            True if cleanup was successful, False otherwise

        Note:
            Only removes directories that:
            1. Contain 'wdl_execution' in the path
            2. Are within the current study's directory structure
            3. Are called only after successful file moves
        """
        import shutil

        working_path = Path(working_dir)

        # Safety check 1: only clean up directories that are clearly WDL execution directories
        if "wdl_execution" not in str(working_path):
            return False

        # Safety check 2: ensure this is within the current study's directory structure
        try:
            working_path.relative_to(self.workflow_path)
        except ValueError:
            return False

        if not working_path.exists():
            return True

        try:
            self.logger.info(
                f"Cleaning up WDL execution directory for study {self.study_name}: {working_path}"
            )

            # Remove the entire directory tree
            shutil.rmtree(working_path)

            return True

        except Exception as e:
            self.logger.error(f"Failed to clean up WDL execution directory: {e}")
            return False


class NMDCWorkflowBiosampleManager:
    """
    Mixin class for managing NMDC biosample attributes and mapping scripts.
    """

    @skip_if_complete("biosample_attributes_fetched", return_value=True)
    def get_biosample_attributes(self, study_id: Optional[str] = None) -> bool:
        """
        Fetch biosample attributes from NMDC API and save to CSV file.

        Uses the nmdc_api_utilities package to query biosamples associated with
        the study ID and saves the attributes to a CSV file in the study's
        metadata directory. Includes skip trigger to avoid re-downloading.

        Args:
            study_id: NMDC study ID (e.g., 'nmdc:sty-11-dwsv7q78').
                     Uses config['study']['id'] if not provided.

        Returns:
            True if biosample attributes fetched successfully, False otherwise

        Note:
            The CSV file is saved as 'biosample_attributes.csv' in the study's metadata directory.
            This method is automatically skipped if biosample_attributes_fetched trigger is set.
        """
        from nmdc_api_utilities.biosample_search import BiosampleSearch

        if study_id is None:
            study_id = self.config["study"]["id"]

        self.logger.info(f"Fetching biosample attributes for study: {study_id}")

        try:
            # Use nmdc_api_utilities to fetch biosamples
            biosample_search = BiosampleSearch()
            biosamples = biosample_search.get_record_by_filter(
                filter=f'{{"associated_studies":"{study_id}"}}',
                max_page_size=1000,
                fields="id,name,samp_name,description,gold_biosample_identifiers,insdc_biosample_identifiers,submitter_id,analysis_type",
                all_pages=True,
            )

            if not biosamples:
                self.logger.error(f"No biosamples found for study {study_id}")
                return False

            # Convert to DataFrame
            biosample_df = pd.DataFrame(biosamples)

            # Create metadata directory if it doesn't exist
            metadata_dir = self.workflow_path / "metadata"
            metadata_dir.mkdir(parents=True, exist_ok=True)

            # Save to CSV
            biosample_csv = metadata_dir / "biosample_attributes.csv"
            biosample_df.to_csv(biosample_csv, index=False)

            # Set skip trigger
            self.set_skip_trigger("biosample_attributes_fetched", True)
            return True

        except Exception as e:
            self.logger.error(f"Error fetching biosample data: {e}")

            return False

    @skip_if_complete("biosample_mapping_script_generated", return_value=True)
    def generate_biosample_mapping_script(
        self, script_name: Optional[str] = None, template_path: Optional[str] = None
    ) -> bool:
        """
        Generate a study-specific TEMPLATE script for mapping raw files to biosamples.

        Creates a customizable Python template script that maps raw data files to NMDC
        biosamples using a template file. The generated script is clearly labeled as a
        TEMPLATE and includes parsing logic that MUST be customized for each study's
        specific file naming conventions.

        Args:
            script_name: Name for the generated script. Defaults to
                        'map_raw_files_to_biosamples_TEMPLATE.py'
            template_path: Path to template file. Defaults to
                          'nmdc_dp_utils/templates/biosample_mapping_script_template.py'

        Returns:
            True if script generation completed successfully, False otherwise

        Note:
            The generated script is labeled as _TEMPLATE to prevent accidental use
            without customization. Users should copy to a new filename and modify
            the parsing logic for their study's specific file naming patterns.
            This method is automatically skipped if biosample_mapping_script_generated trigger is set.
        """
        if script_name is None:
            script_name = "map_raw_files_to_biosamples_TEMPLATE.py"

        if template_path is None:
            # Use default template relative to this module
            template_path = (
                Path(__file__).parent
                / "templates"
                / "biosample_mapping_script_template.py"
            )
        else:
            template_path = Path(template_path)

        script_path = self.workflow_path / "scripts" / script_name

        # Check if template exists
        if not template_path.exists():
            self.logger.error(f"Template file not found: {template_path}")
            return False

        try:
            # Read the template
            with open(template_path, "r") as f:
                template_content = f.read()

            # Format the template with study-specific values
            script_content = template_content.format(
                study_name=self.study_name,
                study_description=self.config["study"]["description"],
                script_name=script_name,
                config_path=self.config_path,
            )

            # Write the script file
            with open(script_path, "w") as f:
                f.write(script_content)

            # Make the script executable
            os.chmod(script_path, 0o755)

            self.logger.info(
                f"Generated biosample mapping TEMPLATE script: {script_path}"
            )

            self.set_skip_trigger("biosample_mapping_script_generated", True)
            return True

        except Exception as e:
            self.logger.error(f"Error generating mapping script: {e}")
            return False

    @skip_if_complete("biosample_mapping_completed", return_value=True)
    def run_biosample_mapping_script(self, script_path: Optional[str] = None) -> bool:
        """
        Execute the biosample mapping script.

        Runs the study-specific biosample mapping script and reports success/failure.
        Sets skip trigger on successful completion.

        Args:
            script_path: Path to the customized mapping script (NOT the template).
                        Looks for 'scripts/map_raw_files_to_biosamples.py' by default.
                        Will not run template files (_TEMPLATE) directly.

        Returns:
            True if mapping completed successfully, False otherwise

        Note:
            This method automatically sets the 'biosample_mapping_completed'
            trigger on successful completion.
        """
        import subprocess

        if script_path is None:
            # Check for both template and non-template versions
            template_script = (
                self.workflow_path
                / "scripts"
                / "map_raw_files_to_biosamples_TEMPLATE.py"
            )
            regular_script = (
                self.workflow_path / "scripts" / "map_raw_files_to_biosamples.py"
            )

            if regular_script.exists():
                script_path = regular_script
            elif template_script.exists():
                self.logger.error(
                    "Found only TEMPLATE script - you must customize it first!"
                )
                self.logger.error(f"Template script: {template_script}")
                return False
            else:
                self.logger.error(
                    "No mapping script found. Run generate_biosample_mapping_script() first"
                )
                return False
        else:
            script_path = Path(script_path)

        # Check if user is trying to run the template directly
        if "_TEMPLATE" in script_path.name:
            self.logger.error("Cannot run TEMPLATE script directly!")
            self.logger.error(f"Template script: {script_path}")
            return False

        if not script_path.exists():
            self.logger.error(f"Mapping script not found: {script_path}")
            return False

        self.logger.info(f"Running biosample mapping script: {script_path}")

        # Store current directory
        original_dir = os.getcwd()

        try:
            # Run the mapping script
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=False,  # Let output go to console
                text=True,
                cwd=self.base_path,
            )  # Run from base directory

            if result.returncode == 0:
                # Generate filtered file list for WDL processing
                self._generate_mapped_files_list()

                self.set_skip_trigger("biosample_mapping_completed", True)
                return True
            else:
                self.logger.warning(
                    f"Biosample mapping script exited with code: {result.returncode}"
                )

                return False

        except Exception as e:
            self.logger.error(f"Error running mapping script: {e}")
            return False

        finally:
            # Always return to original directory
            os.chdir(original_dir)

    def _generate_mapped_files_list(self) -> None:
        """
        Generate a list of raw data files that successfully mapped to biosamples.

        Creates a CSV file containing only the files with high and medium confidence
        matches to NMDC biosamples. This filtered list is used by generate_wdl_jsons()
        to ensure only mappable files are processed.

        Output file: metadata/mapped_raw_files.csv
        Columns: raw_file_path, biosample_id, biosample_name, match_confidence
        """
        import pandas as pd

        # Load the biosample mapping file
        mapping_file = (
            self.workflow_path / "metadata" / "mapped_raw_file_biosample_mapping.csv"
        )
        if not mapping_file.exists():
            self.logger.warning(f"Mapping file not found: {mapping_file}")
            return

        try:
            mapping_df = pd.read_csv(mapping_file)

            # Check if raw_file_type column exists (new format) or not (old format for backwards compatibility)
            has_file_type = "raw_file_type" in mapping_df.columns

            if has_file_type:
                # New format: Filter for high/medium confidence matches AND include calibration/qc files
                # Calibration files are needed for raw_data_inspector even though they don't map to biosamples
                mapped_df = mapping_df[
                    (mapping_df["match_confidence"].isin(["high", "medium"]))
                    | (mapping_df["raw_file_type"].isin(["qc", "calibration"]))
                ].copy()
            else:
                # Old format (backwards compatible): Filter for only high and medium confidence matches
                mapped_df = mapping_df[
                    mapping_df["match_confidence"].isin(["high", "medium"])
                ].copy()

            if len(mapped_df) == 0:
                self.logger.warning(
                    "No high or medium confidence matches found - no files will be processed"
                )
                return

            # Get the full file paths - try to use downloaded_files.csv if available (old format)
            # Otherwise construct paths from raw_data_directory (new format)
            downloaded_files_csv = (
                self.workflow_path / "metadata" / "downloaded_files.csv"
            )
            if downloaded_files_csv.exists():
                downloaded_df = pd.read_csv(downloaded_files_csv)

                # Check if old format (has file_path column) or new format (only has raw_data_file_short)
                if (
                    "file_path" in downloaded_df.columns
                    and "file_name" in downloaded_df.columns
                ):
                    # Old format with full paths
                    mapped_df = mapped_df.merge(
                        downloaded_df[["file_name", "file_path"]],
                        left_on="raw_file_name",
                        right_on="file_name",
                        how="left",
                    )
                    mapped_df["raw_file_path"] = mapped_df["file_path"]
                else:
                    # New format - construct paths
                    raw_data_dir = Path(self.raw_data_directory)
                    mapped_df["raw_file_path"] = mapped_df["raw_file_name"].apply(
                        lambda x: str(raw_data_dir / x)
                    )
            else:
                # No downloaded_files.csv - construct paths from raw_data_directory
                raw_data_dir = Path(self.raw_data_directory)
                mapped_df["raw_file_path"] = mapped_df["raw_file_name"].apply(
                    lambda x: str(raw_data_dir / x)
                )

            # Select columns for output - include raw_file_type if it exists
            if has_file_type:
                output_df = mapped_df[
                    [
                        "raw_file_path",
                        "biosample_id",
                        "biosample_name",
                        "match_confidence",
                    ]
                ].copy()
            else:
                output_df = mapped_df[
                    [
                        "raw_file_path",
                        "biosample_id",
                        "biosample_name",
                        "match_confidence",
                    ]
                ].copy()

            # Save the filtered file list
            output_file = self.workflow_path / "metadata" / "mapped_raw_files.csv"
            output_df.to_csv(output_file, index=False)

            # Report statistics
            total_files = len(mapping_df)
            high_conf = len(mapped_df[mapped_df["match_confidence"] == "high"])
            med_conf = len(mapped_df[mapped_df["match_confidence"] == "medium"])

            self.logger.info(f"Generated filtered file list: {output_file}")
            self.logger.info(
                f"Total mapped files: {len(output_df)} of {total_files} ({len(output_df) / total_files * 100:.1f}%)"
            )
            self.logger.info(f"High confidence: {high_conf}")
            self.logger.info(f"Medium confidence: {med_conf}")

            if has_file_type:
                calibration_files = len(
                    mapped_df[mapped_df["raw_file_type"].isin(["qc", "calibration"])]
                )
                sample_files = len(mapped_df[mapped_df["raw_file_type"] == "sample"])
                self.logger.info(f"Sample files: {sample_files}")
                self.logger.info(f"Calibration/QC files: {calibration_files}")

            self.logger.info(
                f"Files excluded: {total_files - len(output_df)} (no_match + low confidence)"
            )

        except Exception as e:
            self.logger.error(f"Error generating mapped files list: {e}")
            import traceback

            traceback.print_exc()


class WorkflowRawDataInspectionManager:
    """
    Mixin class for managing raw data inspection using Docker containers.
    """

    @staticmethod
    def _find_docker_command():
        """
        Find the docker command in the system.
        
        Checks common locations and PATH to find docker executable.
        This ensures docker can be found even when subprocess doesn't inherit
        the full shell environment.
        
        Returns:
            str: Path to docker executable
            
        Raises:
            FileNotFoundError: If docker cannot be found
        """
        # Try to find docker using shutil.which (checks PATH)
        docker_path = shutil.which('docker')
        if docker_path:
            return docker_path
        
        # Check common installation locations if not in PATH
        common_locations = [
            '/usr/local/bin/docker',
            '/usr/bin/docker',
            '/opt/homebrew/bin/docker',
        ]
        
        for location in common_locations:
            if Path(location).exists():
                return location
        
        # If still not found, raise error
        raise FileNotFoundError(
            "Docker command not found. Please ensure Docker is installed and accessible."
        )

    @skip_if_complete("raw_data_inspected", return_value=True)
    def raw_data_inspector(
        self, file_paths=None, cores=1, limit=None, max_retries=10, retry_delay=10.0
    ) -> bool:
        """
        Run raw data inspection on raw files to extract metadata using Docker.

        Uses workflow-specific inspector based on WORKFLOW_DICT configuration:
        - LCMS workflows: Docker-based raw_data_inspector.py (for .mzML/.raw files)
        - GCMS workflows: Docker-based gcms_data_inspector.py (for .cdf files)

        Both workflows require the same Docker image specified in configuration.

        IMPORTANT: .raw files (LCMS) require single-core processing to prevent crashes due to
        file locking issues with the Thermo RawFileReader in Docker containers. This method
        automatically forces cores=1 when .raw files are detected.

        Args:
            file_paths (List[str], optional): List of file paths to inspect.
                                            If None, uses mapped raw files from metadata.
            cores (int): Number of cores for parallel processing (default: 1).
                        Automatically forced to 1 for .raw files to prevent crashes.
            limit (int, optional): Limit number of files to process (for testing)
            max_retries (int): Maximum number of retry attempts for transient errors (default: 10)
            retry_delay (float): Delay in seconds between retry attempts (default: 10.0)

        Returns:
            True if inspection completed successfully, False otherwise

        Note:
            Results are saved to raw_file_info/raw_file_inspection_results.csv.
            This method is automatically skipped if raw_data_inspected trigger is set.

        Configuration Required:
            "docker": {
                "raw_data_inspector_image": "microbiomedata/metams:3.3.3"
            }
        """
        # Determine which inspector to use based on workflow type
        workflow_type = self.config["workflow"]["workflow_type"]
        workflow_config = WORKFLOW_DICT.get(workflow_type)
        if not workflow_config:
            raise ValueError(f"Unknown workflow type: {workflow_type}")

        inspector_name = workflow_config.get("raw_data_inspector", "raw_data_inspector")

        # Branch to appropriate inspector method
        if inspector_name == "gcms_data_inspector":
            return self._run_gcms_data_inspector(
                file_paths, cores, limit, max_retries, retry_delay
            )
        else:
            return self._run_lcms_data_inspector(
                file_paths, cores, limit, max_retries, retry_delay
            )

    def _run_lcms_data_inspector(
        self, file_paths, cores, limit, max_retries, retry_delay
    ):
        """Run LCMS raw data inspector using Docker container."""
        self.logger.info("Starting LCMS RAW DATA INSPECTION (Docker)")

        try:
            # Get file paths to inspect
            if file_paths is None:
                # Use mapped raw files if available (only inspect high/medium confidence mapped files)
                mapped_files_path = (
                    self.workflow_path / "metadata" / "mapped_raw_files.csv"
                )
                if mapped_files_path.exists():
                    mapped_df = pd.read_csv(mapped_files_path)
                    file_paths = mapped_df["raw_file_path"].tolist()
                else:
                    # Fallback to all files in raw_data_directory
                    raw_data_dir = Path(self.raw_data_directory)
                    file_paths = []
                    for ext in [
                        "*.mzML",
                        "*.raw",
                        "*.mzml",
                    ]:  # Include lowercase variants
                        file_paths.extend([str(f) for f in raw_data_dir.rglob(ext)])

            if not file_paths:
                self.logger.warning("No raw files found to inspect")
                return None

            # Check for previous inspection results and filter out successfully inspected files
            output_dir = self.workflow_path / "raw_file_info"
            output_dir.mkdir(parents=True, exist_ok=True)
            existing_results_file = output_dir / "raw_file_inspection_results.csv"

            previous_results_df = None
            files_to_inspect = file_paths

            if existing_results_file.exists():
                try:
                    previous_results_df = pd.read_csv(existing_results_file)

                    # Identify successfully inspected files (those with numeric rt_max values)
                    # Store just the filenames, not full paths
                    successful_filenames = set()
                    for _, row in previous_results_df.iterrows():
                        try:
                            # Check if rt_max is a valid number (not NaN, not error message)
                            rt_max = pd.to_numeric(row.get("rt_max"), errors="coerce")
                            if pd.notna(rt_max) and isinstance(rt_max, (int, float)):
                                # Extract just the filename from the path
                                file_path = row["file_path"]
                                filename = Path(file_path).name
                                successful_filenames.add(filename)
                        except Exception:
                            continue

                    # Filter out successfully inspected files by comparing filenames
                    files_to_inspect = [
                        fp
                        for fp in file_paths
                        if Path(fp).name not in successful_filenames
                    ]

                    if len(files_to_inspect) == 0:
                        # Set skip trigger
                        self.set_skip_trigger("raw_data_inspected", True)
                        return str(existing_results_file)

                except Exception as e:
                    self.logger.warning(f"Error reading previous results: {e}")
                    files_to_inspect = file_paths
                    previous_results_df = None

            # Now check Docker configuration since we have files to inspect
            docker_image = self.config.get("docker", {}).get("raw_data_inspector_image")
            if not docker_image:
                self.logger.error("Docker image not configured.")

                return None

            # Check for .raw files and force single core processing to prevent crashes
            has_raw_files = any(
                str(fp).lower().endswith(".raw") for fp in files_to_inspect
            )
            original_cores = cores
            if has_raw_files and cores > 1:
                cores = 1
                self.logger.warning(
                    f"Detected .raw files - forcing single core processing (requested: {original_cores} → using: 1)"
                )

            # Use a temporary output file to avoid overwriting existing results
            if previous_results_df is not None:
                # Write to temporary file first, then merge
                temp_output_dir = output_dir / "temp_inspection"
                temp_output_dir.mkdir(parents=True, exist_ok=True)
                inspection_output_dir = temp_output_dir
            else:
                # No previous results, can write directly
                inspection_output_dir = output_dir

            # Run inspection on files that need it
            result = self._run_raw_data_inspector_docker(
                files_to_inspect,
                inspection_output_dir,
                cores,
                limit,
                max_retries,
                retry_delay,
                docker_image,
            )

            # Merge previous and new results if we had previous results
            if result is not None and previous_results_df is not None:
                try:
                    # result is already a DataFrame from _process_inspection_results_from_file
                    if isinstance(result, pd.DataFrame):
                        new_results_df = result
                    else:
                        # If it's a file path, read it
                        new_results_df = pd.read_csv(result)

                    # Combine the dataframes, keeping new results for any duplicates
                    # First, get file paths from new results
                    new_file_paths = set(new_results_df["file_path"].tolist())

                    # Keep only previous results that weren't re-inspected
                    previous_to_keep = previous_results_df[
                        ~previous_results_df["file_path"].isin(new_file_paths)
                    ]

                    # Combine previous and new results
                    combined_df = pd.concat(
                        [previous_to_keep, new_results_df], ignore_index=True
                    )

                    # Remove any exact duplicates (safety measure)
                    before_dedup = len(combined_df)
                    combined_df = combined_df.drop_duplicates(
                        subset=["file_path"], keep="last"
                    )
                    after_dedup = len(combined_df)
                    if before_dedup != after_dedup:
                        self.logger.warning(
                            f"Removed {before_dedup - after_dedup} duplicate entries"
                        )

                    # Sort by file path for consistency
                    combined_df = combined_df.sort_values("file_path").reset_index(
                        drop=True
                    )

                    # Write combined results back to the main results file
                    combined_df.to_csv(existing_results_file, index=False)

                    # Clean up temporary directory
                    if temp_output_dir.exists():
                        import shutil

                        shutil.rmtree(temp_output_dir)

                    result = str(existing_results_file)

                except Exception as e:
                    self.logger.warning(
                        f"Error merging results during raw data inspection: {e}"
                    )
                    import traceback

                    traceback.print_exc()

            # Set the skip trigger on successful completion
            if result is not None:
                self.logger.info("Raw data inspection completed successfully")
                self.set_skip_trigger("raw_data_inspected", True)
                return True
            else:
                self.logger.error("Raw data inspection failed")
                return False

        except Exception as e:
            self.logger.error(f"Error during raw data inspection: {e}")
            import traceback

            traceback.print_exc()
            return False

    def _run_raw_data_inspector_docker(
        self,
        file_paths,
        output_dir,
        cores,
        limit,
        max_retries,
        retry_delay,
        docker_image,
    ):
        """Run raw data inspector using Docker container."""

        # Check if Docker is available
        try:
            docker_exe = self._find_docker_command()
            docker_check = subprocess.run(
                [docker_exe, "--version"], capture_output=True, text=True, timeout=10
            )
            if docker_check.returncode != 0:
                raise RuntimeError("Docker is not available")
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            raise RuntimeError(f"Docker is not installed or not available: {e}")

        # Get script path
        script_path = Path(__file__).parent / "raw_data_inspector.py"
        if not script_path.exists():
            raise ValueError(f"Raw data inspector script not found at: {script_path}")

        # Prepare volume mounts - we need to mount all parent directories of the file paths
        # to ensure the files are accessible within the container
        mount_points = set()

        # Add all unique parent directories of raw files
        for file_path in file_paths:
            file_path_obj = Path(file_path).resolve()
            # Find the deepest common ancestor that makes sense for mounting
            # For now, let's mount the entire raw_data_directory
            raw_data_dir = Path(self.raw_data_directory).resolve()
            mount_points.add(str(raw_data_dir))

        # Always mount the output directory and script directory
        mount_points.add(str(output_dir.resolve()))
        mount_points.add(str(script_path.parent.resolve()))

        # Ensure all mount points exist before Docker tries to mount them
        # This is critical when running with --user flag, as Docker can't create
        # directories without proper permissions in that mode
        for mount_point in mount_points:
            mount_path = Path(mount_point)
            mount_path.mkdir(parents=True, exist_ok=True)

        # Build Docker volume arguments
        volume_args = []
        container_file_paths = []

        for mount_point in mount_points:
            container_path = f"/mnt{mount_point}"
            volume_args.extend(["-v", f"{mount_point}:{container_path}"])

        # Convert file paths to container paths
        raw_data_dir = Path(self.raw_data_directory).resolve()
        for file_path in file_paths:
            file_path_obj = Path(file_path).resolve()
            # Replace the raw_data_dir with the container mount point
            container_file_path = str(file_path_obj).replace(
                str(raw_data_dir), f"/mnt{raw_data_dir}"
            )
            container_file_paths.append(container_file_path)

        # Convert output directory to container path
        container_output_dir = f"/mnt{output_dir.resolve()}"

        # Convert script path to container path
        container_script_path = f"/mnt{script_path.resolve()}"

        # Prepare command arguments
        cmd_args = (
            ["--files"]
            + container_file_paths
            + [
                "--output-dir",
                container_output_dir,
                "--cores",
                str(cores),
                "--max-retries",
                str(max_retries),
                "--retry-delay",
                str(retry_delay),
            ]
        )

        if limit is not None:
            cmd_args.extend(["--limit", str(limit)])

        # Show mount point details for debugging

        # Build Docker command
        docker_cmd = (
            [
                docker_exe,
                "run",
                "--rm",
                "--user",
                f"{os.getuid()}:{os.getgid()}",  # Run as current user to avoid permission issues
            ]
            + volume_args
            + [docker_image, "python", container_script_path]
            + cmd_args
        )

        self.logger.info("Running docker-based raw file inspector...")
        if limit:
            self.logger.debug(f"Limit: {limit} files")

        # Run the Docker command with real-time output
        result = subprocess.run(
            docker_cmd,
            cwd=str(self.workflow_path),
            capture_output=False,  # Let output go directly to console for real-time feedback
            text=True,
            timeout=3600,  # 1 hour timeout
        )

        self.logger.info(
            f"Docker execution completed with exit code: {result.returncode}"
        )

        # Since we used capture_output=False, we need to check the output file directly
        if result.returncode == 0:
            # Look for the output file
            default_output = output_dir / "raw_file_inspection_results.csv"
            if default_output.exists():
                return self._process_inspection_results_from_file(default_output)
            else:
                self.logger.warning(f"Expected output file not found: {default_output}")
                return None
        else:
            self.logger.error(
                f"Docker execution failed with exit code: {result.returncode}"
            )
            return None

    def _run_gcms_data_inspector(
        self, file_paths, cores, limit, max_retries, retry_delay
    ):
        """Run GCMS raw data inspector using gcms_data_inspector.py script."""
        self.logger.info("Running GCMS RAW DATA INSPECTION")

        try:
            # Get file paths to inspect
            if file_paths is None:
                # Use mapped raw files if available
                mapped_files_path = (
                    self.workflow_path / "metadata" / "mapped_raw_files.csv"
                )
                if mapped_files_path.exists():
                    mapped_df = pd.read_csv(mapped_files_path)
                    file_paths = mapped_df["raw_file_path"].tolist()
                else:
                    # Fallback to all .cdf files in raw_data_directory
                    raw_data_dir = Path(self.raw_data_directory)
                    file_paths = []
                    for ext in ["*.cdf", "*.CDF"]:
                        file_paths.extend([str(f) for f in raw_data_dir.rglob(ext)])

            if not file_paths:
                self.logger.warning("No CDF files found to inspect")
                return None

            # Setup output directory
            output_dir = self.workflow_path / "raw_file_info"
            output_dir.mkdir(parents=True, exist_ok=True)
            existing_results_file = output_dir / "raw_file_inspection_results.csv"

            # Check for previous results and filter out successfully inspected files
            previous_results_df = None
            if existing_results_file.exists():
                try:
                    previous_results_df = pd.read_csv(existing_results_file)
                    previous_file_paths = set(previous_results_df["file_path"].tolist())

                    # Filter out already-inspected files
                    files_to_inspect = [
                        f for f in file_paths if f not in previous_file_paths
                    ]

                    if len(files_to_inspect) < len(file_paths):
                        file_paths = files_to_inspect

                    if not files_to_inspect:
                        return str(existing_results_file)
                except Exception as e:
                    self.logger.warning(f"Could not read previous results: {e}")
                    previous_results_df = None

            # Apply limit if specified
            if limit and len(file_paths) > limit:
                self.logger.warning(
                    f"Limiting inspection to {limit} files (total available: {len(file_paths)})"
                )
                file_paths = file_paths[:limit]

            # Get the gcms_data_inspector.py script path
            script_path = Path(__file__).parent / "gcms_data_inspector.py"
            if not script_path.exists():
                raise ValueError(
                    f"GCMS data inspector script not found at: {script_path}"
                )

            # Get Docker image from config (required)
            docker_image = self.config.get("docker", {}).get("raw_data_inspector_image")
            if not docker_image:
                raise ValueError(
                    "Docker configuration required: config['docker']['raw_data_inspector_image'] not found"
                )

            result = self._run_gcms_inspector_docker(
                file_paths,
                output_dir,
                cores,
                max_retries,
                retry_delay,
                docker_image,
                script_path,
            )

            # Merge with previous results if they exist
            if result is not None and previous_results_df is not None:
                try:
                    if isinstance(result, pd.DataFrame):
                        new_results_df = result
                    else:
                        new_results_df = pd.read_csv(result)

                    # Combine dataframes
                    new_file_paths = set(new_results_df["file_path"].tolist())
                    previous_to_keep = previous_results_df[
                        ~previous_results_df["file_path"].isin(new_file_paths)
                    ]

                    combined_df = pd.concat(
                        [previous_to_keep, new_results_df], ignore_index=True
                    )
                    combined_df = combined_df.drop_duplicates(
                        subset=["file_path"], keep="last"
                    )
                    combined_df = combined_df.sort_values("file_path").reset_index(
                        drop=True
                    )

                    # Write combined results
                    combined_df.to_csv(existing_results_file, index=False)

                    result = str(existing_results_file)
                except Exception as e:
                    self.logger.warning(f"Error merging results: {e}")

            # Set skip trigger on success
            if result is not None:
                self.logger.info("GCMS raw data inspection completed successfully.")
                self.set_skip_trigger("raw_data_inspected", True)
                return True
            else:
                self.logger.error("GCMS raw data inspection failed")
                return False

        except Exception as e:
            self.logger.error(f"Error during GCMS inspection: {e}")
            import traceback

            traceback.print_exc()
            return False

    def _run_gcms_inspector_docker(
        self,
        file_paths,
        output_dir,
        cores,
        max_retries,
        retry_delay,
        docker_image,
        script_path,
    ):
        """Run GCMS inspector in Docker container."""
        self.logger.info("Running GCMS inspector in Docker...")

        # Check if Docker is available
        try:
            docker_exe = self._find_docker_command()
            docker_check = subprocess.run(
                [docker_exe, "--version"], capture_output=True, text=True, timeout=10
            )
            if docker_check.returncode != 0:
                raise RuntimeError("Docker is not available")
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            raise RuntimeError(f"Docker is not installed or not available: {e}")

        # Prepare volume mounts
        mount_points = set()
        raw_data_dir = Path(self.raw_data_directory).resolve()
        mount_points.add(str(raw_data_dir))
        mount_points.add(str(output_dir.resolve()))
        mount_points.add(str(script_path.parent.resolve()))

        # Ensure all mount points exist before Docker tries to mount them
        # This is critical when running with --user flag, as Docker can't create
        # directories without proper permissions in that mode
        for mount_point in mount_points:
            mount_path = Path(mount_point)
            mount_path.mkdir(parents=True, exist_ok=True)

        # Build volume arguments
        volume_args = []
        for mount_point in mount_points:
            container_path = f"/mnt{mount_point}"
            volume_args.extend(["-v", f"{mount_point}:{container_path}"])

        # Convert file paths to container paths
        container_file_paths = []
        for file_path in file_paths:
            file_path_obj = Path(file_path).resolve()
            container_file_path = str(file_path_obj).replace(
                str(raw_data_dir), f"/mnt{raw_data_dir}"
            )
            container_file_paths.append(container_file_path)

        # Convert paths to container paths
        container_output_dir = f"/mnt{output_dir.resolve()}"
        container_script_path = f"/mnt{script_path.resolve()}"

        # Build command arguments (files are positional, not --files)
        cmd_args = container_file_paths + [
            "--output-dir",
            container_output_dir,
            "--cores",
            str(cores),
            "--max-retries",
            str(max_retries),
            "--retry-delay",
            str(retry_delay),
        ]

        # Build Docker command
        docker_cmd = (
            [
                docker_exe,
                "run",
                "--rm",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
            ]
            + volume_args
            + [docker_image, "python", container_script_path]
            + cmd_args
        )

        # Run Docker command
        result = subprocess.run(
            docker_cmd,
            cwd=str(self.workflow_path),
            capture_output=False,
            text=True,
            timeout=3600,
        )

        self.logger.info(
            f"Docker execution completed with exit code: {result.returncode}"
        )

        if result.returncode == 0:
            default_output = output_dir / "raw_file_inspection_results.csv"
            if default_output.exists():
                return self._process_inspection_results_from_file(default_output)
            else:
                self.logger.warning(f"Expected output file not found: {default_output}")
                return None
        else:
            self.logger.error(
                f"Docker execution failed with exit code: {result.returncode}"
            )
            return None

    def _process_inspection_results_from_file(self, output_file):
        """Process inspection results from output CSV file."""
        try:
            import pandas as pd

            # Read the CSV file
            df = pd.read_csv(output_file)

            return df
        except Exception as e:
            self.logger.error(f"Failed to read inspection results: {e}")
            return None

    def _process_inspection_results(self, result, output_dir):
        """Process the results from raw data inspection (common for both Docker and venv methods)."""
        if result.returncode == 0:
            self.logger.info("Raw data inspection completed successfully!")

            # Parse output to find the result file path
            lines = result.stdout.strip().split("\n")
            output_file_path = None
            for line in lines:
                if "Results:" in line:
                    output_file_path = line.split("Results:")[-1].strip()
                    break

            # If not found in stdout, use default path
            if not output_file_path or not Path(output_file_path).exists():
                default_output = output_dir / "raw_file_inspection_results.csv"
                if default_output.exists():
                    output_file_path = str(default_output)

            if output_file_path and Path(output_file_path).exists():
                # Show summary statistics
                try:
                    results_df = pd.read_csv(output_file_path)

                    # Count successful vs failed
                    failed_count = len(results_df[results_df["error"].notna()])
                    success_count = len(results_df) - failed_count

                    if success_count > 0:
                        # Show instrument summary if available
                        if "instrument_model" in results_df.columns:
                            instruments = results_df["instrument_model"].value_counts()
                            self.logger.info(f"Instrument models: {dict(instruments)}")

                except Exception as e:
                    self.logger.warning(f"Could not read results summary: {e}")

                # Set skip trigger on successful completion
                self.set_skip_trigger("raw_data_inspected", True)

                return output_file_path
            else:
                self.logger.warning(f"Could not find output file: {result.stdout}")
                return None
        else:
            self.logger.error(
                f"Raw data inspection failed with return code: {result.returncode}"
            )
            self.logger.error("Standard error:")
            self.logger.error(result.stderr)
            self.logger.error("Standard output:")
            self.logger.error(result.stdout)
            return None

    def get_raw_inspection_results_path(self) -> Optional[str]:
        """
        Get the path to the raw data inspection results file.

        Returns:
            Path to the raw inspection results CSV file if it exists, None otherwise
        """
        results_file = (
            self.workflow_path / "raw_file_info" / "raw_file_inspection_results.csv"
        )
        if results_file.exists():
            return str(results_file)
        return None

    @skip_if_complete("processed_data_uploaded_to_minio", return_value=True)
    def upload_processed_data_to_minio(self) -> bool:
        """
        Upload processed data files to MinIO object storage.

        Workflow-specific wrapper around upload_to_minio() that handles processed data uploads.
        Uses configuration to determine source directory, target bucket, and folder structure.
        Includes skip triggers and workflow-specific validation.

        Returns:
            True if upload completed successfully, False otherwise

        Note:
            Uses config paths for source directory and MinIO settings.
            Creates folder structure: bucket/study_name/processed_data/
        """
        if not self.minio_client:
            self.logger.error(
                "MinIO client not available. Please set MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables."
            )
            self.logger.error(
                "Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables"
            )
            return False

        processed_data_dir = self.processed_data_directory
        if not processed_data_dir:
            self.logger.error("processed_data_directory not configured")
            return False

        processed_path = Path(processed_data_dir)
        if not processed_path.exists():
            self.logger.error(f"Processed data directory not found: {processed_path}")
            return False

        # Check if there are any processed files
        processed_files = list(processed_path.rglob("*.csv")) + list(
            processed_path.rglob("*.json")
        )
        if not processed_files:
            self.logger.warning(f"No processed files found in {processed_path}")
            return True  # Not an error, just nothing to upload

        bucket_name = self.config["minio"]["bucket"]
        folder_name = (
            self.config["study"]["name"]
            + "/processed_"
            + self.config["workflow"]["processed_data_date_tag"]
        )

        self.logger.info("Uploading processed data to MinIO...")

        try:
            # Use the core upload_to_minio method
            self.upload_to_minio(
                local_directory=str(processed_path),
                bucket_name=bucket_name,
                folder_name=folder_name,
                file_pattern="*",  # Upload all files
            )

            # Set success trigger regardless of whether files were uploaded or skipped
            # (both indicate the operation completed successfully)
            self.set_skip_trigger("processed_data_uploaded_to_minio", True)

            return True

        except Exception as e:
            self.logger.error(f"Error uploading processed data to MinIO: {e}")
            return False


class WorkflowMetadataManager:
    """
    Mixin class for generating workflow metadata files.
    """

    def _generate_workflow_metadata_inputs_common(
        self, workflow_specific_processor
    ) -> bool:
        """
        Common metadata generation logic shared by all workflow types.

        Args:
            workflow_specific_processor: Function that takes merged_df and returns (merged_df, final_columns)
                                        to add workflow-specific columns and define final column list

        Returns:
            bool: True if metadata generation is successful, False otherwise
        """
        # Check prerequisites
        biosample_mapping_file = (
            self.workflow_path / "metadata" / "mapped_raw_file_biosample_mapping.csv"
        )
        if not biosample_mapping_file.exists():
            self.logger.error(
                f"Biosample mapping file not found: {biosample_mapping_file}"
            )
            return False

        raw_inspection_results = self.get_raw_inspection_results_path()
        if not raw_inspection_results:
            self.logger.error(
                "Raw data inspection results not found. Run raw_data_inspector first."
            )
            return False

        # Load the mapped files (high confidence only)
        mapped_df = pd.read_csv(biosample_mapping_file)
        mapped_df = mapped_df[mapped_df["match_confidence"].isin(["high"])].copy()
        if len(mapped_df) == 0:
            self.logger.error("No high confidence biosample matches found")
            return False

        mapped_df["raw_data_file_short"] = mapped_df["raw_file_name"]

        # Add raw data file paths
        raw_data_dir = str(self.raw_data_directory)
        if not raw_data_dir.endswith("/"):
            raw_data_dir += "/"
        mapped_df["raw_data_file"] = raw_data_dir + mapped_df["raw_data_file_short"]

        # Load and filter inspection results
        file_info_df = pd.read_csv(raw_inspection_results)
        initial_count = len(file_info_df)

        # Remove files with errors
        if "error" in file_info_df.columns:
            error_mask = file_info_df["error"].notna()
            if error_mask.any():
                error_files = file_info_df[error_mask]["file_name"].tolist()
                self.logger.warning(
                    f"Excluding {len(error_files)} files with processing errors:"
                )
                for f in error_files[:5]:
                    self.logger.warning(f"- {f}")
                if len(error_files) > 5:
                    self.logger.warning(f"... and {len(error_files) - 5} more")
                file_info_df = file_info_df[~error_mask]

        # Remove files with missing write_time
        null_time_mask = file_info_df["write_time"].isna()
        if null_time_mask.any():
            null_time_files = file_info_df[null_time_mask]["file_name"].tolist()
            self.logger.warning(
                f"Excluding {len(null_time_files)} files with missing write_time:"
            )
            for f in null_time_files:
                self.logger.warning(f"- {f}")
            file_info_df = file_info_df[~null_time_mask]

        final_count = len(file_info_df)
        if final_count != initial_count:
            self.logger.info(
                f"Raw inspection results: {initial_count} → {final_count} files (excluded {initial_count - final_count} with errors)"
            )
        if final_count == 0:
            self.logger.error(
                "No valid files remaining after filtering - check raw data inspection results"
            )
            return False

        # Process instrument metadata
        file_info_df["instrument_instance_specifier"] = file_info_df[
            "instrument_serial_number"
        ].astype(str)
        file_info_df["instrument_analysis_end_date"] = pd.to_datetime(
            file_info_df["write_time"]
        ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        file_info_df["raw_data_file_short"] = file_info_df["file_name"]

        serial_numbers_to_remove = self.config.get("metadata", {}).get(
            "serial_numbers_to_remove", []
        )
        if serial_numbers_to_remove:
            file_info_df["instrument_instance_specifier"] = file_info_df[
                "instrument_instance_specifier"
            ].replace(serial_numbers_to_remove, pd.NA)

        self.logger.debug(
            f"Unique instrument_instance_specifier values: {file_info_df['instrument_instance_specifier'].unique()}"
        )
        valid_dates = file_info_df["instrument_analysis_end_date"].dropna()
        if len(valid_dates) > 0:
            pass
        else:
            self.logger.debug("No valid dates found")

        # Merge with mapped files
        file_info_columns = [
            "raw_data_file_short",
            "instrument_analysis_end_date",
            "instrument_instance_specifier",
            "write_time",
        ]
        file_info_merge = file_info_df[file_info_columns].drop_duplicates(
            subset=["raw_data_file_short"]
        )
        merged_df = pd.merge(
            mapped_df, file_info_merge, on="raw_data_file_short", how="left"
        )

        if len(merged_df) != len(mapped_df):
            self.logger.error(
                f"Merge error: expected {len(mapped_df)} rows, got {len(merged_df)}"
            )
            return False

        # Check for missing metadata
        missing_metadata = merged_df["instrument_analysis_end_date"].isna().sum()
        if missing_metadata > 0:
            self.logger.warning(
                f"{missing_metadata} files missing instrument metadata (may not be in raw inspection results)"
            )
            missing_files = merged_df[merged_df["instrument_analysis_end_date"].isna()][
                "raw_data_file_short"
            ].tolist()
            for f in missing_files[:5]:
                self.logger.warning(f"- {f}")
            if len(missing_files) > 5:
                self.logger.warning(f"... and {len(missing_files) - 5} more")
            merged_df = merged_df[
                merged_df["instrument_analysis_end_date"].notna()
            ].copy()
            self.logger.info(
                f"Proceeding with {len(merged_df)} files that have complete metadata"
            )

        # Add common metadata
        metadata_config = self.config.get("metadata", {})
        merged_df["processing_institution_workflow"] = metadata_config.get(
            "processing_institution_workflow", "EMSL"
        )
        merged_df["processing_institution_generation"] = metadata_config.get(
            "processing_institution_generation", "EMSL"
        )
        merged_df["sample_id"] = merged_df["biosample_id"]

        # Handle raw_data_url (MASSIVE vs MinIO)
        raw_data_location = self.config.get("metadata", {}).get(
            "raw_data_location", "massive"
        )
        include_raw_data_url = False

        if raw_data_location.lower() == "massive":
            include_raw_data_url = True
            ftp_file = self.workflow_path / "raw_file_info" / "massive_ftp_locs.csv"
            if ftp_file.exists():
                ftp_df = pd.read_csv(ftp_file)
                ftp_mapping = dict(
                    zip(ftp_df["raw_data_file_short"], ftp_df["ftp_location"])
                )
            else:
                raise ValueError(f"MASSIVE FTP URLs file not found: {ftp_file}")

            massive_id = self.config["workflow"]["massive_id"]

            def construct_massive_url(filename):
                import urllib.parse
                import re

                if "MSV" in massive_id:
                    msv_part = "MSV" + massive_id.split("MSV")[1]
                else:
                    msv_part = massive_id

                if filename in ftp_mapping:
                    ftp_url = ftp_mapping[filename]
                    match = re.search(
                        rf"{re.escape(msv_part)}(.+)/{re.escape(filename)}", ftp_url
                    )
                    if match:
                        file_path = f"{msv_part}{match.group(1)}/{filename}"
                    else:
                        file_path = f"{msv_part}/raw/{filename}"
                else:
                    file_path = f"{msv_part}/raw/{filename}"

                encoded_path = urllib.parse.quote(file_path, safe="")
                https_url = f"https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f.{encoded_path}&forceDownload=true"
                if not https_url.startswith(
                    "https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f.MSV"
                ):
                    raise ValueError(
                        f"Invalid MASSIVE URL format generated: {https_url}"
                    )
                return https_url

            merged_df["raw_data_url"] = merged_df["raw_data_file_short"].apply(
                construct_massive_url
            )
            self._validate_massive_urls(merged_df["raw_data_url"].head(5).tolist())
        elif raw_data_location.lower() == "minio":
            pass
        else:
            raise ValueError(
                f"Unsupported raw_data_location: {raw_data_location}, must be 'massive' or 'minio'"
            )

        # Remove problem files
        problem_files = self.config.get("problem_files", [])
        if problem_files:
            initial_count = len(merged_df)
            merged_df = merged_df[
                ~merged_df["raw_data_file_short"].isin(problem_files)
            ].copy()
            self.logger.warning(
                f"Removed {initial_count - len(merged_df)} problematic files from metadata generation"
            )

        # Remove rows without sample_id (e.g., calibration-only files)
        initial_count = len(merged_df)
        merged_df = merged_df[
            merged_df["sample_id"].notna() & (merged_df["sample_id"] != "")
        ].copy()
        removed_count = initial_count - len(merged_df)
        if removed_count > 0:
            self.logger.info(
                f"Removed {removed_count} rows without sample_id (calibration files)"
            )

        # Call workflow-specific processor to add workflow-specific columns and get final column list
        merged_df, final_columns = workflow_specific_processor(
            merged_df, raw_inspection_results, include_raw_data_url
        )

        # Generate configuration-specific CSV files
        config_dfs = self._separate_files_by_configuration(merged_df, metadata_config)
        if not config_dfs:
            self.logger.error("No files matched any configuration filters")
            return False

        # Create output directory and clear existing files
        output_dir = self.workflow_path / "metadata" / "metadata_gen_input_csvs"
        output_dir.mkdir(parents=True, exist_ok=True)
        for f in output_dir.glob("*.csv"):
            f.unlink()

        # Write configuration-specific CSV files
        files_written = 0
        total_files = 0

        for config_name, config_df in config_dfs.items():
            missing_cols = [
                col for col in final_columns if col not in config_df.columns
            ]
            if missing_cols:
                self.logger.error(
                    f"Skipping {config_name}: missing columns {missing_cols}"
                )
                continue
            if len(config_df) == 0:
                self.logger.warning(f"Skipping {config_name}: no files after filtering")
                continue

            try:
                output_df = config_df[final_columns].copy()
                output_file = output_dir / f"{config_name}_metadata.csv"
                output_df.to_csv(output_file, index=False)
                files_written += 1
                total_files += len(output_df)
            except Exception as e:
                self.logger.error(f"Error writing {config_name}_metadata.csv: {e}")
                continue

        if files_written == 0:
            self.logger.error("No metadata files were successfully written")
            return False

        return True

    def _generate_lcms_workflow_metadata_inputs(self) -> bool:
        """Generate metadata inputs specific to LCMS Metabolomics and Lipidomics workflows.

        Returns:
            bool: True if metadata generation is successful, False otherwise
        """

        def lcms_processor(merged_df, raw_inspection_results, include_raw_data_url):
            # Add processed_data_directory for LCMS
            processed_data_dir = str(self.processed_data_directory)
            if not processed_data_dir.endswith("/"):
                processed_data_dir += "/"
            merged_df["processed_data_directory"] = (
                processed_data_dir
                + merged_df["raw_data_file_short"].str.replace(
                    r"(?i)\.(raw|mzml)$", "", regex=True
                )
                + ".corems"
            )

            # Define final columns for LCMS
            final_columns = [
                "sample_id",
                "raw_data_file",
                "processed_data_directory",
                "mass_spec_configuration_name",
                "chromat_configuration_name",
                "instrument_used",
                "processing_institution_workflow",
                "processing_institution_generation",
                "instrument_analysis_end_date",
                "instrument_instance_specifier",
            ]
            if include_raw_data_url:
                final_columns.append("raw_data_url")

            return merged_df, final_columns

        return self._generate_workflow_metadata_inputs_common(lcms_processor)

    def _generate_gcms_workflow_metadata_inputs(self) -> bool:
        """Generate metadata inputs specific to GCMS Metabolomics workflows.

        Returns:
            bool: True if metadata generation is successful, False otherwise
        """

        def gcms_processor(merged_df, raw_inspection_results, include_raw_data_url):
            # Add processed_data_file for GCMS (CSV files)
            processed_data_dir = str(self.processed_data_directory)
            if not processed_data_dir.endswith("/"):
                processed_data_dir += "/"
            merged_df["processed_data_file"] = processed_data_dir + merged_df[
                "raw_data_file_short"
            ].str.replace(r"(?i)\.(cdf|mzml)$", ".csv", regex=True)

            # Match samples to calibration files
            merged_df = self._assign_calibration_files_to_samples(
                merged_df, raw_inspection_results
            )

            # Define final columns for GCMS
            final_columns = [
                "sample_id",
                "raw_data_file",
                "processed_data_file",
                "calibration_file",
                "mass_spec_configuration_name",
                "chromat_configuration_name",
                "instrument_used",
                "processing_institution_workflow",
                "processing_institution_generation",
                "instrument_analysis_end_date",
                "instrument_instance_specifier",
            ]
            if include_raw_data_url:
                final_columns.append("raw_data_url")

            return merged_df, final_columns

        return self._generate_workflow_metadata_inputs_common(gcms_processor)

    def _assign_calibration_files_to_samples(
        self, merged_df: pd.DataFrame, raw_inspection_results: str
    ) -> pd.DataFrame:
        """
        Assign calibration files to samples based on chronological order.

        Matches each sample file to the most recent calibration file that was run before it.
        Uses the same logic as _generate_gcms_metab_wdl to ensure consistency between
        WDL execution and metadata generation.

        Args:
            merged_df: DataFrame with sample files (must have 'raw_data_file_short' and 'write_time' columns)
            raw_inspection_results: Path to raw inspection results CSV

        Returns:
            DataFrame with added 'calibration_file' column containing full paths to calibration files
        """
        # Load full biosample mapping to identify calibration files
        mapping_file = (
            self.workflow_path / "metadata" / "mapped_raw_file_biosample_mapping.csv"
        )
        if not mapping_file.exists():
            raise FileNotFoundError(
                f"Biosample mapping not found: {mapping_file}. Run biosample mapping first."
            )

        mapping_df = pd.read_csv(mapping_file)

        # Load inspection results for all files (samples + calibrations)
        inspection_df = pd.read_csv(raw_inspection_results)

        # Get calibration files from mapping
        calibration_files_df = mapping_df[
            mapping_df["raw_file_type"] == "calibration"
        ].copy()

        if len(calibration_files_df) == 0:
            raise ValueError(
                "No calibration files found in biosample mapping. At least one calibration file is required for GCMS."
            )

        # Merge calibration files with their write_time from inspection results
        calibration_files_df = calibration_files_df.merge(
            inspection_df[["file_name", "write_time"]],
            left_on="raw_file_name",
            right_on="file_name",
            how="left",
        )

        # Convert write_time to datetime for both samples and calibrations
        merged_df["write_time_dt"] = pd.to_datetime(merged_df["write_time"])
        calibration_files_df["write_time_dt"] = pd.to_datetime(
            calibration_files_df["write_time"]
        )

        # Sort calibrations by time
        calibration_files_df = calibration_files_df.sort_values("write_time_dt")

        # Build raw data directory path
        raw_data_dir = str(self.raw_data_directory)
        if not raw_data_dir.endswith("/"):
            raw_data_dir += "/"

        # Function to find the appropriate calibration file for each sample
        def find_calibration_for_sample(sample_time):
            """Find the most recent calibration before this sample time."""
            # Find calibrations that were run before or at the same time as this sample
            valid_calibrations = calibration_files_df[
                calibration_files_df["write_time_dt"] <= sample_time
            ]

            if len(valid_calibrations) == 0:
                # No calibration before this sample - use the first calibration (with warning logged once)
                return calibration_files_df.iloc[0]["raw_file_name"]
            else:
                # Use the most recent calibration before this sample
                return valid_calibrations.iloc[-1]["raw_file_name"]

        # Assign calibration file to each sample
        merged_df["calibration_file_short"] = merged_df["write_time_dt"].apply(
            find_calibration_for_sample
        )
        merged_df["calibration_file"] = (
            raw_data_dir + merged_df["calibration_file_short"]
        )

        # Check for samples that use calibration from after their run time (shouldn't happen, but check)
        early_samples = []
        first_cal_time = calibration_files_df.iloc[0]["write_time_dt"]
        for idx, row in merged_df.iterrows():
            if row["write_time_dt"] < first_cal_time:
                early_samples.append(row["raw_data_file_short"])

        if early_samples:
            self.logger.warning(
                f"{len(early_samples)} sample(s) were run before any calibration:"
            )
            for sample in early_samples[:5]:
                self.logger.warning(f"    - {sample}")
            if len(early_samples) > 5:
                self.logger.warning(f"    ... and {len(early_samples) - 5} more")
            self.logger.warning(
                f"    These will use the first calibration: {calibration_files_df.iloc[0]['raw_file_name']}"
            )

        # Report calibration file assignment summary
        calibration_counts = merged_df["calibration_file_short"].value_counts()
        self.logger.debug("Calibration file assignments:")
        for cal_file, count in calibration_counts.items():
            self.logger.debug(f"    {cal_file}: {count} samples")

        # Drop temporary columns
        merged_df = merged_df.drop(
            columns=["write_time_dt", "calibration_file_short", "write_time"]
        )

        return merged_df

    @skip_if_complete("metadata_mapping_generated", return_value=True)
    def generate_workflow_metadata_generation_inputs(self) -> bool:
        """
        Generate metadata mapping files for generating workflow metadata.

        Creates workflow metadata CSV files separated by configuration that include:
        - Raw data file paths and processed data directories
        - Instrument information and analysis timestamps
        - Configuration-specific metadata for NMDC submission

        Returns:
            True if successful, False otherwise

        Raises:
            ValueError: If workflow_type is not set in config
            NotImplementedError: If workflow_type is not yet supported
        """
        self.logger.info("Generating metadata mapping files...")

        try:
            # Use workflow-specific generator to handle all processing
            workflow_type = self.config["workflow"]["workflow_type"]

            if workflow_type not in WORKFLOW_DICT:
                raise NotImplementedError(
                    f"Unsupported workflow type: {workflow_type}. Supported types: {list(WORKFLOW_DICT.keys())}"
                )

            # Get the generator method name from WORKFLOW_DICT and call it
            wf_input_gen_method_name = WORKFLOW_DICT[workflow_type][
                "workflow_metadata_input_generator"
            ]
            wf_input_gen = getattr(self, wf_input_gen_method_name)

            # Call workflow-specific processing function
            success = wf_input_gen()

            # Replace biosample_id (sample_id) with processed_sample_id from material processing metadata
            if success:
                success = self._update_sample_ids_to_processed_sample_ids()
            
            if success:
                self.set_skip_trigger("metadata_mapping_generated", True)
                return True
            else:
                return False

        except Exception as e:
            self.logger.error(f"Error generating metadata mapping files: {e}")
            import traceback

            traceback.print_exc()
            return False

    def _update_sample_ids_to_processed_sample_ids(self) -> bool:
        """
        Update sample_id in metadata CSV files to use processed_sample_id from material processing metadata.

        Reads the material processing metadata workflowreference CSV to get the mapping from
        raw_data_identifier to processed_sample_id, then updates all the metadata CSV files in
        metadata_gen_input_csvs directory to use processed_sample_id instead of biosample_id.
        
        The merge is done on raw_data_identifier (from workflow reference) matching the filename
        extracted from raw_data_file (from input CSVs), since a single biosample can have multiple
        processed samples corresponding to different raw data files.

        Returns:
            True if successful, False otherwise
        """
        self.logger.info("Updating sample_id to processed_sample_id in metadata CSV files...")

        try:
            # Load the material processing workflowreference CSV
            workflowref_file = (
                self.workflow_path / "metadata" / "nmdc_submission_packages" / 
                "material_processing_metadata_workflowreference.csv"
            )
            
            if not workflowref_file.exists():
                self.logger.error(
                    f"Material processing workflowreference file not found: {workflowref_file}"
                )
                self.logger.error(
                    "Run generate_material_processing_metadata() first"
                )
                return False

            # Read the mapping from raw_data_identifier to last_processed_sample
            workflowref_df = pd.read_csv(workflowref_file)
            
            # Verify required columns exist
            if "raw_data_identifier" not in workflowref_df.columns or "last_processed_sample" not in workflowref_df.columns:
                self.logger.error(
                    f"Material processing workflowreference file missing required columns. "
                    f"Expected: 'raw_data_identifier', 'last_processed_sample'. "
                    f"Found columns: {list(workflowref_df.columns)}"
                )
                return False

            # Select only the columns we need for the merge and rename for clarity
            mapping_df = workflowref_df[["raw_data_identifier", "last_processed_sample"]].copy()
            mapping_df = mapping_df.rename(columns={"last_processed_sample": "processed_sample_id"})

            self.logger.info(
                f"Found {len(mapping_df)} raw_data_identifier to processed_sample_id mappings"
            )

            # Update all metadata CSV files in metadata_gen_input_csvs directory
            input_dir = self.workflow_path / "metadata" / "metadata_gen_input_csvs"
            if not input_dir.exists():
                self.logger.error(f"Metadata input directory not found: {input_dir}")
                return False

            csv_files = list(input_dir.glob("*.csv"))
            if not csv_files:
                self.logger.error(f"No CSV files found in {input_dir}")
                return False

            updated_count = 0
            total_rows_updated = 0

            for csv_file in csv_files:
                # Read the CSV
                df = pd.read_csv(csv_file)
                
                # Check if required columns exist
                if "sample_id" not in df.columns:
                    self.logger.warning(f"Skipping {csv_file.name}: no sample_id column found")
                    continue
                    
                if "raw_data_file" not in df.columns:
                    self.logger.warning(f"Skipping {csv_file.name}: no raw_data_file column found")
                    continue

                # Extract raw data filename from the full path
                df["raw_data_file_short"] = df["raw_data_file"].apply(lambda x: Path(x).name)
                
                # Store original row count for reporting
                original_row_count = len(df)
                
                # Merge with mapping to get processed_sample_id based on raw data filename
                # Left merge to keep all rows from df, adding processed_sample_id column
                df_merged = df.merge(
                    mapping_df,
                    left_on="raw_data_file_short",
                    right_on="raw_data_identifier",
                    how="left"
                )
                
                # Check for any unmapped raw data files (would have NaN in processed_sample_id)
                unmapped_mask = df_merged["processed_sample_id"].isna()
                if unmapped_mask.any():
                    self.logger.warning(
                        f"In {csv_file.name}: {unmapped_mask.sum()} rows have raw data files that could not be mapped to processed_sample_id"
                    )
                    continue # Skip this file

                # Check that the shape was preserved
                if len(df_merged) != original_row_count:
                    self.logger.error(
                        f"Mismatch in row count after merging to processed_sample_id for {csv_file.name}: "
                    )
                    continue # Skip this file

                # Replace sample_id with processed_sample_id
                df_merged["sample_id"] = df_merged["processed_sample_id"]
                
                # Drop the temporary columns from the merge
                df_merged = df_merged.drop(columns=["raw_data_file_short", "raw_data_identifier", "processed_sample_id"])

                # Write updated CSV back to file
                df_merged.to_csv(csv_file, index=False)
                updated_count += 1
                total_rows_updated += len(df_merged)
                self.logger.info(
                    f"Updated {csv_file.name}: {len(df_merged)} rows with processed_sample_id"
                )

            if updated_count == 0:
                self.logger.error("No CSV files were successfully updated")
                return False
            
            if updated_count < len(csv_files):
                self.logger.warning(
                    f"Only {updated_count} out of {len(csv_files)} CSV files were updated successfully"
                )
                return False

            self.logger.info(
                f"Successfully updated {updated_count} CSV files with {total_rows_updated} total rows"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error updating sample_ids to processed_sample_ids: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _separate_files_by_configuration(
        self, merged_df: pd.DataFrame, metadata_config: dict
    ) -> dict:
        """
        Separate files by configuration and apply configuration-specific metadata.

        Args:
            merged_df: DataFrame with merged biosample and raw file metadata
            metadata_config: Global metadata configuration from config file

        Returns:
            Dictionary mapping configuration names to DataFrames with applied metadata
        """
        config_dfs = {}

        # Get default metadata values
        default_instrument = metadata_config.get("instrument_used", "Unknown")
        default_mass_spec = metadata_config.get(
            "mass_spec_configuration_name", "Unknown"
        )
        default_chromat = metadata_config.get("chromat_configuration_name", "Unknown")

        for config in self.config.get("configurations", []):
            config_name = config["name"]
            file_filters = config.get("file_filter", [])

            # Filter files for this configuration using AND logic (all filters must match)
            if file_filters:
                matching_indices = []
                for idx, row in merged_df.iterrows():
                    filename = row["raw_data_file_short"].lower()
                    if all(
                        filter_term.lower() in filename for filter_term in file_filters
                    ):
                        matching_indices.append(idx)

                if matching_indices:
                    config_df = merged_df.loc[matching_indices].copy()
                else:
                    self.logger.warning(
                        f"Configuration '{config_name}': No files match filters {file_filters}"
                    )
                    continue
            else:
                # No filters specified - include all files
                config_df = merged_df.copy()

            # Apply configuration-specific metadata (with fallback to defaults)
            config_df["instrument_used"] = config.get(
                "instrument_used", default_instrument
            )
            config_df["chromat_configuration_name"] = config.get(
                "chromat_configuration_name", default_chromat
            )
            config_df["mass_spec_configuration_name"] = config.get(
                "mass_spec_configuration_name", default_mass_spec
            )

            # Apply pattern-based metadata overrides
            metadata_overrides = config.get("metadata_overrides", {})
            if metadata_overrides:
                for metadata_field, pattern_mapping in metadata_overrides.items():
                    if pattern_mapping:
                        # Apply pattern-specific overrides based on filename patterns
                        def get_override_value(
                            filename, field_name, mapping, fallback_value
                        ):
                            for pattern, override_value in mapping.items():
                                if pattern in filename:
                                    return override_value
                            # Return current value if no pattern matches
                            return fallback_value

                        # Get current values as fallback
                        current_values = (
                            config_df[metadata_field]
                            if metadata_field in config_df.columns
                            else config.get(metadata_field, "Unknown")
                        )
                        config_df[metadata_field] = config_df[
                            "raw_data_file_short"
                        ].apply(
                            lambda filename: get_override_value(
                                filename,
                                metadata_field,
                                pattern_mapping,
                                current_values,
                            )
                        )

            config_dfs[config_name] = config_df

            # Report results with pattern-based differentiation if applicable
            # Extract chromat configuration if needed for reporting (unused)

            # Check for pattern-based overrides in any metadata field
            metadata_overrides = config.get("metadata_overrides", {})
            override_summaries = []

            for metadata_field, pattern_mapping in metadata_overrides.items():
                if pattern_mapping and metadata_field in config_df.columns:
                    unique_values = config_df[metadata_field].unique()
                    if len(unique_values) > 1:
                        # Multiple values due to pattern-based overrides
                        value_breakdown = config_df[metadata_field].value_counts()
                        field_desc = ", ".join(
                            [
                                f"{count} files with {val[:25]}..."
                                if len(val) > 25
                                else f"{count} files with {val}"
                                for val, count in value_breakdown.items()
                            ]
                        )
                        override_summaries.append(f"{metadata_field}: {field_desc}")

        # Fallback: if no configurations worked, create single dataset with defaults
        if not config_dfs:
            self.logger.warning(
                "No configurations matched any files - creating fallback configuration"
            )
            fallback_df = merged_df.copy()
            fallback_df["instrument_used"] = default_instrument
            fallback_df["mass_spec_configuration_name"] = default_mass_spec
            fallback_df["chromat_configuration_name"] = default_chromat
            config_dfs["all_data"] = fallback_df
            self.logger.info(
                f"Fallback configuration: {len(fallback_df)} files with default metadata"
            )

        return config_dfs

    def _validate_massive_urls(self, urls: List[str], max_attempts: int = 5) -> bool:
        """
        Validate MASSIVE URLs to ensure they're accessible.

        Args:
            urls: List of URLs to validate
            max_attempts: Maximum number of URLs to test

        Returns:
            True if at least one URL is accessible, False otherwise

        Raises:
            ValueError: If no URLs are accessible
        """
        import urllib.request
        import urllib.error
        import ssl

        # Create SSL context that ignores certificate verification for MASSIVE
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        successful_urls = 0
        total_tested = min(len(urls), max_attempts)

        for i, url in enumerate(urls[:max_attempts]):
            try:
                # Use HEAD request to check accessibility without downloading
                req = urllib.request.Request(url, method="HEAD")
                response = urllib.request.urlopen(req, context=ssl_context, timeout=15)

                if response.status == 200:
                    successful_urls += 1
                    pass

                    # Check if it looks like a file download
                    content_length = response.headers.get("Content-Length")
                    if content_length:
                        pass
                else:
                    self.logger.warning(
                        f"URL {i + 1}/{total_tested}: Unexpected status {response.status}"
                    )

            except urllib.error.HTTPError as e:
                self.logger.error(
                    f"URL {i + 1}/{total_tested}: HTTP {e.code} - {e.reason}"
                )
                if e.code == 404:
                    self.logger.error("This file may not exist in the MASSIVE dataset")
            except Exception as e:
                self.logger.error(
                    f"URL {i + 1}/{total_tested}: {type(e).__name__}: {e}"
                )

        if successful_urls == 0:
            raise ValueError(
                f"None of the {total_tested} tested MASSIVE URLs are accessible. "
                "Check the MASSIVE dataset ID and file paths."
            )
        elif successful_urls < total_tested // 2:
            self.logger.warning(
                f"Only {successful_urls}/{total_tested} URLs are accessible. "
                "Some files may not be available in MASSIVE."
            )

        return True

    @skip_if_complete("metadata_packages_generated", return_value=True)
    def _generate_processing_metadata(self, test=False) -> bool:
        """
        Generate NMDC metadata packages for workflow execution and related records

        Creates workflow metadata JSON files for NMDC submission using the
        nmdc-ms-metadata-gen package. Generates one metadata package per
        configuration, using the metadata mapping CSV files created by
        generate_metadata_mapping_files().

        Args:
            test: If True, runs in test mode (may alter behavior for testing)

        Returns:
            True if metadata generation completed successfully, False otherwise

        Raises:
            ValueError: If workflow_type is not set in config
            NotImplementedError: If workflow_type is not yet supported

        Note:
            Requires nmdc-ms-metadata-gen package to be installed.
            Uses processed_data_url from MinIO configuration or constructs from
            MinIO endpoint, bucket, and study name.
            Validates metadata against NMDC schema both locally and via API.
        """
        self.logger.info("Generating NMDC metadata packages...")

        # Get workflow-specific metadata generator class
        workflow_type = self.config["workflow"]["workflow_type"]
        if workflow_type not in WORKFLOW_DICT:
            raise ValueError(
                f"Unsupported workflow type: {workflow_type}. Supported types: {list(WORKFLOW_DICT.keys())}"
            )

        metadata_generator_class = WORKFLOW_DICT[workflow_type][
            "metadata_generator_class"
        ]
        if metadata_generator_class is None:
            raise ImportError(
                "nmdc-ms-metadata-gen package not installed. Install with 'pip install nmdc-ms-metadata-gen'."
            )

        # Check for metadata mapping input files
        input_csv_dir = self.workflow_path / "metadata" / "metadata_gen_input_csvs"
        if not input_csv_dir.exists() or not any(input_csv_dir.glob("*.csv")):
            self.logger.error("No metadata mapping CSV files found")
            self.logger.error(f"Expected location: {input_csv_dir}")
            self.logger.error("Run generate_metadata_mapping_files() first")
            return False

        # Build URL from MinIO config
        minio_config = self.config.get("minio", {})
        bucket = minio_config.get("bucket", "")

        # Get date tag from workflow config (required)
        processed_date_tag = self.config["workflow"].get("processed_data_date_tag", "")
        if not processed_date_tag:
            raise ValueError(
                "processed_data_date_tag is required in config['workflow']['processed_data_date_tag']. "
                "Example: '20251210'"
            )

        # Construct folder path with date tag
        folder_path = f"{self.study_name}/processed_{processed_date_tag}"

        # Build URL
        processed_data_url = f"https://nmdcdemo.emsl.pnnl.gov/{bucket}/{folder_path}/"

        # Get existing data objects from config (if any)
        existing_data_objects = self.config.get("metadata", {}).get(
            "existing_data_objects", []
        )

        # Construct raw_data_url based on data location
        raw_data_location = self.config.get("metadata", {}).get(
            "raw_data_location", "massive"
        )
        if raw_data_location.lower() == "minio":
            # Construct MinIO raw data URL: https://nmdcdemo.emsl.pnnl.gov/bucket/study_name/raw/
            minio_endpoint = self.config.get("minio", {}).get(
                "public_url_base", "https://nmdcdemo.emsl.pnnl.gov"
            )
            # Remove any trailing slashes from endpoint
            minio_endpoint = minio_endpoint.rstrip("/")
            raw_data_url = f"{minio_endpoint}/{bucket}/{self.study_name}/raw/"
        else:
            # Use configured raw_data_url or None for MASSIVE
            raw_data_url = self.config.get("metadata", {}).get("raw_data_url", None)

        # Get GCMS-specific configuration file name if needed
        configuration_file_name = self.config.get("metadata", {}).get(
            "configuration_file_name", None
        )

        # Create output directory for workflow metadata JSON files
        output_dir = self.workflow_path / "metadata" / "nmdc_submission_packages"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Process each metadata mapping CSV file
        csv_files = list(input_csv_dir.glob("*.csv"))

        success_count = 0
        failed_files = []

        for csv_file in csv_files:
            # Derive output filename from input (e.g., hilic_pos_metadata.csv -> workflow_metadata_hilic_pos.json)
            config_name = csv_file.stem.replace("_metadata", "")
            output_file = output_dir / f"workflow_metadata_{config_name}.json"

            try:
                # Build kwargs based on workflow type
                generator_kwargs = {
                    "metadata_file": str(csv_file),
                    "database_dump_json_path": str(output_file),
                    "process_data_url": processed_data_url,
                    "raw_data_url": raw_data_url,
                }

                # Add workflow-specific parameters
                if workflow_type == "GCMS Metabolomics":
                    # GCMS requires configuration_file_name but not existing_data_objects
                    if not configuration_file_name:
                        raise ValueError(
                            "GCMS Metabolomics workflow requires 'configuration_file_name' to be set in "
                            "config['metadata']['configuration_file_name']. Example: 'emsl_gcms_corems_params.toml'"
                        )
                    generator_kwargs["configuration_file_name"] = (
                        configuration_file_name
                    )
                else:
                    # LCMS generators accept existing_data_objects
                    generator_kwargs["existing_data_objects"] = existing_data_objects

                # First attempt to generate and validate metadata in test mode
                # Initialize workflow-specific metadata generator in test mode
                self.logger.info(f"Running {config_name} metadata generation in test mode...")
                generator = metadata_generator_class(test=True, **generator_kwargs)

                # Run metadata generation
                metadata = generator.run()

                # Validate without API first (fast local validation)
                self.logger.info(f"Validating {config_name} metadata (local, test mode)...")
                validate_local = generator.validate_nmdc_database(
                    json=metadata, use_api=False
                )
                if validate_local.get("result") != "All Okay!":
                    self.logger.warning(f"Test mode validation issues for {config_name}: {validate_local}")
                    failed_files.append((csv_file.name, "Test mode validation failed"))
                    continue

                # If test mode succeeded and test=False, run again in production mode
                if not test:
                    self.logger.info(f"Test mode succeeded, running {config_name} metadata generation in production mode...")
                    generator = metadata_generator_class(test=False, **generator_kwargs)
                    
                    # Run metadata generation
                    metadata = generator.run()

                    # Validate without API first (fast local validation)
                    self.logger.info(f"Validating {config_name} metadata (local, production mode)...")
                    validate_local = generator.validate_nmdc_database(
                        json=metadata, use_api=False
                    )
                    if validate_local.get("result") != "All Okay!":
                        self.logger.warning(f"Production mode validation issues for {config_name}: {validate_local}")
                        failed_files.append((csv_file.name, "Production mode validation failed"))
                        continue

                success_count += 1

            except Exception as e:
                self.logger.error(f"Failed to generate metadata: {e}")
                failed_files.append((csv_file.name, str(e)))
                import traceback

                traceback.print_exc()

        if failed_files:
            self.logger.warning("Failed files:")
            for filename, error in failed_files:
                self.logger.warning(f"  - {filename}: {error}")

        self.logger.info(f"Metadata packages saved to: {output_dir}")

        # Set skip trigger only if all files succeeded
        if success_count == len(csv_files):
            self.set_skip_trigger("metadata_packages_generated", True)
            return True
        else:
            self.logger.warning("Some metadata packages failed - review errors above")
            return False

    def submit_metadata_packages(self, environment: str = "dev") -> bool:
        """
        Needs to be implemented.
        """
        raise NotImplementedError("submit_metadata_packages() is not yet implemented.")

    @skip_if_complete("material_processing_metadata_generated", return_value=True)
    def generate_material_processing_metadata(self, test=False) -> bool:
        """
        Generate material processing metadata using MaterialProcessingMetadataGenerator.

        This method creates NMDC-compliant material processing metadata from a YAML protocol
        outline and biosample-to-raw-file mapping. It always runs in test mode first for 
        validation, then optionally runs in production mode to mint real IDs.

        Requires:
        - protocol_info/llm_generated_protocol_outline.yaml: YAML outline of processing steps
        - metadata/mapped_raw_files_wprocessed_MANUAL.csv: CSV mapping biosamples to raw files
          with columns: raw_data_identifier, biosample_id, biosample_name, match_confidence,
          processedsample_placeholder, material_processing_protocol_id
        - config['study']['id']: Study ID in NMDC format (nmdc:sty-XX-XXXXXXXX)
        - Environment variables CLIENT_ID and CLIENT_SECRET for NMDC API authentication

        Args:
            test: If False, runs test mode validation then production mode with real ID minting.
                  If True, only runs test mode (uses test IDs with -13- shoulder).
                  Default: False

        Returns:
            True if successful, False otherwise

        Outputs:
            - metadata/nmdc_submission_packages/material_processing_metadata.json
            - metadata/nmdc_submission_packages/material_processing_metadata_workflowreference.csv
            - metadata/nmdc_submission_packages/material_processing_metadata_validation.txt

        Example:
            >>> manager = NMDCWorkflowManager('config.json')
            >>> # First run in test mode only
            >>> success = manager.generate_material_processing_metadata(test=True)
            >>> # Later run with real ID minting
            >>> success = manager.generate_material_processing_metadata(test=False)
        """
        self.logger.info("Generating material processing metadata...")

        try:
            # Check for required files
            prot_dir = self.workflow_path / "protocol_info"
            yaml_path = prot_dir / "llm_generated_protocol_outline.yaml"


            if not yaml_path.exists():
                self.logger.error(f"Material processing YAML not found: {yaml_path}")
                return False

            # Check for mapped biosample raw data file processed sample
            #TODO: Update this input_csv_path once LLM-helper works.
            input_csv_path = self.workflow_path / "metadata" / "mapped_raw_files_wprocessed_MANUAL.csv"
            if not input_csv_path.exists():
                self.logger.error(f"Input CSV for material processing metadata generation not found: {input_csv_path}")
                self.logger.error("Run generate_material_processing_input_csv() first")
                return False

            # Get study ID from config
            study_id = self.config.get("study", {}).get("id")
            if not study_id:
                self.logger.error("study_id not found in config['study']['id']")
                return False

            # Outputs will be written into self.workflow_path / "metadata" / "nmdc_submission_packages"
            output_dir = self.workflow_path / "metadata" / "nmdc_submission_packages"
            output_dir.mkdir(parents=True, exist_ok=True)
            db_path = output_dir / "material_processing_metadata.json"

            # Get minting config credentials path from config or use default
            minting_config = self.config.get("material_processing", {}).get(
                "minting_config_creds",
                str(Path.home() / ".nmdc" / "config.toml")
            )

            # First attempt to generate and validate metadata in test mode 
            # Initialize MaterialProcessingMetadataGenerator in test mode
            generator = MaterialProcessingMetadataGenerator(
                database_dump_json_path=str(db_path),
                study_id=study_id,
                yaml_outline_path=str(yaml_path),
                sample_to_dg_mapping_path=str(input_csv_path),
                minting_config_creds=minting_config,
                test=True,
            )

            # Run metadata generation
            self.logger.info("Running MaterialProcessingMetadataGenerator in test mode...")
            metadata = generator.run()

            # Validate generated metadata
            self.logger.info("Validating generated metadata...")
            validate = generator.validate_nmdc_database(str(db_path), use_api=False)

            if validate["result"] != "All Okay!":
                self.logger.error(f"Validation of test mode MaterialProcessingMetadataGenerator failed: {validate}")
                return False
            
            # If test mode succeeded and test=False, run again in normal mode
            if not test:
                self.logger.info("Test mode succeeded, running MaterialProcessingMetadataGenerator in production mode...")
                generator = MaterialProcessingMetadataGenerator(
                    database_dump_json_path=str(db_path),
                    study_id=study_id,
                    yaml_outline_path=str(yaml_path),
                    sample_to_dg_mapping_path=str(input_csv_path),
                    minting_config_creds=minting_config,
                    test=False,
                )
                # Run metadata generation
                self.logger.info("Running MaterialProcessingMetadataGenerator in production mode...")
                metadata = generator.run()

                # Validate generated metadata
                self.logger.info("Validating generated metadata...")
                validate = generator.validate_nmdc_database(str(db_path), use_api=False)

                if validate["result"] != "All Okay!":
                    self.logger.error(f"Validation of production mode MaterialProcessingMetadataGenerator failed in: {validate}")
                    return False

            self.logger.info(f"Material processing metadata generated successfully")
            self.logger.info(f"Output directory: {output_dir}")
            self.set_skip_trigger("material_processing_metadata_generated", True)

            return True

        except Exception as e:
            self.logger.error(f"Error generating material processing metadata: {e}")
            import traceback
            traceback.print_exc()
            return False

    def generate_nmdc_metadata_for_workflow(self, test=False) -> bool:
        """
        Generate NMDC metadata for workflow submission.

        This consolidated method handles the complete metadata generation workflow in three steps:
        1. Generates material processing metadata from YAML outline
        2. Generates workflow metadata mapping CSV files with URL validation
        3. Generates NMDC workflow submission packages from the mapping files

        Args:
            test: If True, runs in test mode (if applicable)

        Returns:
            True if all steps completed successfully, False otherwise

        Note:
            Requires material_processing/material_processing_outline.yaml to exist.
            Uses mapped_raw_files.csv as the starting point for all metadata generation.

        Example:
            >>> manager = NMDCWorkflowManager('config.json')
            >>> success = manager.generate_nmdc_metadata_for_workflow()
        """

        # Step 1: Generate material processing metadata
        if not self.generate_material_processing_metadata(test=test):
            self.logger.error("Failed to generate material processing metadata")
            return False

        # Step 2: Generate workflow metadata mapping CSV files
        self.logger.info("Step 2: Generating workflow metadata input CSVs...")
        if not self.generate_workflow_metadata_generation_inputs():
            self.logger.error("Failed to generate metadata mapping files")
            return False

        # Step 3: Generate NMDC workflow submission packages for processing related data
        self.logger.info("Step 3: Generating workflow metadata packages...")
        if not self._generate_processing_metadata(test=test):
            self.logger.error("Failed to generate NMDC metadata packages")
            return False

        self.logger.info("All metadata generation steps completed successfully")
        return True


class LLMWorkflowManagerMixin:
    """
    Mixin class for LLM workflow management.
    """

    def __init__(self):
        """
        Initialize LLMWorkflowManagerMixin.
        """
        self._llm_client = None
        self._conversation_obj = None
    
    @property
    def llm_client(self):
        """Lazy-load LLM client on first access."""
        if self._llm_client is None:
            self._llm_client = LLMClient()
        return self._llm_client
    
    @property
    def conversation_obj(self):
        """Lazy-load conversation object on first access."""
        if self._conversation_obj is None:
            self._conversation_obj = ConversationManager(interaction_type="protocol_conversion")
        return self._conversation_obj
        
    @skip_if_complete("protocol_outline_created", return_value=None)
    def load_protocol_description_to_context(self, protocol_description_path: str) -> None:
        """
        Load protocol description from a text file to the LLM conversation context.

        Parameters
        ----------
        protocol_description_path : str
            Path to the text file containing the protocol description.

        Returns
        -------
        None
        """
        with open(protocol_description_path, "r") as f:
            protocol_description = f.read()
        self.conversation_obj.add_protocol_description(description=protocol_description)
    
    @skip_if_complete("protocol_outline_created", return_value=None)
    def save_yaml_to_file(self, output_path: str, content: str) -> None:
        """
        Save content to a specified file.

        Parameters
        ----------
        output_path : str
            Path to the output file.
        content : str
            Content to be saved to the file.

        Returns
        -------
        None
        """
        if content.startswith("```yaml"):
            content = content.replace("```yaml", "").strip()
        if content.endswith("```"):
            content = content[:-3].strip()

        # Ensure the parent directory exists before writing
        output_path_obj = Path(output_path)
        parent_dir = output_path_obj.parent
        if parent_dir and str(parent_dir) != "":
            parent_dir.mkdir(parents=True, exist_ok=True)

        # Write content to file with basic error handling
        try:
            with open(output_path_obj, "w") as f:
                f.write(content)
        except OSError as e:
            raise RuntimeError(f"Failed to write YAML content to '{output_path}': {e}") from e
    
    @skip_if_complete("protocol_outline_created", return_value=True)
    async def get_llm_generated_yaml_outline(self) -> str:
        """
        Get the LLM generated YAML outline for the loaded protocol description.

        Returns
        -------
        str
            The LLM generated YAML outline.
        """
        
        response = await get_llm_yaml_outline(llm_client=self.llm_client, conversation_obj=self.conversation_obj)
        return response


