"""
NMDC Study Management Utilities

A comprehensive system for managing NMDC metabolomics studies, providing:
- Automated discovery and download of datasets from MASSIVE
- MinIO object storage integration for processed data
- WDL workflow JSON generation for batch processing
- Standardized directory structures and configuration management

This module enables reproducible workflows across different NMDC studies
while maintaining flexibility for study-specific requirements.
"""

import os
import json
import re
import pandas as pd
from pathlib import Path
from minio import Minio
from minio.error import S3Error
from tqdm import tqdm
from typing import Dict, List, Optional

class NMDCStudyManager:
    """
    A configurable class for managing NMDC metabolomics study data workflows.
    
    This class provides a unified interface for:
    - Setting up standardized study directory structures
    - Discovering and downloading raw data from MASSIVE datasets
    - Uploading/downloading processed data to/from MinIO object storage
    - Generating WDL workflow configuration files for batch processing
    
    The class is configured via JSON files that specify study metadata,
    file paths, processing configurations, and dataset identifiers.
    
    Example:
        >>> manager = NMDCStudyManager('config.json')
        >>> manager.create_study_structure()
        >>> ftp_df = manager.get_massive_ftp_urls()
        >>> manager.download_from_massive()
        >>> manager.generate_wdl_jsons()
    """
    
    def __init__(self, config_path: str):
        """
        Initialize the study manager with a configuration file.
        
        Args:
            config_path: Path to the JSON configuration file containing study metadata,
                        paths, MinIO settings, and processing configurations.
                        
        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            json.JSONDecodeError: If the configuration file is invalid JSON
            KeyError: If required configuration fields are missing
        """
        self.config = self.load_config(config_path)
        self.study_name = self.config['study']['name']
        self.study_id = self.config['study']['id']
        self.base_path = Path(self.config['paths']['base_directory'])
        self.study_path = self.base_path / f"_{self.study_name}"
        
        # Initialize MinIO client if credentials available
        self.minio_client = self._init_minio_client()
        
    def load_config(self, config_path: str) -> Dict:
        """
        Load and validate configuration from JSON file.
        
        Args:
            config_path: Path to the JSON configuration file
            
        Returns:
            Dictionary containing the parsed configuration
            
        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def _init_minio_client(self) -> Optional[Minio]:
        """
        Initialize MinIO client using environment variables.
        
        Requires MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables.
        Uses endpoint, security, and bucket settings from configuration.
        
        Returns:
            Configured MinIO client, or None if credentials unavailable
        """
        try:
            return Minio(
                self.config['minio']['endpoint'],
                access_key=os.environ["MINIO_ACCESS_KEY"],
                secret_key=os.environ["MINIO_SECRET_KEY"],
                secure=self.config['minio']['secure']
            )
        except KeyError:
            print("MinIO credentials not found in environment variables")
            return None
    
    def create_study_structure(self):
        """
        Create the standard directory structure for a study.
        
        Creates the following directories under the study path:
        - scripts/: Study-specific scripts and utilities
        - metadata/: Configuration files and study metadata
        - wdl_jsons/: Generated WDL workflow configuration files
        - raw_file_info/: Information about raw data files
        
        Additional subdirectories are created for each processing configuration
        specified in the config file.
        """
        directories = [
            self.study_path,
            self.study_path / "scripts",
            self.study_path / "metadata",
            self.study_path / "wdl_jsons",
            self.study_path / "raw_file_info",
        ]
        
        # Add configuration-specific directories
        if 'configurations' in self.config:
            for config in self.config['configurations']:
                directories.append(self.study_path / "wdl_jsons" / config['name'])
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        
        print(f"Created study structure for {self.study_name} at {self.study_path}")
    
    def get_study_info(self) -> Dict:
        """
        Get summary information about the study configuration.
        
        Returns:
            Dictionary containing study metadata, paths, and configuration summary
        """
        info = {
            'study_name': self.study_name,
            'study_id': self.study_id,
            'study_path': str(self.study_path),
            'massive_id': self.config['study'].get('massive_id', 'Not configured'),
            'file_type': self.config['study'].get('file_type', '.raw'),
            'file_filters': self.config['study'].get('file_filters', []),
            'num_configurations': len(self.config.get('configurations', [])),
            'configuration_names': [c['name'] for c in self.config.get('configurations', [])],
            'raw_data_directory': self.config['paths'].get('raw_data_directory', 'Not configured'),
            'processed_data_directory': self.config['paths'].get('processed_data_directory', 'Not configured'),
            'minio_enabled': self.minio_client is not None
        }
        return info
    
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
        
        log_file = self.study_path / f"{self.study_name}_massive_ftp_locs.txt"
        
        print(f"Crawling MASSIVE FTP directory for dataset: {massive_id}")
        print("This may take several minutes...")
        
        ftp_urls = []
        
        try:
            # Connect to MASSIVE FTP server
            ftp = ftplib.FTP('massive-ftp.ucsd.edu')
            ftp.login()  # Anonymous login
            
            # Navigate to the study directory (massive_id should include version path)
            try:
                ftp.cwd(massive_id)
                print(f"Successfully accessed {massive_id}")
            except ftplib.error_perm:
                print(f"Could not access {massive_id} - check that the path includes version (e.g., 'v07/MSV000094090')")
                return []
            
            def collect_files(relative_path=""):
                """Recursively collect files from FTP directory."""
                try:
                    # Get list of items in current directory
                    items = []
                    ftp.retrlines('LIST', items.append)
                    
                    for item in items:
                        # Parse the LIST output (Unix format)
                        parts = item.split()
                        if len(parts) >= 9:
                            permissions = parts[0]
                            filename = ' '.join(parts[8:])  # Handle filenames with spaces
                            
                            if permissions.startswith('d'):
                                # It's a directory, recurse into it
                                current_dir = ftp.pwd()  # Save current directory
                                try:
                                    ftp.cwd(filename)  # Change to subdirectory
                                    new_relative_path = f"{relative_path}/{filename}" if relative_path else filename
                                    collect_files(new_relative_path)
                                    ftp.cwd(current_dir)  # Go back to parent directory
                                except ftplib.error_perm as e:
                                    print(f"Cannot access directory {filename}: {e}")
                            else:
                                # It's a file, check if it matches the configured file type
                                file_type = self.config['study'].get('file_type', '.raw').lower()
                                if filename.lower().endswith(file_type):
                                    current_path = f"{massive_id}/{relative_path}" if relative_path else massive_id
                                    full_url = f"ftp://massive-ftp.ucsd.edu/{current_path}/{filename}"
                                    ftp_urls.append(full_url)
                                    if len(ftp_urls) % 100 == 0:
                                        print(f"Found {len(ftp_urls)} {file_type} files...")
                        
                except ftplib.error_perm as e:
                    # Permission denied or directory doesn't exist
                    print(f"Cannot access current directory: {e}")
                except Exception as e:
                    print(f"Error processing current directory: {e}")
            
            # Start crawling from the dataset root
            collect_files()
            ftp.quit()
            
            # Write URLs to log file
            with open(log_file, 'w') as f:
                for url in ftp_urls:
                    f.write(f"{url}\n")
            
            file_type = self.config['study'].get('file_type', '.raw').lower()
            print(f"Found {len(ftp_urls)} {file_type} files")
            print(f"URLs saved to: {log_file}")
            
            return str(log_file)
            
        except Exception as e:
            print(f"Error crawling FTP: {e}")
            # Create empty log file
            with open(log_file, 'w') as f:
                f.write("# No files found - FTP crawling failed\n")
            return str(log_file)
    
    def parse_massive_ftp_log(self, log_file: Optional[str] = None, output_file: Optional[str] = None) -> pd.DataFrame:
        """
        Parse FTP crawl log file to extract URLs and create a structured DataFrame.
        
        Processes the text file created by _crawl_massive_ftp() to extract FTP URLs
        and create a pandas DataFrame with location and filename information.
        Applies study-specific file filters if configured.
        
        Args:
            log_file: Path to FTP crawl log file (uses default if not provided)
            output_file: Optional filename to save CSV results (defaults to study name)
            
        Returns:
            DataFrame with columns:
            - ftp_location: Full FTP URL for each file
            - raw_data_file_short: Just the filename portion
            
        File Type and Filtering Details:
            First filters by config['study']['file_type'] (e.g., '.raw', '.mzml', '.d')
            to collect only files of the specified type. Then uses 
            config['study']['file_filters'] list to filter filenames. Files are
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
            log_file = self.study_path / f"{self.study_name}_massive_ftp_locs.txt"
        
        if output_file is None:
            output_file = f"{self.study_name}_massive_ftp_locs.csv"
        
        output_path = self.study_path / output_file
        
        print(f"Parsing FTP log file: {log_file}")
        
        ftp_locs = []
        
        try:
            # Get the configured file type
            file_type = self.config['study'].get('file_type', '.raw').lower()
            
            with open(log_file, "r") as f:
                for line in f:
                    # Look for lines ending with the configured file extension
                    if line.rstrip().lower().endswith(file_type):
                        # Extract the FTP URL (should be the entire line for our format)
                        ftp_url = line.strip()
                        if ftp_url.startswith('ftp://'):
                            ftp_locs.append(ftp_url)
        
            # Remove duplicates and create DataFrame
            ftp_locs = list(set(ftp_locs))
            ftp_df = pd.DataFrame(ftp_locs, columns=["ftp_location"])
            
            # Extract filename from URL - use configured file type for pattern
            file_type = self.config['study'].get('file_type', '.raw').lower().lstrip('.')
            pattern = rf'([^/]+\.{file_type})$'
            ftp_df["raw_data_file_short"] = ftp_df["ftp_location"].str.extract(pattern, flags=re.IGNORECASE)[0]
            
            # Apply file filters if specified
            if 'file_filters' in self.config['study'] and len(ftp_df) > 0:
                original_count = len(ftp_df)
                filter_pattern = '|'.join(self.config['study']['file_filters'])
                ftp_df = ftp_df[ftp_df['raw_data_file_short'].str.contains(filter_pattern, na=False, case=False)]
                print(f"Applied filters ({filter_pattern}), reduced from {original_count} to {len(ftp_df)} files")
                print(f"Filter criteria: filename must contain ANY of: {self.config['study']['file_filters']}")
            elif len(ftp_df) > 0:
                print(f"⚠️  WARNING: No file_filters configured - returning ALL {len(ftp_df)} files!")
                print("   Consider adding 'file_filters' to config to avoid downloading unnecessary files")
            
            # Save to CSV
            ftp_df.to_csv(output_path, index=False)
            print(f"✓ Found {len(ftp_df)} files, saved to {output_path}")
            
            return ftp_df
            
        except FileNotFoundError:
            print(f"✗ Log file not found: {log_file}")
            print("Run crawl_massive_ftp() first to generate the log file")
            return pd.DataFrame(columns=['ftp_location', 'raw_data_file_short'])
        except Exception as e:
            print(f"✗ Error parsing log file: {e}")
            return pd.DataFrame(columns=['ftp_location', 'raw_data_file_short'])
    
    def get_massive_ftp_urls(self, massive_id: Optional[str] = None) -> pd.DataFrame:
        """
        Complete workflow to discover and catalog MASSIVE dataset files with filtering.
        
        Performs a three-step process:
        1. Crawls the MASSIVE FTP server to discover files of the configured type
        2. Filters by file type first (config['study']['file_type'])
        3. Applies study-specific filters to reduce the dataset to relevant files only
        
        IMPORTANT - File Type Selection:
        Uses config['study']['file_type'] to specify which file extension to collect
        (e.g., '.raw', '.mzml', '.d'). Only files of this type are discovered.
        
        IMPORTANT - File Filtering:
        The filtering step uses config['study']['file_filters'] to dramatically reduce
        the number of files from potentially thousands to only those matching your
        study's specific criteria. Files are kept if their filename contains ANY of
        the filter keywords (case-insensitive).
        
        Example configuration and filtering:
        - config['study']['file_type'] = '.raw'  # Only collect .raw files
        - config['study']['file_filters'] = ['pos', 'neg', 'hilic']
        - File 'sample_hilic_pos_01.raw' → KEPT (is .raw AND contains 'pos' and 'hilic')
        - File 'sample_rp_neutral_01.mzml' → EXCLUDED (wrong file type)
        - File 'sample_rp_neutral_01.raw' → EXCLUDED (right type but no matching keywords)
        - File 'blank_neg_02.raw' → KEPT (is .raw AND contains 'neg')
        
        Args:
            massive_id: MASSIVE dataset ID with version path (e.g., 'v07/MSV000094090').
                       Uses config['study']['massive_id'] if not provided.
            
        Returns:
            DataFrame containing FTP locations and file information ONLY for files
            matching the filter criteria. Original dataset may contain thousands of
            files, but only filtered subset is returned.
            
        Warning:
            Without proper file_filters configuration, this method could return
            thousands of files for download. Always verify your file_filters are
            configured correctly for your specific study needs.
            
        Example:
            >>> manager = NMDCStudyManager('config.json')
            >>> # Ensure config has: "file_filters": ["pos", "neg", "hilic"]
            >>> ftp_df = manager.get_massive_ftp_urls()
            >>> print(f"Found {len(ftp_df)} filtered files (original dataset may have had many more)")
        """
        if massive_id is None:
            massive_id = self.config['study']['massive_id']
            
        # Step 1: Crawl FTP
        try:
            log_file = self._crawl_massive_ftp(massive_id)
            # Step 2: Parse log and get filtered results
            filtered_df = self.parse_massive_ftp_log(log_file)
            
            # Step 3: Report filtering results with sample files
            file_type = self.config['study'].get('file_type', '.raw')
            file_filters = self.config['study'].get('file_filters', [])
            
            if len(filtered_df) > 0:
                print("\n📊 FILTERING RESULTS:")
                print(f"   🎯 File type: {file_type}")
                print(f"   🔍 Filters applied: {file_filters}")
                print(f"   ✓ Kept {len(filtered_df)} files after applying both file type and filter criteria")
                
                # Show 4 random sample files
                sample_size = min(4, len(filtered_df))
                sample_files = filtered_df['raw_data_file_short'].sample(n=sample_size, random_state=42)
                print("   📁 Sample of kept files:")
                for i, filename in enumerate(sample_files, 1):
                    print(f"      {i}. {filename}")
                
                if len(filtered_df) > 4:
                    print(f"      ... and {len(filtered_df) - 4} more files")
                print()
            else:
                print("\n⚠️  WARNING: No files matched the criteria!")
                print(f"   File type: {file_type}")
                print(f"   File filters: {file_filters}")
                print("   Check your file_type and file_filters configuration")
                print()
            
            return filtered_df
        except Exception as e:
            print(f"Error in MASSIVE FTP process: {e}")
            return pd.DataFrame(columns=['ftp_location', 'raw_data_file_short'])
    
    def download_from_massive(self, ftp_file: Optional[str] = None, 
                            download_dir: Optional[str] = None,
                            massive_id: Optional[str] = None) -> List[str]:
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
                         config['paths']['raw_data_directory'] if not provided.
            massive_id: MASSIVE dataset ID to query directly. Uses
                       config['study']['massive_id'] if not provided.
            
        Returns:
            List of local file paths for successfully downloaded files
            
        Raises:
            ValueError: If neither ftp_file nor massive_id is provided and
                       no massive_id exists in config
                       
        Note:
            Files are downloaded using urllib.request.urlretrieve for reliability.
            Existing files with matching names are skipped to avoid re-downloading.
        """
        if download_dir is None:
            download_dir = self.config['paths']['raw_data_directory']
        
        # Get FTP URLs either from file or by querying MASSIVE
        if massive_id:
            ftp_df = self.get_massive_ftp_urls(massive_id)
        elif ftp_file:
            ftp_path = self.study_path / ftp_file
            if ftp_path.suffix == '.csv':
                ftp_df = pd.read_csv(ftp_path)
            else:
                # Handle text file format
                with open(ftp_path, 'r') as f:
                    lines = f.readlines()
                ftp_df = self._parse_ftp_file(lines)
        else:
            # Try to use MASSIVE ID from config
            if 'massive_id' in self.config['study']:
                ftp_df = self.get_massive_ftp_urls()
            else:
                raise ValueError("Either ftp_file or massive_id must be provided")
        
        if len(ftp_df) == 0:
            print("No files to download")
            return []
        
        # Skip if this looks like a template file
        if 'example_file.raw' in str(ftp_df.iloc[0].get('raw_data_file_short', '')):
            print("FTP file appears to be a template. Please edit with real FTP URLs first.")
            return []
        
        os.makedirs(download_dir, exist_ok=True)
        downloaded_files = []
        
        print(f"Starting download of {len(ftp_df)} files...")
        for index, row in tqdm(ftp_df.iterrows(), total=len(ftp_df), desc="Downloading files"):
            ftp_location = row['ftp_location']
            file_name = row['raw_data_file_short']
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
        
        print(f"Downloaded {len([f for f in downloaded_files if os.path.exists(f)])} files successfully")
        return downloaded_files
    
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
            if line.strip() and not line.startswith('#'):
                # Parse line based on format
                parts = line.strip().split('\t')  # Adjust based on actual format
                if len(parts) >= 2:
                    data.append({
                        'ftp_location': parts[0],
                        'raw_data_file_short': parts[1]
                    })
        return pd.DataFrame(data)
    
    def upload_to_minio(self, local_directory: str, bucket_name: str, 
                       folder_name: str, file_pattern: str = "*") -> int:
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
            raise ValueError("MinIO client not initialized")
        
        local_path = Path(local_directory)
        if not local_path.exists():
            raise ValueError(f"Local directory {local_directory} does not exist")
        
        # Collect files to upload
        files_to_upload = list(local_path.rglob(file_pattern))
        files_to_upload = [f for f in files_to_upload if f.is_file()]
        
        uploaded_count = 0
        
        print(f"Uploading {len(files_to_upload)} files to {bucket_name}/{folder_name}")
        
        for file_path in tqdm(files_to_upload, desc="Uploading files"):
            # Create object name preserving directory structure
            relative_path = file_path.relative_to(local_path)
            object_name = f"{folder_name}/{relative_path}".replace("\\", "/")
            
            try:
                self.minio_client.fput_object(bucket_name, object_name, str(file_path))
                uploaded_count += 1
            except S3Error as e:
                print(f"Error uploading {file_path}: {e}")
        
        print(f"Successfully uploaded {uploaded_count} files")
        return uploaded_count
    
    def download_from_minio(self, bucket_name: str, folder_name: str, 
                          local_directory: str) -> int:
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
            raise ValueError("MinIO client not initialized")
        
        # Create local directory
        Path(local_directory).mkdir(parents=True, exist_ok=True)
        
        # List objects in folder
        objects = self.minio_client.list_objects(bucket_name, prefix=folder_name, recursive=True)
        
        all_objects = [obj for obj in objects if not obj.object_name.endswith('/')]
        downloaded_count = 0
        
        print(f"Found {len(all_objects)} files to download from {bucket_name}/{folder_name}")
        
        for obj in tqdm(all_objects, desc="Downloading files"):
            # Create local file path
            relative_path = obj.object_name[len(folder_name):].lstrip('/')
            local_file_path = os.path.join(local_directory, relative_path)
            
            # Create subdirectories if needed
            Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Check if file exists and has same size
            if os.path.exists(local_file_path):
                local_size = os.path.getsize(local_file_path)
                if local_size == obj.size:
                    continue  # Skip existing files
            
            try:
                self.minio_client.fget_object(bucket_name, obj.object_name, local_file_path)
                downloaded_count += 1
            except S3Error as e:
                print(f"Error downloading {obj.object_name}: {e}")
        
        print(f"Downloaded {downloaded_count} new files")
        return downloaded_count
    
    def generate_wdl_jsons(self, batch_size: int = 50, 
                          processed_files: Optional[List[str]] = None) -> int:
        """
        Generate WDL workflow JSON configuration files for batch processing.
        
        Creates JSON files for each processing configuration defined in the config,
        organizing raw data files into batches of the specified size. Files are
        filtered for each configuration based on the configuration name (e.g.,
        'hilic_pos' will only include files with 'hilic' and 'pos' in the filename).
        
        Args:
            batch_size: Maximum number of files per batch (default: 50)
            processed_files: List of file paths to exclude from processing
                           (useful for resuming interrupted workflows)
            
        Returns:
            Total number of JSON configuration files created across all
            processing configurations
            
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
            
        Example:
            >>> json_count = manager.generate_wdl_jsons(batch_size=25)
            >>> print(f"Created {json_count} WDL JSON files")
        """
        raw_data_dir = self.config['paths']['raw_data_directory']
        
        # Get the configured file type extension
        file_type = self.config['study'].get('file_type', '.raw')
        
        # Get list of files matching the configured file type
        file_pattern = f"*{file_type}"
        raw_files = list(Path(raw_data_dir).rglob(file_pattern))
        
        # Filter out processed files
        if processed_files:
            raw_files = [f for f in raw_files if str(f) not in processed_files]
        
        print(f"Found {len(raw_files)} {file_type} files in {raw_data_dir}")
        
        # Create batches for each configuration
        json_count = 0
        for config in self.config.get('configurations', []):
            config_name = config['name']
            config_dir = self.study_path / "wdl_jsons" / config_name
            config_dir.mkdir(parents=True, exist_ok=True)
            
            # Filter files for this specific configuration
            # Use the configuration's file_filter if specified, otherwise include all files
            config_filters = config.get('file_filter', [])
            
            filtered_files = []
            for file_path in raw_files:
                filename = file_path.name.lower()
                # If no file_filter specified, include all files
                if not config_filters:
                    filtered_files.append(file_path)
                else:
                    # Check if ALL configuration filters are present in the filename
                    if all(filter_term.lower() in filename for filter_term in config_filters):
                        filtered_files.append(file_path)
            
            filter_info = f"filters {config_filters}" if config_filters else "no filters (all files)"
            print(f"Configuration '{config_name}': {len(filtered_files)} files match {filter_info}")
            
            if len(filtered_files) == 0:
                print(f"  ⚠️  No files found for configuration '{config_name}' - skipping")
                continue
            
            # Show sample of filtered files for verification
            sample_size = min(3, len(filtered_files))
            print(f"  📁 Sample files for '{config_name}':")
            for i, sample_file in enumerate(filtered_files[:sample_size], 1):
                print(f"    {i}. {sample_file.name}")
            if len(filtered_files) > sample_size:
                print(f"    ... and {len(filtered_files) - sample_size} more")
            
            # Split files into batches
            batches = [filtered_files[i:i + batch_size] for i in range(0, len(filtered_files), batch_size)]
            
            for batch_num, batch_files in enumerate(batches, 1):
                json_obj = {
                    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.file_paths": [str(f) for f in batch_files],
                    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.output_directory": "output",
                    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.corems_toml_path": config['corems_toml'],
                    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.msp_file_path": config['msp_file'],
                    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.scan_translator_path": config['scan_translator'],
                    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.cores": config.get('cores', 1)
                }
                
                output_file = config_dir / f"run_metaMS_lcms_metabolomics_{config_name}_batch{batch_num}.json"
                
                with open(output_file, 'w') as f:
                    json.dump(json_obj, f, indent=4)
                
                json_count += 1
                print(f"  ✓ Created batch {batch_num} with {len(batch_files)} files")
        
        print(f"\n📋 SUMMARY: Created {json_count} WDL JSON files total")
        return json_count