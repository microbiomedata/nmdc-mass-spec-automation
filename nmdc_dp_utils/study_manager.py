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
import shutil
import subprocess
import sys
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
            config = json.load(f)
        
        # Initialize skip_triggers if not present
        if 'skip_triggers' not in config:
            config['skip_triggers'] = {
                'study_structure_created': False,
                'raw_data_downloaded': False,
                'biosample_attributes_fetched': False,
                'biosample_mapping_script_generated': False,
                'biosample_mapping_completed': False,
                'wdls_generated': False,
                'data_processed': False
            }
        
        # Store config path for later updates
        self.config_path = config_path
        return config
    
    def should_skip(self, trigger_name: str) -> bool:
        """
        Check if a workflow step should be skipped based on trigger.
        
        Args:
            trigger_name: Name of the skip trigger to check
            
        Returns:
            True if the step should be skipped, False otherwise
        """
        return self.config.get('skip_triggers', {}).get(trigger_name, False)
    
    def set_skip_trigger(self, trigger_name: str, value: bool, save: bool = True):
        """
        Set a skip trigger value and optionally save to config file.
        
        Args:
            trigger_name: Name of the skip trigger to set
            value: Boolean value to set
            save: Whether to save the updated config to file
        """
        if 'skip_triggers' not in self.config:
            self.config['skip_triggers'] = {}
        
        self.config['skip_triggers'][trigger_name] = value
        
        if save and hasattr(self, 'config_path'):
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=4)
            print(f"Updated skip trigger '{trigger_name}' to {value}")
    
    def reset_all_triggers(self, save: bool = True):
        """
        Reset all skip triggers to False, allowing all workflow steps to run again.
        
        This method sets all existing skip triggers to False, effectively resetting
        the workflow state to allow re-running all steps from the beginning.
        
        Args:
            save: Whether to save the config file after resetting triggers (default: True)
            
        Note:
            This is useful when you want to re-run the entire workflow or when 
            troubleshooting issues that require reprocessing from scratch.
            
        Example:
            >>> manager = NMDCStudyManager('config.json')
            >>> manager.reset_all_triggers()
            🔄 Reset 5 skip triggers to False:
               • raw_data_downloaded: False
               • biosample_mapping_completed: False
               • data_processed: False
               • biosample_attributes_fetched: False
               • raw_data_inspected: False
            All workflow steps will now run when executed
        """
        if 'skip_triggers' not in self.config:
            self.config['skip_triggers'] = {}
            print("No skip triggers found - nothing to reset")
            return
        
        # Get list of current triggers before resetting
        current_triggers = list(self.config['skip_triggers'].keys())
        
        if not current_triggers:
            print("No skip triggers found - nothing to reset")
            return
        
        # Reset all triggers to False
        for trigger_name in current_triggers:
            self.config['skip_triggers'][trigger_name] = False
        
        if save and hasattr(self, 'config_path'):
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=4)
        
        print(f"🔄 Reset {len(current_triggers)} skip triggers to False:")
        for trigger in current_triggers:
            print(f"   • {trigger}: False")
        print("All workflow steps will now run when executed")
    
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
        if self.should_skip('study_structure_created'):
            print("Skipping study structure creation (already created)")
            return
            
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
        self.set_skip_trigger('study_structure_created', True)
    
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
        if self.should_skip('raw_data_downloaded'):
            print("Skipping MASSIVE FTP URL discovery (raw data already downloaded)")
            # Return existing FTP data if available
            ftp_csv = self.study_path / f"{self.study_name}_massive_ftp_locs.csv"
            if ftp_csv.exists():
                return pd.read_csv(ftp_csv)
            else:
                return pd.DataFrame(columns=['ftp_location', 'raw_data_file_short'])
                
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
        if self.should_skip('raw_data_downloaded'):
            print("Skipping raw data download (already downloaded)")
            # Return list of existing files if directory exists
            if download_dir is None:
                download_dir = self.config['paths']['raw_data_directory']
            if os.path.exists(download_dir):
                file_type = self.config['study'].get('file_type', '.raw')
                existing_files = [os.path.join(download_dir, f) for f in os.listdir(download_dir) 
                                if f.endswith(file_type)]
                print(f"Found {len(existing_files)} existing {file_type} files")
                return existing_files
            return []
            
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
        
        # Write CSV of downloaded file names for biosample mapping
        if len(downloaded_files) > 0:
            downloaded_files_csv = self.study_path / "metadata" / "downloaded_files.csv"
            os.makedirs(downloaded_files_csv.parent, exist_ok=True)
            
            # Create DataFrame with downloaded file information
            file_data = []
            for file_path in downloaded_files:
                if os.path.exists(file_path):
                    file_data.append({
                        'file_path': file_path,
                        'file_name': os.path.basename(file_path),
                        'file_size_bytes': os.path.getsize(file_path) if os.path.exists(file_path) else 0
                    })
            
            if file_data:
                download_df = pd.DataFrame(file_data)
                download_df.to_csv(downloaded_files_csv, index=False)
                print(f"📄 Downloaded files list saved to: {downloaded_files_csv}")
            
            self.set_skip_trigger('raw_data_downloaded', True)
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
    
    def generate_wdl_jsons(self, batch_size: int = 50) -> int:
        """
        Generate WDL workflow JSON configuration files for batch processing.
        
        Creates JSON files for each processing configuration defined in the config,
        organizing raw data files into batches of the specified size. Files are
        filtered for each configuration based on the configuration name (e.g.,
        'hilic_pos' will only include files with 'hilic' and 'pos' in the filename).
        
        Args:
            batch_size: Maximum number of files per batch (default: 50)
            
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
        if self.should_skip('data_processed'):
            print("Skipping WDL JSON generation (already generated)")
            # Count existing JSON files
            wdl_jsons_path = self.study_path / "wdl_jsons"
            if wdl_jsons_path.exists():
                json_count = len(list(wdl_jsons_path.rglob("*.json")))
                print(f"Found {json_count} existing WDL JSON files")
                return json_count
            return 0

        # Always try to move data from the processing site to the processed site before regenerating jsons
        self._move_processed_files(self.config['paths']['wdl_workspace'])
        
        # Always empty the wdl_jsons directory first
        wdl_jsons_path = self.study_path / "wdl_jsons"
        if wdl_jsons_path.exists():
            shutil.rmtree(wdl_jsons_path)
        wdl_jsons_path.mkdir(parents=True, exist_ok=True)

        # Use mapped files list if available (only files that map to biosamples)
        mapped_files_csv = self.study_path / "metadata" / "mapped_raw_files.csv"
        
        if mapped_files_csv.exists():
            print("📋 Using mapped raw files list (only biosample-mapped files will be processed)")
            import pandas as pd
            mapped_df = pd.read_csv(mapped_files_csv)
            raw_files = [Path(file_path) for file_path in mapped_df['raw_file_path'] 
                        if Path(file_path).exists()]
            print(f"Loaded {len(raw_files)} mapped files from {mapped_files_csv}")
            
            # Show mapping statistics
            high_conf = len(mapped_df[mapped_df['match_confidence'] == 'high'])
            med_conf = len(mapped_df[mapped_df['match_confidence'] == 'medium'])
            print(f"  High confidence matches: {high_conf}")
            print(f"  Medium confidence matches: {med_conf}")
        else:
            print("⚠️  Mapped files list not found - using all raw files")
            print("   Run biosample mapping first to filter files by biosample mapping")
            
            raw_data_dir = self.config['paths']['raw_data_directory']
            
            # Get the configured file type extension
            file_type = self.config['study'].get('file_type', '.raw')
            
            # Get list of files matching the configured file type
            file_pattern = f"*{file_type}"
            raw_files = list(Path(raw_data_dir).rglob(file_pattern))
            print(f"Found {len(raw_files)} {file_type} files in {raw_data_dir}")
        
        # Filter out already-processed files by checking for corresponding .corems directories
        processed_data_dir = self.config['paths'].get('processed_data_directory')
        if processed_data_dir:
            processed_path = Path(processed_data_dir)
            if processed_path.exists():
                initial_count = len(raw_files)
                unprocessed_files = []
                
                print(f"🔍 Checking for already-processed files in: {processed_path}")
                
                for raw_file in raw_files:
                    # Get the base name without extension (e.g., sample1.raw -> sample1)
                    base_name = raw_file.stem
                    
                    # Check if corresponding .corems directory exists
                    corems_dir = processed_path / f"{base_name}.corems"
                    
                    if corems_dir.exists() and corems_dir.is_dir():
                        # Check if the .corems directory contains CSV files (indicates successful processing)
                        csv_files = list(corems_dir.glob('*.csv'))
                        if csv_files:
                            continue  # Skip this file - already processed
                    
                    # File is not processed or processing incomplete
                    unprocessed_files.append(raw_file)
                
                excluded_count = initial_count - len(unprocessed_files)
                raw_files = unprocessed_files
                
                if excluded_count > 0:
                    print(f"📊 Excluded {excluded_count} already-processed files")
                    print(f"   Remaining unprocessed files: {len(raw_files)}")
                else:
                    print(f"✅ No processed files found - all {len(raw_files)} files will be included")
            else:
                print(f"⚠️  Processed data directory not found: {processed_path}")
                print("   All files will be included for processing")
        else:
            print("⚠️  processed_data_directory not configured in config")
            print("   All files will be included for processing")
        
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
    
    def generate_wdl_runner_script(self, workflow_name: str = "metaMS_lcms_metabolomics", 
                                  script_name: Optional[str] = None) -> str:
        """
        Generate a shell script to run all WDL JSON files using miniwdl.
        
        Creates a bash script that discovers all JSON files in the study's wdl_jsons
        directory and runs them sequentially using miniwdl. The script includes
        progress reporting and error handling.
        
        Args:
            workflow_name: Name of the WDL workflow file (without .wdl extension)
            script_name: Name for the generated script file. Defaults to 
                        '{study_name}_wdl_runner.sh'
        
        Returns:
            Path to the generated shell script file
            
        Example:
            >>> script_path = manager.generate_wdl_runner_script()
            >>> print(f"Generated script: {script_path}")
            
        Note:
            The generated script expects to be run from a directory containing
            a 'wdl/' subdirectory with the workflow file. Use run_wdl_script()
            to execute from the appropriate location.
        """
        if self.should_skip('data_processed'):
            print("Skipping WDL runner script generation (data already processed)")
            script_path = self.study_path / "scripts" / f"{self.study_name}_wdl_runner.sh"
            if script_path.exists():
                print(f"Found existing script: {script_path}")
                return str(script_path)
            else:
                print("No existing script found.")
                return ""


        if script_name is None:
            script_name = f"{self.study_name}_wdl_runner.sh"
            
        script_path = self.study_path / "scripts" / script_name
        
        # Get absolute path to the wdl_jsons directory
        wdl_jsons_dir = self.study_path / "wdl_jsons"
        
        print("🔍 Validating WDL JSON files...")
        
        # Check if wdl_jsons directory exists
        if not wdl_jsons_dir.exists():
            print(f"❌ WDL JSON directory not found: {wdl_jsons_dir}")
            print("Run generate_wdl_jsons() first to create JSON files")
            raise FileNotFoundError(f"WDL JSON directory not found: {wdl_jsons_dir}")
        
        # Find all JSON files
        json_files = list(wdl_jsons_dir.rglob("*.json"))
        if not json_files:
            print(f"❌ No JSON files found in: {wdl_jsons_dir}")
            print("Run generate_wdl_jsons() first to create JSON files")
            raise FileNotFoundError(f"No JSON files found in: {wdl_jsons_dir}")
        
        print(f"✅ Found {len(json_files)} JSON files")
        
        # Validate each JSON file and check referenced files
        missing_files = []
        corrupted_jsons = []
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    json_data = json.load(f)
                
                # Check for file paths in the JSON
                file_paths_key = "lcmsMetabolomics.runMetaMSLCMSMetabolomics.file_paths"
                if file_paths_key in json_data:
                    file_paths = json_data[file_paths_key]
                    for file_path in file_paths:
                        if not Path(file_path).exists():
                            missing_files.append(file_path)
                
                # Check for other referenced files
                config_files = [
                    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.corems_toml_path",
                    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.msp_file_path", 
                    "lcmsMetabolomics.runMetaMSLCMSMetabolomics.scan_translator_path"
                ]
                
                for config_key in config_files:
                    if config_key in json_data:
                        config_path = json_data[config_key]
                        if not Path(config_path).exists():
                            missing_files.append(config_path)
                            
            except json.JSONDecodeError as e:
                corrupted_jsons.append(f"{json_file}: {e}")
            except Exception as e:
                corrupted_jsons.append(f"{json_file}: {e}")
        
        # Report any issues found
        if corrupted_jsons:
            print("❌ Corrupted JSON files found:")
            for error in corrupted_jsons:
                print(f"  • {error}")
            raise ValueError("Corrupted JSON files detected")
        
        if missing_files:
            print("❌ Missing referenced files:")
            unique_missing = list(set(missing_files))
            for missing_file in unique_missing[:10]:  # Show first 10
                print(f"  • {missing_file}")
            if len(unique_missing) > 10:
                print(f"  ... and {len(unique_missing) - 10} more files")
            print("\nPlease ensure all raw data files and configuration files exist")
            raise FileNotFoundError(f"Missing {len(unique_missing)} referenced files")
        
        print("✅ All JSON files and referenced files validated")
        
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
WDL_FILE="wdl/{workflow_name}.wdl"
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
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make the script executable
        os.chmod(script_path, 0o755)
        
        print(f"Generated WDL runner script: {script_path}")
        print("Made script executable (chmod +x)")
        print(f"Script expects to find WDL file at: wdl/{workflow_name}.wdl")
        
        return str(script_path)
    
    def run_wdl_script(self, script_path: str, working_directory: Optional[str] = None) -> int:
        """
        Execute the WDL runner script with comprehensive error handling.
        
        Handles all validation, error checking, workspace detection, Docker verification,
        venv activation, and execution of WDL workflows. Sets the data_processed trigger 
        on successful completion.
        
        Args:
            script_path: Path to the shell script to execute
            working_directory: Directory to run the script from. If not provided,
                             uses 'wdl_workspace' from config paths.
        
        Returns:
            Exit code from the script execution (0 for success, non-zero for failure)
            
        Note:
            This method handles all error cases internally and provides comprehensive
            feedback. It will automatically set the data_processed trigger on success.
        """
        import subprocess
        
        # Check if already processed
        if self.should_skip('data_processed'):
            print("Skipping WDL workflow execution (data already processed)")
            return 0
        
        # Determine working directory
        if working_directory is None:
            if 'wdl_workspace' not in self.config.get('paths', {}):
                print("❌ WDL workspace not configured")
                print("To run workflows, add 'wdl_workspace' to config paths:")
                print('  "paths": { "wdl_workspace": "/path/to/wdl/workspace" }')
                return 1
            working_directory = self.config['paths']['wdl_workspace']
        
        script_path = Path(script_path)
        working_dir = Path(working_directory)
        
        # Validate script exists
        if not script_path.exists():
            print(f"❌ Script not found: {script_path}")
            return 1
            
        # Validate working directory exists
        if not working_dir.exists():
            print(f"❌ Working directory not found: {working_dir}")
            return 1
        
        # Check if WDL directory exists in working directory
        wdl_dir = working_dir / "wdl"
        if not wdl_dir.exists():
            print(f"⚠️  WARNING: WDL directory not found at {wdl_dir}")
            print("The script may fail if it cannot find the workflow files")
        
        # Check if Docker is running
        print("🐳 Checking Docker availability...")
        try:
            docker_check = subprocess.run(['docker', 'info'], 
                                        capture_output=True, text=True, timeout=10)
            if docker_check.returncode != 0:
                print("❌ Docker is not running or not available")
                print("Please start Docker Desktop and try again")
                print("You can check Docker status with: docker info")
                return 1
            print("✅ Docker is running")
        except subprocess.TimeoutExpired:
            print("❌ Docker check timed out - Docker may not be running")
            print("Please start Docker Desktop and try again")
            return 1
        except FileNotFoundError:
            print("❌ Docker command not found")
            print("Please install Docker Desktop and try again")
            return 1
        except Exception as e:
            print(f"❌ Error checking Docker: {e}")
            return 1
        
        # Check if venv exists and can be activated
        venv_dir = working_dir / "venv"
        if not venv_dir.exists():
            print(f"❌ Virtual environment not found at: {venv_dir}")
            print("Please create a virtual environment:")
            print(f"  cd {working_dir}")
            print("  python -m venv venv")
            print("  source venv/bin/activate")
            print("  pip install -r requirements.txt")
            return 1
        
        venv_python = venv_dir / "bin" / "python"
        if not venv_python.exists():
            print(f"❌ Virtual environment Python not found at: {venv_python}")
            print("Virtual environment may be corrupted. Please recreate it.")
            return 1
        
        print(f"✅ Virtual environment found at: {venv_dir}")
        
        print(f"🚀 Running WDL workflows from: {working_dir}")
        print("This will take a long time for large datasets...")
        
        # Store current directory
        original_dir = os.getcwd()
        
        try:
            print(f"📁 Changing to working directory: {working_dir}")
            os.chdir(working_dir)
            
            print(f"⚡ Executing script with venv activation: {script_path}")
            print("=" * 50)
            
            # Create a command that activates venv and runs the script
            activate_and_run = f"source venv/bin/activate && {script_path}"
            
            # Run the script with bash to handle source command
            result = subprocess.run(['bash', '-c', activate_and_run], 
                                  capture_output=False,  # Let output go to console
                                  text=True)
            
            print("=" * 50)
            print(f"📊 Script execution completed with exit code: {result.returncode}")
            
            if result.returncode == 0:
                print("🎉 All WDL workflows completed successfully!")
                
                # Move processed output files to the designated location
                self._move_processed_files(working_dir)
                
                self.set_skip_trigger('data_processed', True)
            else:
                print(f"⚠️  Some workflows failed (exit code: {result.returncode})")
                print("Check the logs above for details.")
                print("You can run the script manually later:")
                print(f"  cd {working_dir}")
                print("  source venv/bin/activate")
                print(f"  {script_path}")
            
            return result.returncode
            
        except Exception as e:
            print(f"❌ Error executing script: {e}")
            print("You can run the script manually later:")
            print(f"  cd {working_dir}")
            print("  source venv/bin/activate")
            print(f"  {script_path}")
            return 1
            
        finally:
            # Always return to original directory
            os.chdir(original_dir)
            print(f"🔙 Returned to original directory: {original_dir}")
    
    def _move_processed_files(self, working_dir: str) -> None:
        """
        Move processed output files from working directory to designated processed data location.
        
        Searches for directories ending with .corems in the working directory and moves them
        to the processed data directory specified in the configuration. Only moves directories
        that contain CSV files (indicating successful processing).
        
        Args:
            working_dir: Directory where WDL workflows were executed and output files created
            
        Note:
            Uses config['paths']['processed_data_directory'] as the destination.
            Creates the destination directory if it doesn't exist.
        """
        import shutil
        
        working_path = Path(working_dir)
        processed_data_dir = self.config['paths'].get('processed_data_directory')
        
        if not processed_data_dir:
            print("⚠️  processed_data_directory not configured - skipping file move")
            return
            
        processed_path = Path(processed_data_dir)
        
        # Create processed data directory if it doesn't exist
        if not processed_path.exists():
            processed_path.mkdir(parents=True, exist_ok=True)
            print(f"📁 Created processed data directory: {processed_path}")
        
        print(f"🔍 Searching for processed files in: {working_path}")
        print(f"📁 Moving processed files to: {processed_path}")
        
        moved_count = 0
        
        # Search for .corems directories recursively
        for dirpath in working_path.rglob('*'):
            if dirpath.is_dir() and dirpath.name.endswith('.corems'):
                # Check that there is a .csv within the directory (indicates successful processing)
                csv_files = list(dirpath.glob('*.csv'))
                if not csv_files:
                    print(f"  ⚠️  No .csv files found in {dirpath.name}, skipping.")
                    continue
                
                # Move the entire .corems directory to processed location
                destination = processed_path / dirpath.name
                
                # Handle case where destination already exists
                if destination.exists():
                    print(f"  ⚠️  Destination already exists: {destination.name}, skipping.")
                    continue
                
                try:
                    shutil.move(str(dirpath), str(destination))
                    moved_count += 1
                    print(f"  ✅ Moved {dirpath.name} -> {destination}")
                except Exception as e:
                    print(f"  ❌ Failed to move {dirpath.name}: {e}")
        
        print(f"📋 Processed file move summary:")
        print(f"   Files moved: {moved_count}")
        print(f"   Destination: {processed_path}")
        
        if moved_count > 0:
            # Report total processed files in destination
            total_corems = len(list(processed_path.glob('*.corems')))
            print(f"   Total processed files in destination: {total_corems}")
        else:
            print("   No processed files were moved")
    
    def get_biosample_attributes(self, study_id: Optional[str] = None) -> str:
        """
        Fetch biosample attributes from NMDC API and save to CSV file.
        
        Uses the nmdc_api_utilities package to query biosamples associated with 
        the study ID and saves the attributes to a CSV file in the study's 
        metadata directory. Includes skip trigger to avoid re-downloading.
        
        Args:
            study_id: NMDC study ID (e.g., 'nmdc:sty-11-dwsv7q78'). 
                     Uses config['study']['id'] if not provided.
        
        Returns:
            Path to the generated biosample attributes CSV file
            
        Note:
            This method sets a skip trigger 'biosample_attributes_fetched' to
            avoid re-downloading on subsequent runs. The CSV file is saved as
            'biosample_attributes.csv' in the study's metadata directory.
        """
        from nmdc_api_utilities.biosample_search import BiosampleSearch
        
        # Check skip trigger
        if self.should_skip('biosample_attributes_fetched'):
            print("Skipping biosample attributes fetch (already downloaded)")
            biosample_csv = self.study_path / "metadata" / "biosample_attributes.csv"
            if biosample_csv.exists():
                return str(biosample_csv)
        
        if study_id is None:
            study_id = self.config['study']['id']
        
        print(f"🔍 Fetching biosample attributes for study: {study_id}")
        
        try:
            # Use nmdc_api_utilities to fetch biosamples
            biosample_search = BiosampleSearch()
            biosamples = biosample_search.get_record_by_filter(
                filter=f'{{"associated_studies":"{study_id}"}}',
                max_page_size=1000,
                fields="id,name,samp_name,description,gold_biosample_identifiers,insdc_biosample_identifiers,submitter_id",
                all_pages=True,
            )
            
            if not biosamples:
                print(f"❌ No biosamples found for study {study_id}")
                print("Please verify the study ID and check if biosamples are available in NMDC")
                raise ValueError(f"No biosamples found for study {study_id}")
            
            print(f"✅ Found {len(biosamples)} biosamples")
            
            # Convert to DataFrame
            biosample_df = pd.DataFrame(biosamples)
            
            # Create metadata directory if it doesn't exist
            metadata_dir = self.study_path / "metadata"
            metadata_dir.mkdir(parents=True, exist_ok=True)
            
            # Save to CSV
            biosample_csv = metadata_dir / "biosample_attributes.csv"
            biosample_df.to_csv(biosample_csv, index=False)
            
            print(f"💾 Saved biosample attributes to: {biosample_csv}")
            print(f"📊 Columns available: {list(biosample_df.columns)}")
            
            # Set skip trigger
            self.set_skip_trigger('biosample_attributes_fetched', True)
            
            return str(biosample_csv)
            
        except Exception as e:
            print(f"❌ Error fetching biosample data: {e}")
            print("Please check your internet connection and verify the study ID")
            print("Make sure nmdc_api_utilities package is installed: pip install nmdc-api-utilities")
            raise
    
    def generate_biosample_mapping_script(self, script_name: Optional[str] = None, 
                                         template_path: Optional[str] = None) -> str:
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
            Path to the generated TEMPLATE script
            
        Note:
            The generated script is labeled as _TEMPLATE to prevent accidental use
            without customization. Users should copy to a new filename and modify
            the parsing logic for their study's specific file naming patterns.
        """
        if self.should_skip('biosample_mapping_script_generated'):
            print("Skipping biosample mapping script generation (already generated)")
            script_path = self.study_path / "scripts" / (script_name or "map_raw_files_to_biosamples_TEMPLATE.py")
            if script_path.exists():
                return str(script_path)
        
        if script_name is None:
            script_name = "map_raw_files_to_biosamples_TEMPLATE.py"
            
        if template_path is None:
            # Use default template relative to this module
            template_path = Path(__file__).parent / "templates" / "biosample_mapping_script_template.py"
        else:
            template_path = Path(template_path)
            
        script_path = self.study_path / "scripts" / script_name
        
        print(f"📝 Generating biosample mapping TEMPLATE script: {script_path}")
        print(f"📄 Using template: {template_path}")
        
        # Check if template exists
        if not template_path.exists():
            raise FileNotFoundError(f"Template file not found: {template_path}")
        
        # Read the template
        with open(template_path, 'r') as f:
            template_content = f.read()
        
        # Format the template with study-specific values
        script_content = template_content.format(
            study_name=self.study_name,
            study_description=self.config['study']['description'],
            script_name=script_name
        )
        
        # Write the script file
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make the script executable
        os.chmod(script_path, 0o755)
        
        print(f"📝 Generated biosample mapping TEMPLATE script: {script_path}")
        print("Made script executable (chmod +x)")
        print("🔥 IMPORTANT: This is a TEMPLATE file - customize it for your study!")
        print("   1. Copy to a new filename (remove _TEMPLATE)")
        print("   2. Modify the parsing and matching logic for your specific file naming patterns")
        print("   3. Test with a small subset of files before running on the full dataset")
        
        self.set_skip_trigger('biosample_mapping_script_generated', True)
        return str(script_path)
    
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
        
        if self.should_skip('biosample_mapping_completed'):
            print("Skipping biosample mapping (already completed)")
            return True
        
        if script_path is None:
            # Check for both template and non-template versions
            template_script = self.study_path / "scripts" / "map_raw_files_to_biosamples_TEMPLATE.py"
            regular_script = self.study_path / "scripts" / "map_raw_files_to_biosamples.py"
            
            if regular_script.exists():
                script_path = regular_script
            elif template_script.exists():
                print("❌ Found only TEMPLATE script - you must customize it first!")
                print(f"Template script: {template_script}")
                print("📝 To use it:")
                print("   1. Copy the template to a new filename (remove _TEMPLATE)")
                print("   2. Customize the parsing logic for your study")
                print("   3. Run this function again with the customized script path")
                return False
            else:
                print(f"❌ No mapping script found. Run generate_biosample_mapping_script() first")
                return False
        else:
            script_path = Path(script_path)
        
        # Check if user is trying to run the template directly
        if '_TEMPLATE' in script_path.name:
            print("❌ Cannot run TEMPLATE script directly!")
            print(f"Template script: {script_path}")
            print("📝 You must customize the template first:")
            print("   1. Copy the template to a new filename (remove _TEMPLATE)")
            print("   2. Customize the parsing logic for your study")
            print("   3. Run this function again with the customized script path")
            return False
        
        if not script_path.exists():
            print(f"❌ Mapping script not found: {script_path}")
            print("Make sure you've customized and saved the script from the template")
            return False
        
        print(f"🔗 Running biosample mapping script: {script_path}")
        
        # Store current directory
        original_dir = os.getcwd()
        
        try:
            # Run the mapping script
            result = subprocess.run([sys.executable, str(script_path)], 
                                  capture_output=False,  # Let output go to console
                                  text=True,
                                  cwd=self.base_path)  # Run from base directory
            
            if result.returncode == 0:
                print("✅ Biosample mapping completed successfully!")
                
                # Generate filtered file list for WDL processing
                self._generate_mapped_files_list()
                
                self.set_skip_trigger('biosample_mapping_completed', True)
                return True
            else:
                print(f"⚠️  Biosample mapping script exited with code: {result.returncode}")
                print("This may indicate unmatched files or multiple matches that need review")
                print("Check the mapping file and customize the script as needed")
                return False
                
        except Exception as e:
            print(f"❌ Error running mapping script: {e}")
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
        mapping_file = self.study_path / f"{self.study_name}_raw_file_biosample_mapping.csv"
        if not mapping_file.exists():
            print(f"⚠️  Mapping file not found: {mapping_file}")
            return
            
        try:
            mapping_df = pd.read_csv(mapping_file)
            
            # Filter for only high and medium confidence matches
            mapped_df = mapping_df[mapping_df['match_confidence'].isin(['high', 'medium'])].copy()
            
            if len(mapped_df) == 0:
                print("⚠️  No high or medium confidence matches found - no files will be processed")
                return
            
            # Get the full file paths from the downloaded files list
            downloaded_files_csv = self.study_path / "metadata" / "downloaded_files.csv"
            if downloaded_files_csv.exists():
                downloaded_df = pd.read_csv(downloaded_files_csv)
                
                # Match by filename to get full paths
                mapped_df = mapped_df.merge(
                    downloaded_df[['file_name', 'file_path']], 
                    left_on='raw_file_name', 
                    right_on='file_name', 
                    how='left'
                )
                
                # Select and rename columns for output
                output_df = mapped_df[['file_path', 'biosample_id', 'biosample_name', 'match_confidence']].copy()
                output_df = output_df.rename(columns={'file_path': 'raw_file_path'})
            else:
                # Fallback if downloaded_files.csv doesn't exist
                print("⚠️  downloaded_files.csv not found - using relative paths")
                raw_data_dir = self.config['paths']['raw_data_directory']
                mapped_df['raw_file_path'] = mapped_df['raw_file_name'].apply(
                    lambda x: str(Path(raw_data_dir) / x)
                )
                output_df = mapped_df[['raw_file_path', 'biosample_id', 'biosample_name', 'match_confidence']].copy()
            
            # Save the filtered file list
            output_file = self.study_path / "metadata" / "mapped_raw_files.csv"
            output_df.to_csv(output_file, index=False)
            
            # Report statistics
            total_files = len(mapping_df)
            high_conf = len(mapped_df[mapped_df['match_confidence'] == 'high'])
            med_conf = len(mapped_df[mapped_df['match_confidence'] == 'medium'])
            
            print(f"📋 Generated filtered file list: {output_file}")
            print(f"   Total mapped files: {len(output_df)} of {total_files} ({len(output_df)/total_files*100:.1f}%)")
            print(f"   High confidence: {high_conf}")
            print(f"   Medium confidence: {med_conf}")
            print(f"   Files excluded: {total_files - len(output_df)} (control samples + unmapped)")
            
        except Exception as e:
            print(f"❌ Error generating mapped files list: {e}")
            import traceback
            traceback.print_exc()

    def raw_data_inspector(self, file_paths=None, cores=1, limit=None, max_retries=10, retry_delay=10.0):
        """
        Run raw data inspection on raw files to extract metadata using CoreMS in Docker.
        
        This method runs a specialized script to extract instrument metadata, scan parameters, 
        and data range information from raw MS files using a Docker container.
        
        IMPORTANT: .raw files require single-core processing to prevent crashes due to file
        locking issues with the Thermo RawFileReader in Docker containers. This method 
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
            str: Path to the output CSV file with inspection results
            
        Configuration Required:
            "docker": {
                "raw_data_inspector_image": "microbiomedata/metams:3.3.3"
            }
        """
        # Check skip trigger first
        if self.should_skip('raw_data_inspected'):
            print("⏭️  Skipping raw data inspection (skip trigger set)")
            # Try to return existing inspection results
            output_dir = self.study_path / "raw_file_info"
            existing_file = output_dir / "raw_file_inspection_results.csv"
            if existing_file.exists():
                print(f"📊 Found existing inspection results: {existing_file}")
                return str(existing_file)
            return None
        
        print("🔍 Starting raw data inspection...")
        
        # Check Docker configuration first
        docker_image = self.config.get('docker', {}).get('raw_data_inspector_image')
        if not docker_image:
            print("❌ Docker image not configured.")
            print("Please add 'docker.raw_data_inspector_image' to your config:")
            print('  "docker": {')
            print('    "raw_data_inspector_image": "microbiomedata/metams:3.3.3"')
            print('  }')
            return None
        
        try:
            # Get file paths to inspect
            if file_paths is None:
                # Use mapped raw files if available
                mapped_files_path = self.study_path / "metadata" / "mapped_raw_files.csv"
                if mapped_files_path.exists():
                    mapped_df = pd.read_csv(mapped_files_path)
                    file_paths = mapped_df['raw_file_path'].tolist()
                    print(f"📋 Using {len(file_paths)} mapped raw files for inspection")
                else:
                    # Fallback to all files in raw_data_directory
                    raw_data_dir = Path(self.config['paths']['raw_data_directory'])
                    file_paths = []
                    for ext in ['*.mzML', '*.raw', '*.mzml']:  # Include lowercase variants
                        file_paths.extend([str(f) for f in raw_data_dir.rglob(ext)])
                    print(f"📋 Using {len(file_paths)} raw files from data directory for inspection")
            
            if not file_paths:
                print("⚠️  No raw files found to inspect")
                return None
            
            # Check for .raw files and force single core processing to prevent crashes
            has_raw_files = any(str(fp).lower().endswith('.raw') for fp in file_paths)
            original_cores = cores
            if has_raw_files and cores > 1:
                cores = 1
                print(f"⚠️  Detected .raw files - forcing single core processing (requested: {original_cores} → using: 1)")
                print("   Reason: Multi-core processing of .raw files in Docker causes crashes")
                print("   due to file locking issues with Thermo RawFileReader library")
            elif has_raw_files:
                print("✅ .raw files detected - single core processing already configured")
            
            # Set up output directory
            output_dir = self.study_path / "raw_file_info"
            output_dir.mkdir(parents=True, exist_ok=True)
            
            result = self._run_raw_data_inspector_docker(file_paths, output_dir, cores, limit, max_retries, retry_delay, docker_image)
            
            # Set the skip trigger on successful completion
            if result is not None:
                print("✅ Raw data inspection completed successfully!")
                self.set_skip_trigger('raw_data_inspected', True)
                return result
            else:
                print("❌ Raw data inspection failed")
                return None
                
        except Exception as e:
            print(f"❌ Error during raw data inspection: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _run_raw_data_inspector_docker(self, file_paths, output_dir, cores, limit, max_retries, retry_delay, docker_image):
        """Run raw data inspector using Docker container."""
        print(f"🐳 Using Docker image: {docker_image}")
        
        # Check if Docker is available
        try:
            docker_check = subprocess.run(['docker', '--version'], 
                                        capture_output=True, text=True, timeout=10)
            if docker_check.returncode != 0:
                raise RuntimeError("Docker is not available")
            print(f"✅ Docker available: {docker_check.stdout.strip()}")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            raise RuntimeError("Docker is not installed or not available")
        
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
            raw_data_dir = Path(self.config['paths']['raw_data_directory']).resolve()
            mount_points.add(str(raw_data_dir))
        
        # Always mount the output directory and script directory
        mount_points.add(str(output_dir.resolve()))
        mount_points.add(str(script_path.parent.resolve()))
        
        # Build Docker volume arguments
        volume_args = []
        container_file_paths = []
        
        for mount_point in mount_points:
            container_path = f"/mnt{mount_point}"
            volume_args.extend(["-v", f"{mount_point}:{container_path}"])
            
        # Convert file paths to container paths
        raw_data_dir = Path(self.config['paths']['raw_data_directory']).resolve()
        for file_path in file_paths:
            file_path_obj = Path(file_path).resolve()
            # Replace the raw_data_dir with the container mount point
            container_file_path = str(file_path_obj).replace(str(raw_data_dir), f"/mnt{raw_data_dir}")
            container_file_paths.append(container_file_path)
        
        # Convert output directory to container path
        container_output_dir = f"/mnt{output_dir.resolve()}"
        
        # Convert script path to container path
        container_script_path = f"/mnt{script_path.resolve()}"
        
        # Prepare command arguments
        cmd_args = [
            "--files"] + container_file_paths + [
            "--output-dir", container_output_dir,
            "--cores", str(cores),
            "--max-retries", str(max_retries),
            "--retry-delay", str(retry_delay)
        ]
        
        if limit is not None:
            cmd_args.extend(["--limit", str(limit)])
        
        print(f"📁 Output directory: {output_dir}")
        print(f"🔧 Processing {len(file_paths)} files with {cores} cores")
        print(f"📦 Volume mounts: {len(mount_points)} directories")
        
        # Show mount point details for debugging
        print(f"🗂️  Mount points:")
        for i, mount_point in enumerate(mount_points, 1):
            print(f"   {i}. {mount_point} → /mnt{mount_point}")
        
        # Build Docker command
        docker_cmd = [
            "docker", "run", "--rm",
            "--user", f"{os.getuid()}:{os.getgid()}",  # Run as current user to avoid permission issues
        ] + volume_args + [
            docker_image,
            "python", container_script_path
        ] + cmd_args
        
        print("⚡ Running Docker command...")
        print(f"   Image: {docker_image}")
        print(f"   Script: {container_script_path}")
        print(f"   Files: {len(container_file_paths)} files")
        print(f"   Cores: {cores}")
        print(f"   Max retries: {max_retries}")
        print(f"   Retry delay: {retry_delay}s")
        if limit:
            print(f"   Limit: {limit} files")
        
        print(f"\n🐳 Docker command preview:")
        cmd_preview = " ".join(docker_cmd[:8]) + " ... " + " ".join(docker_cmd[-3:])
        print(f"   {cmd_preview}")
        
        print(f"\n🔄 Starting raw data inspection (this may take several minutes)...")
        print(f"💡 Processing files in batches - watch for progress updates below:")
        print("=" * 70)
        
        # Run the Docker command with real-time output
        result = subprocess.run(
            docker_cmd,
            cwd=str(self.study_path),
            capture_output=False,  # Let output go directly to console for real-time feedback
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        print("=" * 70)
        print(f"📊 Docker execution completed with exit code: {result.returncode}")
        
        # Since we used capture_output=False, we need to check the output file directly
        if result.returncode == 0:
            # Look for the output file
            default_output = output_dir / "raw_file_inspection_results.csv"
            if default_output.exists():
                return self._process_inspection_results_from_file(default_output)
            else:
                print(f"⚠️  Expected output file not found: {default_output}")
                return None
        else:
            print(f"❌ Docker execution failed with exit code: {result.returncode}")
            return None

    def _process_inspection_results_from_file(self, output_file):
        """Process inspection results from output CSV file."""
        try:
            import pandas as pd
            print(f"📋 Reading inspection results from: {output_file}")
            
            # Read the CSV file
            df = pd.read_csv(output_file)
            print(f"✅ Successfully loaded {len(df)} inspection results")
            
            # Show a preview of the results
            if len(df) > 0:
                print("📊 Results preview:")
                print(f"   Columns: {list(df.columns)}")
                print(f"   Sample records: {min(3, len(df))}")
                for i in range(min(3, len(df))):
                    filename = df.iloc[i].get('filename', 'Unknown')
                    print(f"     {i+1}. {filename}")
            
            return df
        except Exception as e:
            print(f"❌ Failed to read inspection results: {e}")
            return None
    

    
    def _process_inspection_results(self, result, output_dir):
        """Process the results from raw data inspection (common for both Docker and venv methods)."""
        if result.returncode == 0:
            print("✅ Raw data inspection completed successfully!")
            
            # Parse output to find the result file path
            lines = result.stdout.strip().split('\n')
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
                print(f"📊 Results saved to: {output_file_path}")
                
                # Show summary statistics
                try:
                    results_df = pd.read_csv(output_file_path)
                    print("📈 Inspection summary:")
                    print(f"   Files processed: {len(results_df)}")
                    
                    # Count successful vs failed
                    failed_count = len(results_df[results_df['error'].notna()])
                    success_count = len(results_df) - failed_count
                    print(f"   Successful: {success_count}")
                    print(f"   Failed: {failed_count}")
                    
                    if success_count > 0:
                        # Show instrument summary if available
                        if 'instrument_model' in results_df.columns:
                            instruments = results_df['instrument_model'].value_counts()
                            print(f"   Instrument models: {dict(instruments)}")
                
                except Exception as e:
                    print(f"⚠️  Could not read results summary: {e}")
                
                # Set skip trigger on successful completion
                self.set_skip_trigger('raw_data_inspected', True)
                
                return output_file_path
            else:
                print("⚠️  Could not find output file")
                print("Standard output:")
                print(result.stdout)
                return None
        else:
            print(f"❌ Raw data inspection failed with return code: {result.returncode}")
            print("Standard error:")
            print(result.stderr)
            print("Standard output:")
            print(result.stdout)
            return None
    
    def get_raw_inspection_results_path(self) -> Optional[str]:
        """
        Get the path to the raw data inspection results file.
        
        Returns:
            Path to the raw inspection results CSV file if it exists, None otherwise
        """
        results_file = self.study_path / "raw_file_info" / "raw_file_inspection_results.csv"
        if results_file.exists():
            return str(results_file)
        return None
    
    def generate_metadata_mapping_files(self) -> bool:
        """
        Generate metadata mapping files for NMDC submission.
        
        Creates workflow metadata CSV files separated by configuration that include:
        - Raw data file paths and processed data directories
        - Instrument information and analysis timestamps
        - Configuration-specific metadata for NMDC submission
        
        Returns:
            True if successful, False otherwise
        """
        if self.should_skip('metadata_mapping_generated'):
            print("⏭️  Skipping metadata mapping generation (skip trigger set)")
            return True
        
        print("📋 Generating metadata mapping files...")
        
        try:
            # Check prerequisites
            biosample_mapping_file = self.study_path / f"{self.study_name}_raw_file_biosample_mapping.csv"
            if not biosample_mapping_file.exists():
                print(f"❌ Biosample mapping file not found: {biosample_mapping_file}")
                return False
            
            raw_inspection_results = self.get_raw_inspection_results_path()
            if not raw_inspection_results:
                print("❌ Raw data inspection results not found. Run raw_data_inspector first.")
                return False
            
            # Load the mapped files
            mapped_df = pd.read_csv(biosample_mapping_file)
            
            # Filter for only high and medium confidence matches
            mapped_df = mapped_df[mapped_df['match_confidence'].isin(['high', 'medium'])].copy()
            
            if len(mapped_df) == 0:
                print("❌ No high or medium confidence biosample matches found")
                return False
            
            # Extract raw_data_file_short for merging
            mapped_df['raw_data_file_short'] = mapped_df['raw_file_name']
            
            # Add raw data file paths
            raw_data_dir = self.config['paths']['raw_data_directory']
            if not raw_data_dir.endswith('/'):
                raw_data_dir += '/'
            mapped_df['raw_data_file'] = raw_data_dir + mapped_df['raw_data_file_short']
            
            # Add processed data directories
            processed_data_dir = self.config['paths']['processed_data_directory']
            if not processed_data_dir.endswith('/'):
                processed_data_dir += '/'
            mapped_df['processed_data_directory'] = (
                processed_data_dir +
                mapped_df['raw_data_file_short'].str.replace(r'(?i)\.(raw|mzml)$', '', regex=True) +
                '.corems'
            )
            
            # Add instrument times from raw inspection results
            file_info_df = pd.read_csv(raw_inspection_results)
            
            # Filter out files with errors or missing critical metadata
            initial_count = len(file_info_df)
            
            # Remove files with errors
            if 'error' in file_info_df.columns:
                error_mask = file_info_df['error'].notna()
                if error_mask.any():
                    error_files = file_info_df[error_mask]['file_name'].tolist()
                    print(f"⚠️  Excluding {len(error_files)} files with processing errors:")
                    for f in error_files[:5]:  # Show first 5
                        print(f"   - {f}")
                    if len(error_files) > 5:
                        print(f"   ... and {len(error_files) - 5} more")
                    file_info_df = file_info_df[~error_mask]
            
            # Remove files with missing write_time (critical for metadata)
            null_time_mask = file_info_df['write_time'].isna()
            if null_time_mask.any():
                null_time_files = file_info_df[null_time_mask]['file_name'].tolist()
                print(f"⚠️  Excluding {len(null_time_files)} files with missing write_time:")
                for f in null_time_files:
                    print(f"   - {f}")
                file_info_df = file_info_df[~null_time_mask]
            
            final_count = len(file_info_df)
            if final_count != initial_count:
                print(f"📊 Raw inspection results: {initial_count} → {final_count} files (excluded {initial_count - final_count} with errors)")
            
            if final_count == 0:
                print("❌ No valid files remaining after filtering - check raw data inspection results")
                return False
            
            # Process remaining valid files
            file_info_df['instrument_instance_specifier'] = file_info_df['instrument_serial_number'].astype(str)
            file_info_df['instrument_analysis_end_date'] = pd.to_datetime(file_info_df["write_time"]).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            file_info_df['raw_data_file_short'] = file_info_df['file_name']
            
            # Remove unwanted serial numbers
            serial_numbers_to_remove = self.config.get('metadata', {}).get('serial_numbers_to_remove', [])
            if serial_numbers_to_remove:
                file_info_df['instrument_instance_specifier'] = file_info_df['instrument_instance_specifier'].replace(serial_numbers_to_remove, pd.NA)
            
            print(f"Unique instrument_instance_specifier values: {file_info_df['instrument_instance_specifier'].unique()}")
            # Only show date range for valid dates
            valid_dates = file_info_df['instrument_analysis_end_date'].dropna()
            if len(valid_dates) > 0:
                print(f"Date range: {valid_dates.min()} to {valid_dates.max()}")
            else:
                print("No valid dates found")
            
            # Keep only relevant columns for merging
            file_info_df = file_info_df[['raw_data_file_short', 'instrument_analysis_end_date', 'instrument_instance_specifier']]
            
            # Merge instrument information
            merged_df = pd.merge(mapped_df, file_info_df, on='raw_data_file_short', how='left')
            
            # Validate merge didn't change row count
            if len(merged_df) != len(mapped_df):
                print(f"❌ Merge error: expected {len(mapped_df)} rows, got {len(merged_df)}")
                return False
            
            # Check for files that didn't get instrument metadata
            missing_metadata = merged_df['instrument_analysis_end_date'].isna().sum()
            if missing_metadata > 0:
                print(f"⚠️  {missing_metadata} files missing instrument metadata (may not be in raw inspection results)")
                missing_files = merged_df[merged_df['instrument_analysis_end_date'].isna()]['raw_data_file_short'].tolist()
                for f in missing_files[:5]:  # Show first 5
                    print(f"   - {f}")
                if len(missing_files) > 5:
                    print(f"   ... and {len(missing_files) - 5} more")
                
                # Remove files without metadata for metadata generation
                merged_df = merged_df[merged_df['instrument_analysis_end_date'].notna()].copy()
                print(f"📊 Proceeding with {len(merged_df)} files that have complete metadata")
            
            # Add common metadata from config that applies to all files
            metadata_config = self.config.get('metadata', {})
            merged_df['processing_institution_workflow'] = metadata_config.get('processing_institution_workflow', 'EMSL')
            merged_df['processing_institution_generation'] = metadata_config.get('processing_institution_generation', 'EMSL')
            
            # Add sample_id (alias for biosample_id)
            merged_df['sample_id'] = merged_df['biosample_id']
            
            # Add biosample.associated_studies (must be in brackets as a list)
            merged_df['biosample.associated_studies'] = f"['{self.config['study']['id']}']"
            
            # Add raw_data_url using configurable URL construction
            use_massive_urls = self.config.get('metadata', {}).get('use_massive_urls', True)
            
            if use_massive_urls:
                # Load FTP URLs to get correct directory structure
                ftp_file = self.study_path / f"{self.study_name}_massive_ftp_locs.csv"
                if ftp_file.exists():
                    ftp_df = pd.read_csv(ftp_file)
                    # Create mapping from filename to full FTP path
                    ftp_mapping = dict(zip(ftp_df['raw_data_file_short'], ftp_df['ftp_location']))
                else:
                    print("⚠️  FTP URLs file not found - using simplified path structure")
                    ftp_mapping = {}
                
                # Construct MASSIVE download URLs
                massive_id = self.config['study']['massive_id']
                
                def construct_massive_url(filename):
                    import urllib.parse
                    import re
                    
                    # Extract just the MSV part (remove version prefix like v07/)
                    if 'MSV' in massive_id:
                        msv_part = 'MSV' + massive_id.split('MSV')[1]
                    else:
                        msv_part = massive_id
                    
                    # Get the full directory path from FTP URL if available
                    if filename in ftp_mapping:
                        ftp_url = ftp_mapping[filename]
                        # Extract path after MSV number: /raw/directory/rawdata/filename.raw
                        match = re.search(rf'{re.escape(msv_part)}(.+)/{re.escape(filename)}', ftp_url)
                        if match:
                            relative_path = match.group(1)  # e.g., /raw/20210819_JGI-AK_MK_506588.../rawdata
                            # Construct the file path: MSV.../full/path/filename
                            file_path = f"{msv_part}{relative_path}/{filename}"
                        else:
                            print(f"⚠️  Could not extract directory structure for {filename}")
                            file_path = f"{msv_part}/raw/{filename}"
                    else:
                        # Fallback to simple structure
                        file_path = f"{msv_part}/raw/{filename}"
                    
                    encoded_path = urllib.parse.quote(file_path, safe='')
                    https_url = f"https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f.{encoded_path}&forceDownload=true"
                    
                    # Validate URL format
                    if not https_url.startswith("https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=f.MSV"):
                        raise ValueError(f"Invalid MASSIVE URL format generated: {https_url}")
                    
                    return https_url
                
                merged_df['raw_data_url'] = merged_df['raw_data_file_short'].apply(construct_massive_url)
                
                # Validate at least 5 URLs to ensure they're accessible
                print("🔍 Validating MASSIVE URL accessibility...")
                self._validate_massive_urls(merged_df['raw_data_url'].head(5).tolist())
            else:
                # Use placeholder URLs for future implementation
                merged_df['raw_data_url'] = 'placeholder://raw_data/' + merged_df['raw_data_file_short']
                print("⚠️  Using placeholder URLs - configure alternative URL construction method")
            
            # Create output directory
            output_dir = self.study_path / "metadata" / "metadata_gen_input_csvs"
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Clear existing files
            for f in output_dir.glob("*.csv"):
                f.unlink()
            
            # Generate configuration-specific CSV files
            config_dfs = self._separate_files_by_configuration(merged_df, metadata_config)
            
            if not config_dfs:
                print("❌ No files matched any configuration filters")
                return False
            
            # Define final columns
            final_columns = [
                'sample_id', 'biosample.associated_studies', 'raw_data_file', 'processed_data_directory', 
                'mass_spec_configuration_name', 'chromat_configuration_name', 'instrument_used', 
                'processing_institution_workflow', 'processing_institution_generation',
                'instrument_analysis_end_date', 'instrument_instance_specifier', 'raw_data_url'
            ]
            
            # Write configuration-specific CSV files
            files_written = 0
            total_files = 0
            
            for config_name, config_df in config_dfs.items():
                # Validate required columns exist
                missing_cols = [col for col in final_columns if col not in config_df.columns]
                if missing_cols:
                    print(f"❌ Skipping {config_name}: missing columns {missing_cols}")
                    continue
                
                # Check for empty dataframe
                if len(config_df) == 0:
                    print(f"⚠️  Skipping {config_name}: no files after filtering")
                    continue
                
                # Write the CSV file
                try:
                    output_df = config_df[final_columns].copy()
                    output_file = output_dir / f"{config_name}_metadata.csv"
                    output_df.to_csv(output_file, index=False)
                    
                    files_written += 1
                    total_files += len(output_df)
                    
                    # Show sample of metadata applied
                    sample_chromat = output_df['chromat_configuration_name'].iloc[0]
                    sample_ms = output_df['mass_spec_configuration_name'].iloc[0]
                    print(f"✅ {config_name}_metadata.csv: {len(output_df)} files ({sample_chromat}, {sample_ms})")
                    
                except Exception as e:
                    print(f"❌ Error writing {config_name}_metadata.csv: {e}")
                    continue
            
            if files_written == 0:
                print("❌ No metadata files were successfully written")
                return False
            
            # Mark as completed and report success
            self.set_skip_trigger('metadata_mapping_generated', True)
            print(f"📋 Successfully generated {files_written} metadata files with {total_files} total entries")
            print(f"   Output directory: {output_dir}")
            return True
            
        except Exception as e:
            print(f"❌ Error generating metadata mapping files: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _separate_files_by_configuration(self, merged_df: pd.DataFrame, metadata_config: dict) -> dict:
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
        default_instrument = metadata_config.get('instrument_used', 'Unknown')
        default_mass_spec = metadata_config.get('mass_spec_configuration_name', 'Unknown') 
        default_chromat = metadata_config.get('chromat_configuration_name', 'Unknown')
        
        print(f"📊 Separating {len(merged_df)} files into configurations...")
        
        for config in self.config.get('configurations', []):
            config_name = config['name']
            file_filters = config.get('file_filter', [])
            
            # Filter files for this configuration using AND logic (all filters must match)
            if file_filters:
                matching_indices = []
                for idx, row in merged_df.iterrows():
                    filename = row['raw_data_file_short'].lower()
                    if all(filter_term.lower() in filename for filter_term in file_filters):
                        matching_indices.append(idx)
                
                if matching_indices:
                    config_df = merged_df.loc[matching_indices].copy()
                else:
                    print(f"⚠️  Configuration '{config_name}': No files match filters {file_filters}")
                    continue
            else:
                # No filters specified - include all files
                config_df = merged_df.copy()
                print(f"ℹ️  Configuration '{config_name}': No filters specified, including all files")
            
            # Apply configuration-specific metadata (with fallback to defaults)
            config_df['instrument_used'] = config.get('instrument_used', default_instrument)
            config_df['chromat_configuration_name'] = config.get('chromat_configuration_name', default_chromat)
            config_df['mass_spec_configuration_name'] = config.get('mass_spec_configuration_name', default_mass_spec)
            
            # Apply pattern-based metadata overrides
            metadata_overrides = config.get('metadata_overrides', {})
            if metadata_overrides:
                for metadata_field, pattern_mapping in metadata_overrides.items():
                    if pattern_mapping:
                        # Apply pattern-specific overrides based on filename patterns
                        def get_override_value(filename, field_name, mapping, fallback_value):
                            for pattern, override_value in mapping.items():
                                if pattern in filename:
                                    return override_value
                            # Return current value if no pattern matches
                            return fallback_value
                        
                        # Get current values as fallback
                        current_values = config_df[metadata_field] if metadata_field in config_df.columns else config.get(metadata_field, 'Unknown')
                        config_df[metadata_field] = config_df['raw_data_file_short'].apply(
                            lambda filename: get_override_value(filename, metadata_field, pattern_mapping, current_values)
                        )
            
            config_dfs[config_name] = config_df
            
            # Report results with pattern-based differentiation if applicable
            filter_desc = f"filters {file_filters}" if file_filters else "no filters (all files)"
            chromat_config = config_df['chromat_configuration_name'].iloc[0]
            
            # Check for pattern-based overrides in any metadata field
            metadata_overrides = config.get('metadata_overrides', {})
            override_summaries = []
            
            for metadata_field, pattern_mapping in metadata_overrides.items():
                if pattern_mapping and metadata_field in config_df.columns:
                    unique_values = config_df[metadata_field].unique()
                    if len(unique_values) > 1:
                        # Multiple values due to pattern-based overrides
                        value_breakdown = config_df[metadata_field].value_counts()
                        field_desc = ", ".join([f"{count} files with {val[:25]}..." if len(val) > 25 else f"{count} files with {val}" 
                                              for val, count in value_breakdown.items()])
                        override_summaries.append(f"{metadata_field}: {field_desc}")
            
            if override_summaries:
                # Multiple metadata configurations due to pattern-based overrides
                metadata_desc = f"chromat='{chromat_config}', Pattern-based overrides: {'; '.join(override_summaries)}"
            else:
                # Single metadata configuration
                ms_config = config_df['mass_spec_configuration_name'].iloc[0]
                metadata_desc = f"chromat='{chromat_config}', ms='{ms_config}'"
                
            print(f"✅ Configuration '{config_name}': {len(config_df)} files match {filter_desc} ({metadata_desc})")
        
        # Fallback: if no configurations worked, create single dataset with defaults
        if not config_dfs:
            print("⚠️  No configurations matched any files - creating fallback configuration")
            fallback_df = merged_df.copy()
            fallback_df['instrument_used'] = default_instrument
            fallback_df['mass_spec_configuration_name'] = default_mass_spec
            fallback_df['chromat_configuration_name'] = default_chromat
            config_dfs['all_data'] = fallback_df
            print(f"📋 Fallback configuration: {len(fallback_df)} files with default metadata")
        
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
                req = urllib.request.Request(url, method='HEAD')
                response = urllib.request.urlopen(req, context=ssl_context, timeout=15)
                
                if response.status == 200:
                    successful_urls += 1
                    print(f"✅ URL {i+1}/{total_tested}: Accessible (Status: {response.status})")
                    
                    # Check if it looks like a file download
                    content_type = response.headers.get('Content-Type', '')
                    content_length = response.headers.get('Content-Length')
                    if content_length:
                        print(f"   File size: {int(content_length):,} bytes")
                else:
                    print(f"⚠️  URL {i+1}/{total_tested}: Unexpected status {response.status}")
                    
            except urllib.error.HTTPError as e:
                print(f"❌ URL {i+1}/{total_tested}: HTTP {e.code} - {e.reason}")
                if e.code == 404:
                    print("   This file may not exist in the MASSIVE dataset")
            except Exception as e:
                print(f"❌ URL {i+1}/{total_tested}: {type(e).__name__}: {e}")
        
        if successful_urls == 0:
            raise ValueError(f"None of the {total_tested} tested MASSIVE URLs are accessible. "
                           "Check the MASSIVE dataset ID and file paths.")
        elif successful_urls < total_tested // 2:
            print(f"⚠️  Warning: Only {successful_urls}/{total_tested} URLs are accessible. "
                  "Some files may not be available in MASSIVE.")
        else:
            print(f"✅ URL validation passed: {successful_urls}/{total_tested} URLs accessible")
            
        return True
    
    def generate_nmdc_submission_packages(self) -> bool:
        """
        Needs to be implemented.
        """
        raise NotImplementedError("generate_nmdc_submission_packages() is not yet implemented.")
    
    def submit_metadata_packages(self, environment: str = 'dev') -> bool:
        """
        Needs to be implemented.
        """
        raise NotImplementedError("submit_metadata_packages() is not yet implemented.")