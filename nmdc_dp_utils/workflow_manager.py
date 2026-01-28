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
import logging
import pandas as pd
from pathlib import Path
from minio import Minio
from typing import Dict, List, Optional

from nmdc_dp_utils.workflow_manager_mixins import (
    skip_if_complete,
    WorkflowDataMovementManager,
    NMDCWorkflowDataProcessManager,
    NMDCWorkflowBiosampleManager,
    WorkflowRawDataInspectionManager,
    WorkflowMetadataManager,
    WORKFLOW_DICT,
    LLMWorkflowManagerMixin
)


class NMDCWorkflowManager(
    WorkflowDataMovementManager,
    NMDCWorkflowDataProcessManager,
    NMDCWorkflowBiosampleManager,
    WorkflowRawDataInspectionManager,
    WorkflowMetadataManager,
    LLMWorkflowManagerMixin
):
    """
    A configurable class for managing NMDC mass spectrometry data workflows.

    This class provides a unified interface for:
    - Setting up standardized workflow directory structures
    - Discovering and downloading raw data from MASSIVE datasets
    - Uploading/downloading processed data to/from MinIO object storage
    - Generating WDL workflow configuration files for batch processing

    The class is configured via JSON files that specify study metadata,
    file paths, processing configurations, and dataset identifiers.

    Example:
        >>> manager = NMDCWorkflowManager('config.json')
        >>> manager.create_workflow_structure()
        >>> ftp_df = manager.get_massive_ftp_urls()
        >>> manager.download_from_massive()
        >>> manager.generate_wdl_jsons()
    """

    def __init__(self, config_path: str):
        """
        Initialize the workflow manager with a configuration file.

        Args:
            config_path: Path to the JSON configuration file containing study and workflow metadata,
                        paths, MinIO settings, and processing configurations.

        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            json.JSONDecodeError: If the configuration file is invalid JSON
            KeyError: If required configuration fields are missing
        """
        # Convert to absolute path so it works regardless of current working directory
        self.config_path = str(Path(config_path).resolve())
        self.config = self.load_config(config_path)
        self.workflow_name = self.config["workflow"]["name"]
        self.study_name = self.config["study"]["name"]
        self.study_id = self.config["study"]["id"]
        self.base_path = Path(self.config["paths"]["base_directory"])
        self.workflow_path = self.base_path / "studies" / f"{self.workflow_name}"

        # Initialize logger
        self.logger = logging.getLogger(f"nmdc.{self.workflow_name}")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
            self.logger.addHandler(handler)

        # Configure level from environment (default INFO)
        level_name = os.getenv("NMDC_LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
        self.logger.setLevel(level)

        # Optional file logging via environment variable
        log_file = os.getenv("NMDC_LOG_FILE")
        if log_file:
            abs_log_file = os.path.abspath(log_file)
            has_same_file_handler = any(
                isinstance(h, logging.FileHandler)
                and getattr(h, "baseFilename", None) == abs_log_file
                for h in self.logger.handlers
            )
            if not has_same_file_handler:
                file_handler = logging.FileHandler(abs_log_file)
                file_handler.setFormatter(
                    logging.Formatter(
                        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                    )
                )
                self.logger.addHandler(file_handler)
                self.logger.info(f"File logging enabled: {abs_log_file}")

        # Construct dynamic paths from data_directory
        self.data_directory = Path(self.config["paths"]["data_directory"])
        self.raw_data_directory = self.data_directory / f"{self.study_name}" / "raw"

        # Construct processed_data_directory with date tag
        processed_date_tag = self.config["workflow"].get("processed_data_date_tag", "")
        if processed_date_tag:
            self.processed_data_directory = (
                self.data_directory
                / f"{self.study_name}"
                / f"processed_{processed_date_tag}"
            )
        else:
            self.processed_data_directory = (
                self.data_directory / f"{self.study_name}" / "processed"
            )

        # MinIO client will be lazy-loaded when first accessed
        self._minio_client = None
        super().__init__()

    @property
    def minio_client(self) -> Optional[Minio]:
        """
        Get MinIO client, initializing it on first access if credentials are available.
        
        This lazy-loads the MinIO client only when actually needed, avoiding
        initialization errors when MinIO is not required for the workflow.
        
        Returns:
            Configured MinIO client, or None if credentials unavailable
        """
        if self._minio_client is None:
            self._minio_client = self._init_minio_client()
        return self._minio_client

    def show_available_workflow_types(self) -> List[str]:
        """
        Show the available workflow types supported by the NMDCWorkflowManager.

        Returns:
            List of available workflow type names.
        """
        return list(WORKFLOW_DICT.keys())

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
        with open(config_path, "r") as f:
            config = json.load(f)

        # Initialize skip_triggers with default values, preserving existing ones
        default_triggers = {
            "study_structure_created": False,
            "raw_data_downloaded": False,
            "protocol_outline_created": True,
            "biosample_attributes_fetched": False,
            "biosample_mapping_script_generated": False,
            "biosample_mapping_completed": False,
            "wdls_generated": False,
            "data_processed": False,
        }
        
        if "skip_triggers" not in config:
            config["skip_triggers"] = default_triggers
        else:
            # Merge defaults with existing triggers, preserving existing values
            for key, value in default_triggers.items():
                if key not in config["skip_triggers"]:
                    config["skip_triggers"][key] = value

        return config

    def should_skip(self, trigger_name: str) -> bool:
        """
        Check if a workflow step should be skipped based on trigger.

        Args:
            trigger_name: Name of the skip trigger to check

        Returns:
            True if the step should be skipped, False otherwise
        """
        return self.config.get("skip_triggers", {}).get(trigger_name, False)

    def set_skip_trigger(self, trigger_name: str, value: bool, save: bool = True):
        """
        Set a skip trigger value and optionally save to config file.

        Args:
            trigger_name: Name of the skip trigger to set
            value: Boolean value to set
            save: Whether to save the updated config to file
        """
        if "skip_triggers" not in self.config:
            self.config["skip_triggers"] = {}

        self.config["skip_triggers"][trigger_name] = value

        if save:
            with open(self.config_path, "w") as f:
                json.dump(self.config, f, indent=4)

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
            >>> manager = NMDCWorkflowManager('config.json')
            >>> manager.reset_all_triggers()
            All workflow steps will now run when executed
        """
        if "skip_triggers" not in self.config:
            self.config["skip_triggers"] = {}
            self.logger.info("No skip triggers found - nothing to reset")
            return

        # Get list of current triggers before resetting
        current_triggers = list(self.config["skip_triggers"].keys())

        if not current_triggers:
            self.logger.info("No skip triggers found - nothing to reset")
            return

        # Reset all triggers to False
        for trigger_name in current_triggers:
            self.config["skip_triggers"][trigger_name] = False

        if save and hasattr(self, "config_path"):
            with open(self.config_path, "w") as f:
                json.dump(self.config, f, indent=4)

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
                self.config["minio"]["endpoint"],
                access_key=os.environ["MINIO_ACCESS_KEY"],
                secret_key=os.environ["MINIO_SECRET_KEY"],
                secure=self.config["minio"]["secure"],
            )
        except KeyError:
            return None

    @skip_if_complete("study_structure_created", return_value=True)
    def create_workflow_structure(self) -> bool:
        """
        Create the standard directory structure for a workflow for a study.

        Creates the following directories under the workflow path:
        - scripts/: Workflow-specific scripts and utilities
        - metadata/: Configuration files and study metadata
        - wdl_jsons/: Generated WDL workflow configuration files
        - raw_file_info/: Information about raw data files

        Additional subdirectories are created for each processing configuration
        specified in the config file.

        Returns:
            True if structure creation completed successfully, False otherwise
        """
        try:
            directories = [
                self.workflow_path,
                self.workflow_path / "scripts",
                self.workflow_path / "metadata",
                self.workflow_path / "wdl_jsons",
                self.workflow_path / "raw_file_info",
            ]

            # Add configuration-specific directories
            if "configurations" in self.config:
                for config in self.config["configurations"]:
                    directories.append(
                        self.workflow_path / "wdl_jsons" / config["name"]
                    )

            for directory in directories:
                directory.mkdir(parents=True, exist_ok=True)

            self.logger.info(
                f"Created study structure for {self.workflow_name} at {self.workflow_path}"
            )
            self.set_skip_trigger("study_structure_created", True)
            return True
        except Exception as e:
            self.logger.error(f"Error creating workflow structure: {e}")
            return False

    def get_workflow_info(self) -> Dict:
        """
        Get summary information about the workflow configuration.

        Returns:
            Dictionary containing workflow metadata, paths, and configuration summary
        """
        info = {
            "workflow_name": self.workflow_name,
            "study_name": self.study_name,
            "study_id": self.study_id,
            "workflow_path": str(self.workflow_path),
            "massive_id": self.config["workflow"].get("massive_id", "Not configured"),
            "file_type": self.config["workflow"].get("file_type", ".raw"),
            "file_filters": self.config["workflow"].get("file_filters", []),
            "num_configurations": len(self.config.get("configurations", [])),
            "configuration_names": [
                c["name"] for c in self.config.get("configurations", [])
            ],
            "raw_data_directory": str(self.raw_data_directory),
            "processed_data_directory": str(self.processed_data_directory),
            "minio_enabled": self.minio_client is not None,
        }
        return info
