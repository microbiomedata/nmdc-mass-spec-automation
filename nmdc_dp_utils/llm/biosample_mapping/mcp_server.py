"""
MCP server for biosample mapping validation
"""

import sys
from pathlib import Path

# Add workspace root to path to allow imports when running as MCP subprocess
workspace_root = Path(__file__).parent.parent.parent.parent
if str(workspace_root) not in sys.path:
    sys.path.insert(0, str(workspace_root))

import logging
from mcp.server.fastmcp import FastMCP
from nmdc_dp_utils.llm.biosample_mapping.validation import validate_biosample_mapping_csv

logging.basicConfig(level=logging.INFO)

# Global storage for input data (to be set before running validation)
_biosample_attributes = None
_raw_files = None
_material_processing_yaml = None

mcp = FastMCP(
    "NMDC Biosample Mapping Validator",
    instructions=(
        "You are an MCP server that validates biosample-to-raw-file mappings for NMDC mass spectrometry data."
    ),
)


def set_validation_context(biosample_attributes: str, raw_files: str, material_processing_yaml: str):
    """
    Set the context data needed for validation.
    This should be called before starting the MCP server.
    
    Parameters
    ----------
    biosample_attributes (str) : Biosample attributes CSV content
    raw_files (str) : Raw files CSV content
    material_processing_yaml (str) : Material processing YAML content
    """
    global _biosample_attributes, _raw_files, _material_processing_yaml
    _biosample_attributes = biosample_attributes
    _raw_files = raw_files
    _material_processing_yaml = material_processing_yaml
    logging.info("Validation context set successfully")


def clean_csv_response(response: str) -> str:
    """Remove markdown code fences from LLM response."""
    response = response.strip()
    if response.startswith("```csv"):
        response = response[6:]  # Remove ```csv
    elif response.startswith("```"):
        response = response[3:]  # Remove ```
    if response.endswith("```"):
        response = response[:-3]  # Remove trailing ```
    return response.strip()


@mcp.tool()
def validate_biosample_mapping(csv_mapping: str) -> dict:
    """
    Validate the provided biosample mapping CSV against the input data.
    You must call this function after generating the CSV to ensure correctness.

    This tool validates:
    - Biosample IDs exist in biosample attributes and follow NMDC format (nmdc:bsm-XX-XXXXXXXX)
    - Biosample names match the biosample IDs
    - Processed sample placeholders exist in the material processing YAML
    - Protocol IDs match top-level protocols in the YAML
    - CSV formatting is correct with required columns
    
    Note: Unmapped raw files generate warnings (not errors) and are saved to unmapped_files.txt
    for manual review. This is expected for QC samples, blanks, standards, etc.

    Parameters
    ----------
    csv_mapping (str): The biosample mapping CSV as a string (with or without markdown code fences)

    Returns
    -------
    dict: Validation results with:
          - 'valid' (bool): True if no errors found
          - 'errors' (list of str): Critical issues that must be fixed
          - 'warnings' (list of str): Non-critical issues for review
          - 'unmapped_files_count' (int): Number of unmapped raw files
          - 'unmapped_files_saved' (str): Path to file with unmapped files list (if any)
    """
    global _biosample_attributes, _raw_files, _material_processing_yaml
    
    logging.info("Within validate_biosample_mapping MCP tool")
    
    # Check that context has been set
    if _biosample_attributes is None or _raw_files is None or _material_processing_yaml is None:
        return {
            "valid": False,
            "errors": ["Validation context not set. Please ensure biosample attributes, raw files, and material processing YAML have been provided."]
        }
    
    # Clean the CSV response
    clean_csv = clean_csv_response(csv_mapping)
    
    # Perform validation
    try:
        validation_result = validate_biosample_mapping_csv(
            csv_content=clean_csv,
            biosample_attributes_csv=_biosample_attributes,
            material_processing_yaml=_material_processing_yaml,
            raw_files_csv=_raw_files
        )
        
        logging.info(f"Validation result: {'PASS' if validation_result['valid'] else 'FAIL'}")
        
        # Handle errors
        if not validation_result['valid']:
            logging.warning(f"Validation errors: {len(validation_result['errors'])} found")
            for error in validation_result['errors'][:5]:  # Log first 5 errors
                logging.warning(f"  - {error}")
        
        # Handle warnings
        if validation_result.get('warnings'):
            logging.info(f"Validation warnings: {len(validation_result['warnings'])} found")
            for warning in validation_result['warnings']:
                logging.info(f"  - {warning}")
        
        # Save unmapped files to a file if any exist
        unmapped_files = validation_result.get('unmapped_files', [])
        unmapped_files_path = None
        if unmapped_files:
            unmapped_files_path = "unmapped_files.txt"
            with open(unmapped_files_path, 'w') as f:
                f.write(f"# Unmapped raw data files ({len(unmapped_files)})\n")
                f.write("# These files could not be mapped to biosamples\n")
                f.write("# May include QC samples, blanks, standards, extraction controls, etc.\n")
                f.write("# Please review and determine appropriate handling\n\n")
                for filename in unmapped_files:
                    f.write(f"{filename}\n")
            logging.info(f"Saved {len(unmapped_files)} unmapped files to {unmapped_files_path}")
        
        # Build response
        response = {
            "valid": validation_result['valid'],
            "errors": validation_result['errors'],
            "warnings": validation_result.get('warnings', []),
            "unmapped_files_count": len(unmapped_files)
        }
        
        if unmapped_files_path:
            response["unmapped_files_saved"] = unmapped_files_path
            response["message"] = f"Validation {'passed' if validation_result['valid'] else 'failed'}. {len(unmapped_files)} unmapped files saved to {unmapped_files_path} - please review."
        else:
            response["message"] = f"Validation {'passed' if validation_result['valid'] else 'failed'}."
        
        return response
        
    except Exception as e:
        logging.error(f"Error during biosample mapping validation: {e}")
        return {
            "valid": False,
            "errors": [f"Validation system error: {str(e)}"]
        }


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
