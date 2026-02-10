"""
MCP server for biosample mapping validation
"""

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
    - All raw files are mapped
    - CSV formatting is correct with required columns

    Parameters
    ----------
    csv_mapping (str): The biosample mapping CSV as a string (with or without markdown code fences)

    Returns
    -------
    dict: Validation results with 'valid' (bool) and 'errors' (list of str).
          If valid is True, the mapping is correct.
          If valid is False, errors list contains specific issues to fix.
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
        if not validation_result['valid']:
            logging.warning(f"Validation errors: {len(validation_result['errors'])} found")
            for error in validation_result['errors'][:5]:  # Log first 5 errors
                logging.warning(f"  - {error}")
        
        return validation_result
        
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
