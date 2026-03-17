"""
Combined MCP server for NMDC data processing tools.

This server provides tools for both:
1. Protocol conversion: NMDC LinkML schema context and YAML validation
2. Biosample mapping: Script execution and validation with autonomous iteration

Tool filtering can be applied on the client side when connecting to this server.
Use create_static_tool_filter or dynamic filtering to expose only relevant tools
to each agent. Example:

    from agents.mcp import MCPServerStdio, create_static_tool_filter
    
    # For protocol conversion agent
    protocol_server = MCPServerStdio(
        params={"command": "python", "args": ["-m", "nmdc_dp_utils.llm.mcp_server"]},
        tool_filter=create_static_tool_filter(
            allowed_tool_names=["get_protocol_schema_context", "validate_generated_yaml"]
        ),
    )
    
    # For biosample mapping agent (autonomous script generation and testing)
    biosample_server = MCPServerStdio(
        params={"command": "python", "args": ["-m", "nmdc_dp_utils.llm.mcp_server"]},
        tool_filter=create_static_tool_filter(
            allowed_tool_names=["set_biosample_validation_context", "execute_and_validate_mapping_script"]
        ),
    )
"""

import sys
from pathlib import Path

# Add workspace root to path to allow imports when running as MCP subprocess
workspace_root = Path(__file__).parent.parent.parent.parent
if str(workspace_root) not in sys.path:
    sys.path.insert(0, str(workspace_root))

import os
from linkml_runtime.utils.schemaview import SchemaView
import nmdc_schema
from nmdc_ms_metadata_gen.validate_yaml_outline import validate_yaml_outline
import logging
logging.basicConfig(level=logging.INFO)

from mcp.server.fastmcp import FastMCP

# Import after sys.path setup to allow module resolution
from nmdc_dp_utils.llm.biosample_mapping.validation import validate_biosample_mapping_csv

mcp = FastMCP(
    "NMDC Data Processing Tools",
    instructions=(
        "You are an MCP server that provides tools for NMDC data processing. "
        "Tools for protocol conversion: get_protocol_schema_context, validate_generated_yaml. "
        "Tools for biosample mapping: set_biosample_validation_context, execute_and_validate_mapping_script, validate_biosample_mapping. "
        "For biosample mapping workflow: (1) call set_biosample_validation_context first, "
        "(2) then generate and test scripts with execute_and_validate_mapping_script."
    ),
)

# Global storage for biosample mapping validation context
_biosample_attributes = None
_raw_files = None
_material_processing_yaml = None

# =============================================================================
# PROTOCOL CONVERSION TOOLS
# =============================================================================

@mcp.tool()
def get_protocol_schema_context() -> dict:
    """
    Extract classes related to 'MaterialProcessing' from NMDC schema
    and convert them to a JSON format suitable for LLM context.
    
    Use this tool to get the NMDC schema definitions needed for protocol conversion.
    """
    logging.info("Within get_protocol_schema_context mcp tool.")
    # Initialize SchemaView from NMDC schema package
    nmdc_path = os.path.dirname(nmdc_schema.__file__)
    schema_path = os.path.join(nmdc_path, "nmdc_materialized_patterns.yaml")
    schema_view = SchemaView(schema_path)

    # Get all classes that are subclasses of 'MaterialProcessing'
    all_classes = schema_view.all_classes()
    relevant_classes = {
        class_name: class_def
        for class_name, class_def in all_classes.items()
        if class_def.is_a and "MaterialProcessing" in schema_view.get_class(class_def.is_a).name or
              class_name == "ProcessedSample"
    }

    # Recursively find all related classes and enums
    # For each slot in each relevant class, if the range is an enum or inline class, add it
    # Only include classes that are used inline (not just referenced by ID)
    # Continue until no new classes or enums are found
    enums = {}
    new_found = True
    while new_found:
        new_found = False
        for class_name, class_def in list(relevant_classes.items()):
            # Check only slots defined in this class (not inherited) for inline usage
            for slot_name in class_def.slots:
                # Get the induced slot (which includes slot_usage overrides)
                slot_def = schema_view.induced_slot(slot_name, class_name)
                slot_range = slot_def.range
                
                # Check if range is an enum
                enum_def = schema_view.get_enum(slot_range)
                if enum_def and slot_range not in enums:
                    enums[slot_range] = enum_def
                    new_found = True
                
                # Check if range is a class that's used inline, if so, add it to relevant_classes
                class_range_def = schema_view.get_class(slot_range)
                if class_range_def and slot_range not in relevant_classes:
                    # Only include if the slot is inlined or inlined_as_list
                    if slot_def.inlined or slot_def.inlined_as_list:
                        relevant_classes[slot_range] = class_range_def
                        new_found = True

    # Convert classes and enums to LLM-friendly format
    schema_output = {
        "classes": {},
        "slots": {},
        "enums": {name: enum_def._as_json_obj() for name, enum_def in enums.items()}
    }
    
    # Collect all unique slot definitions across all classes
    all_slot_definitions = {}
    
    # For each class, include slot names and collect slot definitions
    for class_name, class_def in relevant_classes.items():
        class_data = class_def._as_json_obj()
        
        # Get all induced slots for this class (includes inherited slots)
        class_slot_names = []
        for slot_name in schema_view.class_slots(class_name):
            class_slot_names.append(slot_name)
            
            # Collect slot definition if not already captured
            if slot_name not in all_slot_definitions:
                induced_slot = schema_view.induced_slot(slot_name, class_name)
                slot_info = {
                    "range": induced_slot.range,
                }
                # Only add non-null values for these fields
                for attr in ["description", "required", "multivalued"]:
                    value = getattr(induced_slot, attr, None)
                    if value is not None:
                        slot_info[attr] = value
                
                all_slot_definitions[slot_name] = slot_info
        
        # Store just the slot names in the class
        class_data["class_slots"] = class_slot_names
        # Remove the class_data["slots"] since we are replacing it with class_slots
        if "slots" in class_data:
            del class_data["slots"]
        schema_output["classes"][class_name] = class_data
    
    # Add all collected slot definitions
    schema_output["slots"] = all_slot_definitions
    
    return schema_output

def _clean_yaml_response(response: str) -> str:
    """Remove markdown code fences from YAML LLM response."""
    # Remove ```yaml and ``` markers
    response = response.strip()
    if response.startswith("```yaml"):
        response = response[7:]  # Remove ```yaml
    elif response.startswith("```"):
        response = response[3:]  # Remove ```
    if response.endswith("```"):
        response = response[:-3]  # Remove trailing ```
    return response.strip()


def _clean_csv_response(response: str) -> str:
    """Remove markdown code fences from CSV LLM response."""
    response = response.strip()
    if response.startswith("```csv"):
        response = response[6:]  # Remove ```csv
    elif response.startswith("```"):
        response = response[3:]  # Remove ```
    if response.endswith("```"):
        response = response[:-3]  # Remove trailing ```
    return response.strip()


@mcp.tool()
def validate_generated_yaml(yaml_outline: str) -> dict:
    """
    Validate the provided YAML outline against NMDC schema.
    You must call this function at least once after generating the outline to ensure compliance.

    Parameters
    ----------
    yaml_outline (str): The YAML outline as a string (with or without markdown code fences).

    Returns
    -------
    dict: Validation results including errors and warnings.
    """
    clean_yaml_res = _clean_yaml_response(yaml_outline)
    logging.info("Within validate_generated_yaml MCP tool.")
    # save the yaml outline to a temporary file
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".yaml") as temp_yaml_file:
        temp_yaml_file.write(clean_yaml_res)
        temp_yaml_file_path = temp_yaml_file.name
    logging.info(f"Temporary YAML outline saved to: {temp_yaml_file_path}")
    try:
        validation_results = validate_yaml_outline(temp_yaml_file_path, test=True)
    except Exception as e:
        logging.error(f"Error during YAML validation: {e}")
        validation_results = {"errors": [str(e)], "warnings": []}
    logging.info(f"Validation results: {validation_results}")
    return validation_results

# =============================================================================
# BIOSAMPLE MAPPING TOOLS
# =============================================================================

@mcp.tool()
def set_biosample_validation_context(
    biosample_attributes: str, 
    raw_files: str, 
    material_processing_yaml: str
) -> dict:
    """
    Set the validation context for biosample mapping.
    
    This tool stores the reference data that will be used to validate generated mapping CSV files.
    Call this tool ONCE at the beginning before generating any mapping scripts.
    
    The data you provide here will be used to validate:
    - Biosample IDs exist and follow NMDC format
    - Protocol IDs match those in the YAML
    - Processed sample placeholders are valid
    - All raw files are accounted for
    
    Parameters
    ----------
    biosample_attributes (str): CSV content with biosample id and name columns
    raw_files (str): CSV content with raw file names  
    material_processing_yaml (str): YAML content defining protocols and processed samples
    
    Returns
    -------
    dict: Confirmation message with data summary
    """
    global _biosample_attributes, _raw_files, _material_processing_yaml
    
    _biosample_attributes = biosample_attributes
    _raw_files = raw_files
    _material_processing_yaml = material_processing_yaml
    
    logging.info("Biosample validation context set successfully")
    
    # Parse to provide feedback
    import io
    import csv
    import yaml as yaml_lib
    
    biosample_reader = csv.DictReader(io.StringIO(biosample_attributes))
    biosample_count = len(list(biosample_reader))
    
    file_reader = csv.DictReader(io.StringIO(raw_files))
    file_count = len(list(file_reader))
    
    yaml_data = yaml_lib.safe_load(material_processing_yaml)
    protocol_count = len(yaml_data.keys()) if yaml_data else 0
    
    return {
        "status": "success",
        "message": "Validation context set successfully",
        "summary": {
            "biosamples": biosample_count,
            "raw_files": file_count,
            "protocols": protocol_count,
            "protocol_ids": list(yaml_data.keys()) if yaml_data else []
        }
    }


@mcp.tool()
def execute_and_validate_mapping_script(script_code: str, output_csv_path: str) -> dict:
    """
    Execute a Python biosample mapping script and validate the generated CSV output.
    This tool allows you to test your generated script and receive immediate feedback.
    
    The script will be executed in the workspace root directory. After execution,
    the generated CSV will be validated against the biosample attributes, raw files,
    and material processing YAML that were provided as context.
    
    Use this tool to iteratively test and refine your mapping script. If validation
    fails, review the errors and generate a corrected script.
    
    Parameters
    ----------
    script_code (str): The complete Python script code (with or without markdown code fences)
    output_csv_path (str): Path where the script will write the output CSV (relative to workspace root)
    
    Returns
    -------
    dict: Execution and validation results with:
          - 'execution_success' (bool): True if script ran without errors
          - 'execution_error' (str): Error message if script failed to execute
          - 'stdout' (str): Script standard output
          - 'stderr' (str): Script standard error
          - 'csv_created' (bool): True if output CSV was created
          - 'validation_results' (dict): Validation results (if CSV was created)
            - 'valid' (bool): True if CSV passed validation
            - 'errors' (list of str): Validation errors
            - 'warnings' (list of str): Validation warnings
          - 'message' (str): Overall summary
    """
    import subprocess
    import tempfile
    import os
    
    global _biosample_attributes, _raw_files, _material_processing_yaml
    
    logging.info("Within execute_and_validate_mapping_script MCP tool")
    
    # Check that context has been set
    if _biosample_attributes is None or _raw_files is None or _material_processing_yaml is None:
        return {
            "execution_success": False,
            "execution_error": "Validation context not set. Biosample attributes, raw files, and YAML must be provided.",
            "csv_created": False,
            "message": "Cannot execute: context not initialized."
        }
    
    # Clean the script code (remove markdown if present)
    clean_script = script_code.strip()
    if clean_script.startswith("```python"):
        clean_script = clean_script[9:]  # Remove ```python
    elif clean_script.startswith("```"):
        clean_script = clean_script[3:]  # Remove ```
    if clean_script.endswith("```"):
        clean_script = clean_script[:-3]  # Remove trailing ```
    clean_script = clean_script.strip()
    
    # Create a temporary file for the script
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_script:
        temp_script.write(clean_script)
        temp_script_path = temp_script.name
    
    try:
        # Get workspace root (assuming it's 4 levels up from this file)
        workspace_root = Path(__file__).parent.parent.parent
        
        # Execute the script
        logging.info(f"Executing script, output expected at: {output_csv_path}")
        result = subprocess.run(
            [sys.executable, temp_script_path],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(workspace_root)
        )
        
       # Check execution result
        if result.returncode != 0:
            logging.error(f"Script execution failed with return code {result.returncode}")
            return {
                "execution_success": False,
                "execution_error": f"Script failed with return code {result.returncode}",
                "stdout": result.stdout,
                "stderr": result.stderr,
                "csv_created": False,
                "message": "Script execution failed. Review stderr for details."
            }
        
        logging.info("Script executed successfully")
        
        # Check if output CSV was created
        output_path = workspace_root / output_csv_path
        if not output_path.exists():
            logging.error(f"Output CSV not created at: {output_path}")
            return {
                "execution_success": True,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "csv_created": False,
                "execution_error": f"Script ran but did not create output file: {output_csv_path}",
                "message": "Script executed but no CSV output was created."
            }
        
        logging.info(f"Output CSV created: {output_path}")
        
        # Read the generated CSV
        with open(output_path, 'r') as f:
            generated_csv = f.read()
        
        # Validate the CSV
        logging.info("Validating generated CSV...")
        validation_result = validate_biosample_mapping_csv(
            csv_content=generated_csv,
            biosample_attributes_csv=_biosample_attributes,
            material_processing_yaml=_material_processing_yaml,
            raw_files_csv=_raw_files
        )
        
        logging.info(f"Validation result: {'PASS' if validation_result['valid'] else 'FAIL'}")
        
        # Build response
        response = {
            "execution_success": True,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "csv_created": True,
            "validation_results": {
                "valid": validation_result['valid'],
                "errors": validation_result['errors'],
                "warnings": validation_result.get('warnings', [])
            }
        }
        
        if validation_result['valid']:
            response["message"] = "Script executed successfully and CSV passed validation!"
        else:
            response["message"] = f"Script executed but CSV has {len(validation_result['errors'])} validation errors. Review and fix the script."
        
        return response
        
    except subprocess.TimeoutExpired:
        logging.error("Script execution timed out")
        return {
            "execution_success": False,
            "execution_error": "Script execution timed out (>30 seconds)",
            "csv_created": False,
            "message": "Script took too long to execute. Make it more efficient."
        }
    except Exception as e:
        logging.error(f"Error during script execution: {e}")
        import traceback
        return {
            "execution_success": False,
            "execution_error": str(e),
            "stderr": traceback.format_exc(),
            "csv_created": False,
            "message": f"Error during execution: {str(e)}"
        }
    finally:
        # Clean up temporary script file
        try:
            os.unlink(temp_script_path)
        except OSError:
            pass


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
    
    Note: It is not unusual for some raw files to remain unmapped. This is expected for QC samples, blanks, standards, etc.

    Parameters
    ----------
    csv_mapping (str): The biosample mapping CSV as a string (with or without markdown code fences)

    Returns
    -------
    dict: Validation results with:
          - 'valid' (bool): True if no errors found
          - 'errors' (list of str): Critical issues that must be fixed
          - 'warnings' (list of str): Non-critical issues for review
          - 'message' (str): Summary message
    """
    global _biosample_attributes, _raw_files, _material_processing_yaml
    
    logging.info("Within validate_biosample_mapping MCP tool")
    
    # Check that context has been set
    if _biosample_attributes is None or _raw_files is None or _material_processing_yaml is None:
        return {
            "valid": False,
            "errors": ["Validation context not set. Please ensure biosample attributes, raw files, and material processing YAML have been provided."],
            "warnings": [],
            "message": "Validation context not initialized."
        }
    
    # Clean the CSV response
    clean_csv = _clean_csv_response(csv_mapping)
    
    # Perform validation
    try:
        validation_result = validate_biosample_mapping_csv(
            csv_content=clean_csv,
            biosample_attributes_csv=_biosample_attributes,
            material_processing_yaml=_material_processing_yaml,
            raw_files_csv=_raw_files
        )
        
        logging.info(f"Biosample mapping validation result: {'PASS' if validation_result['valid'] else 'FAIL'}")
        
        # Handle errors
        if not validation_result['valid']:
            logging.warning(f"Biosample mapping validation errors: {len(validation_result['errors'])} found")
            for error in validation_result['errors'][:5]:  # Log first 5 errors
                logging.warning(f"  - {error}")
        
        # Handle warnings
        if validation_result.get('warnings'):
            logging.info(f"Biosample mapping validation warnings: {len(validation_result['warnings'])} found")
            for warning in validation_result['warnings']:
                logging.info(f"  - {warning}")

        # Build response
        response = {
            "valid": validation_result['valid'],
            "errors": validation_result['errors'],
            "warnings": validation_result.get('warnings', [])
        }
        response["message"] = f"Biosample mapping validation {'passed' if validation_result['valid'] else 'failed'}."
        
        return response
        
    except Exception as e:
        logging.error(f"Error during biosample mapping validation: {e}")
        return {
            "valid": False,
            "errors": [f"Biosample mapping validation system error: {str(e)}"],
            "warnings": [],
            "message": "Validation failed due to system error."
        }


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()



