"""
Biosample Mapping Pipeline - LLM-assisted code generation for mapping raw files to biosamples

This module provides automated biosample-to-raw-file mapping through LLM-generated Python scripts.
The pipeline:
1. Loads study data (biosamples, raw files, material processing YAML)
2. Asks LLM to generate a mapping script
3. Executes and validates the generated script
4. Iteratively fixes errors until validation passes

The validation ensures:
- Biosample IDs follow NMDC format and exist in biosample attributes
- Protocol IDs match those defined in the YAML
- Processed sample placeholders are valid
- All mappable raw files are included
"""

import sys
from pathlib import Path
import asyncio
from dotenv import load_dotenv
import time
import logging

# Add workspace root to path to allow imports when running as script
workspace_root = Path(__file__).parent.parent.parent.parent
if str(workspace_root) not in sys.path:
    sys.path.insert(0, str(workspace_root))

# Load environment variables from .env file
env_path = workspace_root / '.env'
load_dotenv(dotenv_path=env_path)

from nmdc_dp_utils.llm.llm_client import LLMClient
from nmdc_dp_utils.llm.llm_conversation_manager import ConversationManager
import subprocess

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


async def get_llm_generated_script(llm_client: LLMClient, conversation_obj: ConversationManager, 
                                   biosample_path: str, files_path: str, yaml_path: str, output_path: str):
    """
    Get LLM to generate a Python script that does the mapping.
    
    Parameters
    ----------
    llm_client : LLMClient
        LLM client instance
    conversation_obj : ConversationManager
        Conversation manager with context
    biosample_path : str
        Path to biosample CSV (for script to read)
    files_path : str
        Path to raw files CSV (for script to read)
    yaml_path : str
        Path to material processing YAML (for script to read)
    output_path : str
        Path where script should write output CSV
    
    Returns
    -------
    str : Python script code
    """
    # Determine column name from files CSV
    import pandas as pd
    files_df = pd.read_csv(files_path)
    if 'raw_data_file_name' in files_df.columns:
        column_name = 'raw_data_file_name'
    elif 'file_name' in files_df.columns:
        column_name = 'file_name'
    else:
        column_name = files_df.columns[0]
    
    prompt = f"""Generate a Python script that maps the raw files to biosamples and processed samples.

The script should:
- Read biosamples from: {biosample_path}
- Read raw files from: {files_path} (column: {column_name})
- Read material processing YAML from: {yaml_path}
- Write output CSV to: {output_path}
- Use the mapping logic we discussed (parse filenames, match to biosamples, determine protocols)

IMPORTANT: Use these EXACT file paths in your script. Do not guess or change the paths.

Provide ONLY the Python script code, no markdown blocks or explanations."""
    
    conversation_obj.add_message(role="user", content=prompt)
    
    logging.info("Waiting for LLM to generate mapping script...")
    start_time = time.time()
    response = await llm_client.get_response(conversation_obj.messages, timeout_seconds=300)
    elapsed = time.time() - start_time
    logging.info(f"Script generated ({elapsed:.1f}s)")
    
    return response


async def validate_and_fix_script(llm_client: LLMClient, conversation_obj: ConversationManager,
                                  script_path: str, output_path: str, 
                                  biosample_path: str, files_path: str, yaml_path: str,
                                  max_iterations: int = 3):
    """
    Execute script, validate output, and fix if needed.
    
    Parameters
    ----------
    llm_client : LLMClient
        LLM client instance
    conversation_obj : ConversationManager
        Conversation manager
    script_path : str
        Path to the generated script
    output_path : str
        Path to the output CSV
    biosample_path : str
        Path to biosample attributes CSV (for validation)
    files_path : str
        Path to raw files CSV (for validation)
    yaml_path : str
        Path to YAML file (for validation)
    max_iterations : int
        Max number of fix attempts
    
    Returns
    -------
    bool : True if validation passed
    """
    # Import validation function
    from nmdc_dp_utils.llm.biosample_mapping.validation import validate_biosample_mapping_csv
    import os
    
    # Load validation context
    with open(biosample_path, 'r') as f:
        biosample_content = f.read()
    with open(files_path, 'r') as f:
        files_content = f.read()
    with open(yaml_path, 'r') as f:
        yaml_content = f.read()
    
    for iteration in range(max_iterations):
        logging.info(f"Iteration {iteration + 1}/{max_iterations}")
        
        # Execute the script
        logging.info(f"Executing script: {script_path}")
        try:
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=workspace_root
            )
            
            if result.returncode != 0:
                logging.error("Script execution failed")
                logging.debug(f"STDOUT: {result.stdout}")
                logging.error(f"STDERR: {result.stderr}")
                
                # Ask LLM to fix the script
                conversation_obj.add_message(
                    role="assistant",
                    content=f"Script execution failed with error:\n{result.stderr}\n\n{result.stdout}"
                )
                conversation_obj.add_message(
                    role="user",
                    content="Fix the script to resolve this error. Provide the complete corrected script."
                )
                
                logging.info("Asking LLM to fix the script...")
                fixed_script = await llm_client.get_response(conversation_obj.messages, timeout_seconds=300)
                
                # Clean up markdown if present
                if '```python' in fixed_script:
                    fixed_script = fixed_script.split('```python')[1].split('```')[0].strip()
                elif '```' in fixed_script:
                    fixed_script = fixed_script.split('```')[1].split('```')[0].strip()
                
                # Save fixed script
                with open(script_path, 'w') as f:
                    f.write(fixed_script)
                logging.info("Script updated")
                
                conversation_obj.add_message(role="assistant", content=fixed_script)
                continue
            
            logging.info("Script executed successfully")
            
            # Check if output file was created
            if not os.path.exists(output_path):
                logging.error(f"Output file not created: {output_path}")
                conversation_obj.add_message(
                    role="user",
                    content=f"Script ran but did not create output file: {output_path}. Fix the script to ensure it creates this file."
                )
                fixed_script = await llm_client.get_response(conversation_obj.messages, timeout_seconds=300)
                
                if '```python' in fixed_script:
                    fixed_script = fixed_script.split('```python')[1].split('```')[0].strip()
                elif '```' in fixed_script:
                    fixed_script = fixed_script.split('```')[1].split('```')[0].strip()
                
                with open(script_path, 'w') as f:
                    f.write(fixed_script)
                conversation_obj.add_message(role="assistant", content=fixed_script)
                continue
            
            logging.info(f"Output file created: {output_path}")
            
            # Read the generated CSV
            with open(output_path, 'r') as f:
                generated_csv = f.read()
            
            # Validate
            logging.info("Validating CSV...")
            validation_result = validate_biosample_mapping_csv(
                csv_content=generated_csv,
                biosample_attributes_csv=biosample_content,
                raw_files_csv=files_content,
                material_processing_yaml=yaml_content
            )
            
            # Check validation result
            if validation_result.get('valid', False):
                logging.info("Validation passed!")
                
                # Check for warnings
                warnings = validation_result.get('warnings', [])
                unmapped_files = validation_result.get('unmapped_files', [])
                
                if warnings:
                    logging.warning(f"Validation warnings: {len(warnings)}")
                    for warning in warnings[:3]:
                        logging.warning(f"  {warning}")
                    if len(warnings) > 3:
                        logging.warning(f"  ... and {len(warnings) - 3} more warnings")
                
                if unmapped_files:
                    logging.info(f"Unmapped files: {len(unmapped_files)} (saved separately)")
                    unmapped_path = output_path.replace('.csv', '_unmapped_files.txt')
                    with open(unmapped_path, 'w') as f:
                        f.write('\n'.join(unmapped_files))
                
                return True
            else:
                # Validation failed
                errors = validation_result.get('errors', [])
                logging.error(f"Validation failed: {len(errors)} errors")
                for error in errors[:5]:
                    logging.error(f"  {error}")
                if len(errors) > 5:
                    logging.error(f"  ... and {len(errors) - 5} more errors")
                
                # Ask LLM to fix the script
                error_summary = '\n'.join(errors[:10])
                conversation_obj.add_message(
                    role="user",
                    content=f"""The generated CSV failed validation with these errors:

{error_summary}

Fix the script to resolve these validation errors. The validation checks:
- Biosample IDs exist in biosample_attributes.csv and follow NMDC format
- Biosample names match the biosample IDs
- Processed sample placeholders exist in the YAML
- Protocol IDs match top-level protocols in the YAML
- All raw files are mapped

Provide the complete corrected script."""
                )
                
                logging.info("Asking LLM to fix validation errors...")
                fixed_script = await llm_client.get_response(conversation_obj.messages, timeout_seconds=300)
                
                if '```python' in fixed_script:
                    fixed_script = fixed_script.split('```python')[1].split('```')[0].strip()
                elif '```' in fixed_script:
                    fixed_script = fixed_script.split('```')[1].split('```')[0].strip()
                
                with open(script_path, 'w') as f:
                    f.write(fixed_script)
                logging.info("Script updated")
                
                conversation_obj.add_message(role="assistant", content=fixed_script)
                continue
                
        except subprocess.TimeoutExpired:
            logging.error("Script execution timed out (>30s)")
            conversation_obj.add_message(
                role="user",
                content="Script execution timed out. Make the script more efficient."
            )
            fixed_script = await llm_client.get_response(conversation_obj.messages, timeout_seconds=300)
            
            if '```python' in fixed_script:
                fixed_script = fixed_script.split('```python')[1].split('```')[0].strip()
            elif '```' in fixed_script:
                fixed_script = fixed_script.split('```')[1].split('```')[0].strip()
            
            with open(script_path, 'w') as f:
                f.write(fixed_script)
            conversation_obj.add_message(role="assistant", content=fixed_script)
            continue
        except Exception as e:
            logging.error(f"Validation error: {e}")
            import traceback
            logging.debug(traceback.format_exc())
            conversation_obj.add_message(
                role="user",
                content=f"Validation encountered an error: {e}. Fix the script to produce valid output."
            )
            fixed_script = await llm_client.get_response(conversation_obj.messages, timeout_seconds=300)
            
            if '```python' in fixed_script:
                fixed_script = fixed_script.split('```python')[1].split('```')[0].strip()
            elif '```' in fixed_script:
                fixed_script = fixed_script.split('```')[1].split('```')[0].strip()
            
            with open(script_path, 'w') as f:
                f.write(fixed_script)
            conversation_obj.add_message(role="assistant", content=fixed_script)
            continue
    
    logging.error(f"Failed to generate valid script after {max_iterations} iterations")
    return False


async def add_study_data_to_conversation(
    conversation_obj: ConversationManager,
    biosample_attributes_path: str,
    raw_files_path: str,
    material_processing_yaml_path: str,
    additional_context_path: str = None
):
    """
    Add study-specific data to the conversation context.
    Applies token reduction by filtering to essential columns only.
    
    Parameters
    ----------
    conversation_obj (ConversationManager) : conversation manager to add data to
    biosample_attributes_path (str) : path to biosample attributes CSV file
    raw_files_path (str) : path to raw files CSV file
    material_processing_yaml_path (str) : path to material processing YAML file
    additional_context_path (str) : optional path to additional context text file with naming conventions or protocol details
    """
    import pandas as pd
    import yaml as yaml_lib
    
    # Load and filter biosample attributes (only id and name)
    biosample_df = pd.read_csv(biosample_attributes_path)
    if 'id' in biosample_df.columns and 'name' in biosample_df.columns:
        biosample_minimal = biosample_df[['id', 'name']].to_csv(index=False)
    else:
        biosample_minimal = biosample_df[['id']].to_csv(index=False) if 'id' in biosample_df.columns else biosample_df.to_csv(index=False)
    
    # Load and filter raw files (only file_name)
    files_df = pd.read_csv(raw_files_path)
    if 'file_name' in files_df.columns:
        files_minimal = files_df[['file_name']].to_csv(index=False)
    elif 'raw_data_file_name' in files_df.columns:
        files_minimal = files_df[['raw_data_file_name']].to_csv(index=False)
    else:
        files_minimal = files_df.to_csv(index=False)
    
    # Load and simplify YAML (only description, has_input, has_output, processedsamples)
    with open(material_processing_yaml_path, "r") as f:
        yaml_full = yaml_lib.safe_load(f)
    
    yaml_minimal = {}
    for protocol_name, protocol_data in yaml_full.items():
        yaml_minimal[protocol_name] = {}
        
        # Simplify steps - keep only description, has_input, has_output
        if 'steps' in protocol_data:
            yaml_minimal[protocol_name]['steps'] = []
            for step in protocol_data['steps']:
                simplified_step = {}
                for step_name, step_data in step.items():
                    for process_type, process_details in step_data.items():
                        simplified_process = {}
                        if 'description' in process_details:
                            simplified_process['description'] = process_details['description']
                        if 'has_input' in process_details:
                            simplified_process['has_input'] = process_details['has_input']
                        if 'has_output' in process_details:
                            simplified_process['has_output'] = process_details['has_output']
                        simplified_step[step_name] = {process_type: simplified_process}
                yaml_minimal[protocol_name]['steps'].append(simplified_step)
        
        # Keep processedsamples as-is (needed for validation)
        if 'processedsamples' in protocol_data:
            yaml_minimal[protocol_name]['processedsamples'] = protocol_data['processedsamples']
    
    yaml_minimal_str = yaml_lib.dump(yaml_minimal, default_flow_style=False, sort_keys=False)
    
    # Add to conversation
    conversation_obj.add_message(
        role="system",
        content=f"Biosample attributes for the study:\n{biosample_minimal}"
    )
    
    conversation_obj.add_message(
        role="system",
        content=f"Material processing protocol (YAML):\n{yaml_minimal_str}"
    )
    
    conversation_obj.add_message(
        role="system",
        content=f"Raw mass spectrometry files:\n{files_minimal}"
    )
    
    if additional_context_path:
        with open(additional_context_path, "r") as f:
            additional_context = f.read()
        conversation_obj.add_message(
            role="system",
            content=f"Additional context (experimental methods and sample processing):\n{additional_context}"
        )


if __name__ == "__main__":
    import os
    
    # Example usage for biosample mapping via code generation approach
    biosample_attributes_path = "nmdc_dp_utils/llm/examples/example_1/biosample_attributes.csv"
    raw_files_path = "nmdc_dp_utils/llm/examples/example_1/downloaded_files.csv"
    material_processing_yaml_path = "nmdc_dp_utils/llm/examples/example_1/combined_outline.yaml"
    additional_context_path = "nmdc_dp_utils/llm/examples/example_1/additional_mapping_context.txt"
    
    # Output paths
    script_output_path = "nmdc_dp_utils/llm/examples/example_1/llm_generated_mapping_script_20260317.py"
    csv_output_path = "nmdc_dp_utils/llm/examples/example_1/llm_generated_mapping_20260317.csv"
    
    # Check if additional context file exists
    if not os.path.exists(additional_context_path):
        additional_context_path = None
    
    # Create LLM client (no MCP servers needed for code generation)
    logging.info("Initializing LLM client for code generation...")
    llm_client = LLMClient(mcp_servers=[])
    
    # Create conversation manager with code generation prompt and examples
    logging.info("Setting up conversation with code generation prompt and examples...")
    conversation_obj = ConversationManager(interaction_type="biosample_mapping")
    
    # Add study-specific data to the conversation
    logging.info("Adding study data to conversation...")
    asyncio.run(add_study_data_to_conversation(
        conversation_obj=conversation_obj,
        biosample_attributes_path=biosample_attributes_path,
        raw_files_path=raw_files_path,
        material_processing_yaml_path=material_processing_yaml_path,
        additional_context_path=additional_context_path
    ))
       
    # Get the mapping script from LLM
    logging.info("Generating mapping script via code generation approach...")
    
    script_code = asyncio.run(get_llm_generated_script(
        llm_client=llm_client,
        conversation_obj=conversation_obj,
        biosample_path=biosample_attributes_path,
        files_path=raw_files_path,
        yaml_path=material_processing_yaml_path,
        output_path=csv_output_path
    ))
        
    # Clean up the script (remove markdown blocks if present)
    if '```python' in script_code:
        script_code = script_code.split('```python')[1].split('```')[0].strip()
    elif '```' in script_code:
        script_code = script_code.split('```')[1].split('```')[0].strip()
    
    # Save the script
    with open(script_output_path, 'w') as f:
        f.write(script_code)
        
    logging.info(f"Script saved to: {script_output_path}")
    
    # Add script to conversation for potential fixes
    conversation_obj.add_message(role="assistant", content=script_code)
    
    # Execute and validate
    logging.info("Executing and validating script...")
    
    success = asyncio.run(validate_and_fix_script(
        llm_client=llm_client,
        conversation_obj=conversation_obj,
        script_path=script_output_path,
        output_path=csv_output_path,
        biosample_path=biosample_attributes_path,
        files_path=raw_files_path,
        yaml_path=material_processing_yaml_path,
        max_iterations=3
    ))

    if success:
        logging.info(f"LLM generated code saved to: {script_output_path} and associated CSV mapping saved to: {csv_output_path}")
    else:
        logging.error("Failed to generate a valid mapping script after multiple attempts. Please review the conversation and outputs for debugging.")

