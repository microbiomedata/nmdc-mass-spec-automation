import sys
from pathlib import Path
import asyncio
from dotenv import load_dotenv
import time
import subprocess
import os

# Add workspace root to path
workspace_root = Path(__file__).parent.parent.parent.parent
if str(workspace_root) not in sys.path:
    sys.path.insert(0, str(workspace_root))

# Load environment variables
env_path = workspace_root / '.env'
load_dotenv(dotenv_path=env_path)

from nmdc_dp_utils.llm.llm_client import LLMClient
from nmdc_dp_utils.llm.llm_conversation_manager import ConversationManager
from nmdc_dp_utils.llm.biosample_mapping.pipeline import add_study_data_to_conversation


async def get_llm_generated_script(llm_client: LLMClient, conversation_obj: ConversationManager, 
                                   biosample_path: str, files_path: str, output_path: str):
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
- Write output CSV to: {output_path}
- Use the mapping logic we discussed (parse filenames, match to biosamples, determine protocols)

Provide ONLY the Python script code, no markdown blocks or explanations."""
    
    conversation_obj.add_message(role="user", content=prompt)
    
    print("  Waiting for LLM to generate mapping script...")
    start_time = time.time()
    response = await llm_client.get_response(conversation_obj.messages, timeout_seconds=300)
    elapsed = time.time() - start_time
    print(f"  ✓ Script generated ({elapsed:.1f}s)")
    
    return response


async def validate_and_fix_script(llm_client: LLMClient, conversation_obj: ConversationManager,
                                  script_path: str, output_path: str, 
                                  biosample_path: str, files_path: str, yaml_path: str,
                                  max_iterations: int = 3):
    """
    Execute script, validate output with MCP tool, and fix if needed.
    
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
    
    # Load validation context
    with open(biosample_path, 'r') as f:
        biosample_content = f.read()
    with open(files_path, 'r') as f:
        files_content = f.read()
    with open(yaml_path, 'r') as f:
        yaml_content = f.read()
    
    for iteration in range(max_iterations):
        print(f"\n  Iteration {iteration + 1}/{max_iterations}")
        
        # Execute the script
        print(f"    Executing script: {script_path}")
        try:
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=workspace_root
            )
            
            if result.returncode != 0:
                print(f"    ❌ Script execution failed:")
                print(f"    STDOUT: {result.stdout}")
                print(f"    STDERR: {result.stderr}")
                
                # Ask LLM to fix the script
                conversation_obj.add_message(
                    role="assistant",
                    content=f"Script execution failed with error:\n{result.stderr}\n\n{result.stdout}"
                )
                conversation_obj.add_message(
                    role="user",
                    content="Fix the script to resolve this error. Provide the complete corrected script."
                )
                
                print("    Asking LLM to fix the script...")
                fixed_script = await llm_client.get_response(conversation_obj.messages, timeout_seconds=300)
                
                # Clean up markdown if present
                if '```python' in fixed_script:
                    fixed_script = fixed_script.split('```python')[1].split('```')[0].strip()
                elif '```' in fixed_script:
                    fixed_script = fixed_script.split('```')[1].split('```')[0].strip()
                
                # Save fixed script
                with open(script_path, 'w') as f:
                    f.write(fixed_script)
                print("    Script updated")
                
                conversation_obj.add_message(role="assistant", content=fixed_script)
                continue
            
            print(f"    ✓ Script executed successfully")
            
            # Check if output file was created
            if not os.path.exists(output_path):
                print(f"    ❌ Output file not created: {output_path}")
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
            
            print(f"    ✓ Output file created: {output_path}")
            
            # Read the generated CSV
            with open(output_path, 'r') as f:
                generated_csv = f.read()
            
            # Validate with MCP tool
            print(f"    Validating CSV with MCP tool...")
            validation_result = validate_biosample_mapping_csv(
                csv_content=generated_csv,
                biosample_attributes_csv=biosample_content,
                raw_files_csv=files_content,
                material_processing_yaml=yaml_content
            )
            
            # Check validation result
            if validation_result.get('valid', False):
                print(f"    ✓ Validation passed!")
                
                # Check for warnings
                warnings = validation_result.get('warnings', [])
                unmapped_files = validation_result.get('unmapped_files', [])
                
                if warnings:
                    print(f"    ⚠️  Validation warnings: {len(warnings)}")
                    for warning in warnings[:3]:  # Show first 3 warnings
                        print(f"       - {warning}")
                    if len(warnings) > 3:
                        print(f"       ... and {len(warnings) - 3} more warnings")
                
                if unmapped_files:
                    print(f"    ℹ️  Unmapped files: {len(unmapped_files)} (saved separately)")
                    # Save unmapped files
                    unmapped_path = output_path.replace('.csv', '_unmapped_files.txt')
                    with open(unmapped_path, 'w') as f:
                        f.write('\n'.join(unmapped_files))
                
                return True
            else:
                # Validation failed
                errors = validation_result.get('errors', [])
                print(f"    ❌ Validation failed: {len(errors)} errors")
                for error in errors[:5]:  # Show first 5 errors
                    print(f"       - {error}")
                if len(errors) > 5:
                    print(f"       ... and {len(errors) - 5} more errors")
                
                # Ask LLM to fix the script based on validation errors
                error_summary = '\n'.join(errors[:10])  # Send first 10 errors
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
                
                print("    Asking LLM to fix validation errors...")
                fixed_script = await llm_client.get_response(conversation_obj.messages, timeout_seconds=300)
                
                if '```python' in fixed_script:
                    fixed_script = fixed_script.split('```python')[1].split('```')[0].strip()
                elif '```' in fixed_script:
                    fixed_script = fixed_script.split('```')[1].split('```')[0].strip()
                
                with open(script_path, 'w') as f:
                    f.write(fixed_script)
                print("    Script updated")
                
                conversation_obj.add_message(role="assistant", content=fixed_script)
                continue
                
        except subprocess.TimeoutExpired:
            print(f"    ❌ Script execution timed out (>30s)")
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
            print(f"    ❌ Validation error: {e}")
            import traceback
            traceback.print_exc()
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
    
    print(f"\n  ❌ Failed to generate valid script after {max_iterations} iterations")
    return False


if __name__ == "__main__":
    # Example usage for biosample mapping via code generation
    biosample_attributes_path = "nmdc_dp_utils/llm/examples/example_1/biosample_attributes.csv"
    raw_files_path = "nmdc_dp_utils/llm/examples/example_1/downloaded_files.csv"
    material_processing_yaml_path = "nmdc_dp_utils/llm/examples/example_1/combined_outline.yaml"
    
    # Output paths
    script_output_path = "nmdc_dp_utils/llm/examples/example_1/generated_mapping_script.py"
    csv_output_path = "nmdc_dp_utils/llm/examples/example_1/llm_generated_mapping_codegen.csv"
    
    # Read study ID if available
    study_id = None
    try:
        with open("nmdc_dp_utils/llm/examples/example_1/study_id.txt", "r") as f:
            study_id = f.read().strip()
    except FileNotFoundError:
        pass
    
    # Create client WITHOUT MCP servers (will add validation in future iteration)
    print("Initializing LLM client for code generation...")
    llm_client = LLMClient(mcp_servers=[])
    
    # Create conversation manager with CODE GENERATION prompt
    # We'll manually set up the context instead of using the default biosample_mapping type
    from nmdc_dp_utils.llm.biosample_mapping.instructions_codegen import system_prompt as CODEGEN_PROMPT
    
    print("Setting up conversation with code generation prompt...")
    conversation_obj = ConversationManager.__new__(ConversationManager)
    conversation_obj.messages = []
    conversation_obj.add_message(role="system", content=CODEGEN_PROMPT)
    
    # Add examples (using same minimal context approach)
    import pandas as pd
    import yaml
    
    dirs = ["nmdc_dp_utils/llm/examples/example_2", 
            "nmdc_dp_utils/llm/examples/example_4",
            "nmdc_dp_utils/llm/examples/example_6"]
    for dir in dirs:
        # Load and simplify YAML
        with open(f"{dir}/combined_outline.yaml", "r") as f:
            yaml_full = yaml.safe_load(f)
        
        yaml_minimal = {}
        for protocol_name, protocol_data in yaml_full.items():
            yaml_minimal[protocol_name] = {}
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
            if 'processedsamples' in protocol_data:
                yaml_minimal[protocol_name]['processedsamples'] = protocol_data['processedsamples']
        
        yaml_minimal_str = yaml.dump(yaml_minimal, default_flow_style=False, sort_keys=False)
        
        # Load combined_inputs_v2.csv as example
        with open(f"{dir}/combined_inputs_v2.csv", "r") as f:
            combined_inputs = f.read()
        
        conversation_obj.add_message(role="system", content="Here is the YAML outline describing the material processing steps:\n" + yaml_minimal_str)
        conversation_obj.add_message(role="system", content="Here is an example of the expected CSV mapping:\n" + combined_inputs)
    
    example_chars = sum(len(msg.get('content', '')) for msg in conversation_obj.messages if msg.get('role') == 'system')
    system_prompt_chars = len(conversation_obj.messages[0].get('content', ''))
    example_only_chars = example_chars - system_prompt_chars
    example_tokens = example_only_chars // 4
    print(f"  Examples loaded: ~{example_tokens:,} tokens")
    
    # Add study-specific data to the conversation
    print("Adding study data to conversation...")
    asyncio.run(add_study_data_to_conversation(
        conversation_obj=conversation_obj,
        biosample_attributes_path=biosample_attributes_path,
        raw_files_path=raw_files_path,
        material_processing_yaml_path=material_processing_yaml_path,
        study_id=study_id
    ))
    
    # Estimate input size
    total_chars = sum(len(msg.get('content', '')) for msg in conversation_obj.messages)
    estimated_tokens = total_chars // 4
    print(f"Study data added (~{estimated_tokens:,} tokens, ~{total_chars:,} characters)")
    
    # Get the mapping script from LLM
    print("\n🚀 Starting code generation approach...")
    print("=" * 70)
    
    try:
        script_code = asyncio.run(get_llm_generated_script(
            llm_client=llm_client,
            conversation_obj=conversation_obj,
            biosample_path=biosample_attributes_path,
            files_path=raw_files_path,
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
        
        print(f"\n✓ Script saved to: {script_output_path}")
        
        # Add script to conversation for potential fixes
        conversation_obj.add_message(role="assistant", content=script_code)
        
        # Execute and validate
        print("\n📝 Executing and validating script...")
        print("=" * 70)
        
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
            print(f"\n✅ SUCCESS! Final mapping saved to: {csv_output_path}")
            print(f"   Generated script saved to: {script_output_path}")
            
            # Show summary
            import pandas as pd
            df = pd.read_csv(csv_output_path)
            print(f"\n📊 Summary:")
            print(f"   Total files mapped: {len(df)}")
            print(f"   Files with biosamples: {df['biosample_id'].notna().sum()}")
            print(f"   Files without biosamples (QC/blank): {df['biosample_id'].isna().sum()}")
            
            # Check for unmapped files list
            unmapped_path = csv_output_path.replace('.csv', '_unmapped_files.txt')
            if os.path.exists(unmapped_path):
                with open(unmapped_path, 'r') as f:
                    unmapped_count = len(f.readlines())
                print(f"   Unmapped files saved to: {unmapped_path} ({unmapped_count} files)")
        else:
            print(f"\n❌ Failed to generate valid mapping")
            
    except Exception as e:
        print(f"\n❌ Error during code generation: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
