import sys
from pathlib import Path
import asyncio
from dotenv import load_dotenv
import time

# Add workspace root to path to allow imports when running as script
workspace_root = Path(__file__).parent.parent.parent.parent
if str(workspace_root) not in sys.path:
    sys.path.insert(0, str(workspace_root))

# Load environment variables from .env file
env_path = workspace_root / '.env'
load_dotenv(dotenv_path=env_path)

from nmdc_dp_utils.llm.llm_client import LLMClient
from nmdc_dp_utils.llm.llm_conversation_manager import ConversationManager


async def get_llm_biosample_mapping(llm_client: LLMClient, conversation_obj: ConversationManager):
    """
    Get the LLM generated biosample to raw file to processed sample mapping.
    
    Parameters
    ----------
    llm_client (LLMClient) : object that holds LLM configuration information.
    conversation_obj (ConversationManager) : object that contains current session conversation information.
    
    Returns
    -------
    str : CSV mapping of raw files to biosamples and processed samples
    """
    conversation_obj.add_message(
        role="user", 
        content="Generate the CSV mapping of raw files to biosamples and processed samples."
    )

    print("  Waiting for LLM to generate CSV (this may take 2-5 minutes for large datasets)...")
    start_time = time.time()
    response = await llm_client.get_response(conversation_obj.messages, timeout_seconds=600)  # 10 min timeout
    elapsed = time.time() - start_time
    print(f"  ✓ CSV generated ({elapsed:.1f}s)")
    
    conversation_obj.add_message(role="assistant", content=response)
    conversation_obj.add_message(
        role="user", 
        content="Now, validate the generated CSV mapping using the `validate_biosample_mapping` tool. If there are any validation errors, please fix them and provide a corrected CSV that passes validation."
    )
    
    print("  Validating with MCP tool (this may take 1-3 minutes)...")
    start_time = time.time()
    response = await llm_client.get_response(conversation_obj.messages, timeout_seconds=600)  # 10 min timeout
    elapsed = time.time() - start_time
    print(f"  ✓ Validation complete ({elapsed:.1f}s)")
    
    return response


async def add_study_data_to_conversation(
    conversation_obj: ConversationManager,
    biosample_attributes_path: str,
    raw_files_path: str,
    material_processing_yaml_path: str,
    study_id: str = None
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
    study_id (str) : optional study identifier
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
    
    if study_id:
        conversation_obj.add_message(
            role="system",
            content=f"Study ID: {study_id}"
        )


if __name__ == "__main__":
    import os
    
    # Example usage for biosample mapping
    # Replace these paths with your actual study data
    biosample_attributes_path = "nmdc_dp_utils/llm/examples/example_8/biosample_attributes.csv"
    raw_files_path = "nmdc_dp_utils/llm/examples/example_8/downloaded_files.csv"
    material_processing_yaml_path = "nmdc_dp_utils/llm/examples/example_8/combined_outline.yaml"
    
    # Read study ID if available
    study_id = None
    try:
        with open("nmdc_dp_utils/llm/examples/example_8/study_id.txt", "r") as f:
            study_id = f.read().strip()
    except FileNotFoundError:
        pass
    
    # Set up biosample mapping MCP server
    biosample_mcp_server_path = os.path.join(
        os.path.dirname(__file__), 
        "mcp_server.py"
    )
    
    # Read input data for MCP server validation context
    print("Loading study data...")
    with open(biosample_attributes_path, "r") as f:
        biosample_attributes_content = f.read()
    with open(raw_files_path, "r") as f:
        raw_files_content = f.read()
    with open(material_processing_yaml_path, "r") as f:
        material_processing_yaml_content = f.read()
    
    # Import and configure the MCP server validation context
    from nmdc_dp_utils.llm.biosample_mapping import mcp_server
    mcp_server.set_validation_context(
        biosample_attributes=biosample_attributes_content,
        raw_files=raw_files_content,
        material_processing_yaml=material_processing_yaml_content
    )
    print("MCP server validation context set")
    
    # Create the client with biosample mapping MCP server
    llm_client = LLMClient(mcp_servers=[biosample_mcp_server_path])
    print("LLM client initialized")
    
    # Create the conversation manager object that will handle adding the system prompt and examples
    conversation_obj = ConversationManager(interaction_type="biosample_mapping")
    print("Conversation manager initialized")
    
    # Add study-specific data to the conversation
    print("Adding study data to conversation...")
    asyncio.run(add_study_data_to_conversation(
        conversation_obj=conversation_obj,
        biosample_attributes_path=biosample_attributes_path,
        raw_files_path=raw_files_path,
        material_processing_yaml_path=material_processing_yaml_path,
        study_id=study_id
    ))
    
    # Estimate input size for user
    total_chars = sum(len(msg.get('content', '')) for msg in conversation_obj.messages)
    estimated_tokens = total_chars // 4  # Rough estimate: 1 token ≈ 4 chars
    print(f"Study data added to conversation (~{estimated_tokens:,} tokens, ~{total_chars:,} characters)")
    if estimated_tokens > 100000:
        print("  ⚠️ Large input detected - LLM may take 5-10 minutes to respond")
    
    # Get the biosample mapping from the LLM (validation happens via MCP tool)
    print("\nStarting LLM biosample mapping generation...")
    try:
        response = asyncio.run(get_llm_biosample_mapping(
            llm_client=llm_client,
            conversation_obj=conversation_obj
        ))
    except Exception as e:
        print(f"\n❌ Error during LLM processing: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
    
    # Save the output CSV
    output_path = "nmdc_dp_utils/llm/examples/example_8/llm_generated_mapping.csv"
    with open(output_path, "w") as f:
        f.write(response)
    
    print(f"\n✅ LLM generated biosample mapping saved to: {output_path}")
    print("Check unmapped_files.txt (if created) for files that could not be mapped.")
