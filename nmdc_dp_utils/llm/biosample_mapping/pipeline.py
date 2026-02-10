from nmdc_dp_utils.llm.llm_client import LLMClient
from nmdc_dp_utils.llm.llm_conversation_manager import ConversationManager
import asyncio


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

    response = await llm_client.get_response(conversation_obj.messages)

    conversation_obj.add_message(role="assistant", content=response)
    conversation_obj.add_message(
        role="user", 
        content="Now, validate the generated CSV mapping using the `validate_biosample_mapping` tool. If there are any validation errors, please fix them and provide a corrected CSV that passes validation."
    )
    response = await llm_client.get_response(conversation_obj.messages)
    
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
    
    Parameters
    ----------
    conversation_obj (ConversationManager) : conversation manager to add data to
    biosample_attributes_path (str) : path to biosample attributes CSV file
    raw_files_path (str) : path to raw files CSV file
    material_processing_yaml_path (str) : path to material processing YAML file
    study_id (str) : optional study identifier
    """
    # Read biosample attributes
    with open(biosample_attributes_path, "r") as f:
        biosample_attributes = f.read()
    
    # Read raw files list
    with open(raw_files_path, "r") as f:
        raw_files = f.read()
    
    # Read material processing YAML
    with open(material_processing_yaml_path, "r") as f:
        material_processing_yaml = f.read()
    
    # Add to conversation
    conversation_obj.add_message(
        role="system",
        content=f"Biosample attributes for the study:\n{biosample_attributes}"
    )
    
    conversation_obj.add_message(
        role="system",
        content=f"Material processing protocol (YAML):\n{material_processing_yaml}"
    )
    
    conversation_obj.add_message(
        role="system",
        content=f"Raw mass spectrometry files:\n{raw_files}"
    )
    
    if study_id:
        conversation_obj.add_message(
            role="system",
            content=f"Study ID: {study_id}"
        )


if __name__ == "__main__":
    # Example usage for biosample mapping
    # Replace these paths with your actual study data
    biosample_attributes_path = "nmdc_dp_utils/llm/examples/example_8/biosample_attributes.csv"
    raw_files_path = "nmdc_dp_utils/llm/examples/example_8/raw_files.csv"
    material_processing_yaml_path = "nmdc_dp_utils/llm/examples/example_8/combined_yaml.yaml"
    
    # Read study ID if available
    study_id = None
    try:
        with open("nmdc_dp_utils/llm/examples/example_8/study_id.txt", "r") as f:
            study_id = f.read().strip()
    except FileNotFoundError:
        pass
    
    # Create the client that contains configuration information
    llm_client = LLMClient()
    
    # Create the conversation manager object that will handle adding the system prompt and examples
    conversation_obj = ConversationManager(interaction_type="biosample_mapping")
    
    # Add study-specific data to the conversation
    asyncio.run(add_study_data_to_conversation(
        conversation_obj=conversation_obj,
        biosample_attributes_path=biosample_attributes_path,
        raw_files_path=raw_files_path,
        material_processing_yaml_path=material_processing_yaml_path,
        study_id=study_id
    ))
    
    # Get the biosample mapping from the LLM (validation happens via MCP tool)
    response = asyncio.run(get_llm_biosample_mapping(
        llm_client=llm_client,
        conversation_obj=conversation_obj
    ))
    
    # Save the output CSV
    output_path = "nmdc_dp_utils/llm/examples/example_8/llm_generated_mapping.csv"
    with open(output_path, "w") as f:
        f.write(response)
    
    print(f"LLM generated biosample mapping saved to: {output_path}")
