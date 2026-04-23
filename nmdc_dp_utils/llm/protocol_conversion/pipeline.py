from nmdc_dp_utils.llm.llm_client import LLMClient
from nmdc_dp_utils.llm.llm_conversation_manager import ConversationManager
from agents.mcp import create_static_tool_filter
import asyncio

async def get_llm_yaml_outline(llm_client:LLMClient, conversation_obj:ConversationManager):
    """
    Get the LLM generated YAML outline.
    
    This function uses the shared MCP server with protocol conversion tools:
    - get_protocol_schema_context: Get NMDC schema definitions
    - validate_generated_yaml: Validate generated YAML
    
    Parameters
    ----------
    llm_client (LLMClient) : object that hold LLM configuration information.
    conversation_obj (ConversationManager) : object that contains currrent session conversation information.
    """
    conversation_obj.add_message(role="user", content="Generate the YAML outline for the provided protocol description.")

    response = await llm_client.get_response(conversation_obj.messages)

    conversation_obj.add_message(role="assistant", content=response)
    conversation_obj.add_message(role="user", content="Now, validate the generated YAML outline against the NMDC schema using the `validate_generated_yaml` tool. If there are any validation errors, please fix them and provide a corrected YAML outline that passes validation.")
    response = await llm_client.get_response(conversation_obj.messages)
    return response


if __name__ == "__main__":
    # read in the protocol description
    protocol_description_path = "nmdc_dp_utils/llm/examples/example_4/extracted_text.txt"
    with open(protocol_description_path, "r") as f:
        protocol_description = f.read()

    # Create tool filter for protocol conversion tools only
    protocol_filter = create_static_tool_filter(
        allowed_tool_names=["get_protocol_schema_context", "validate_generated_yaml"]
    )
    
    # create the client that contains configuration information
    llm_client = LLMClient(mcp_tool_filter=protocol_filter)
    # create the conversation manager object that will handle adding the system prompt and examples
    conversation_obj = ConversationManager(interaction_type="protocol_conversion")
    # use the converation obj to add the protocol decsription
    conversation_obj.add_protocol_description(description=protocol_description)
    response = asyncio.run(get_llm_yaml_outline(llm_client=llm_client, conversation_obj=conversation_obj))
    # save as yaml 
    output_path = "nmdc_dp_utils/llm/examples/example_4/llm_generated_outline.yaml"
    with open(output_path, "w") as f:
        f.write(response)
    print(f"LLM generated YAML outline saved to: {output_path}")


