from llm_client import LLMClient
from llm_conversation_manager import ConversationManager
import asyncio

async def get_llm_yaml_outline(llm_client:LLMClient, conversation_obj:ConversationManager):
    """
    Get the LLM generated YAML outline.
    
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
    protocol_description_path = "nmdc_dp_utils/llm/llm_protocol_context/example_4/extracted_text.txt"
    with open(protocol_description_path, "r") as f:
        protocol_description = f.read()

    # create the client that contains configuration information
    llm_client = LLMClient()
    # create the conversation manager object that will handle adding the system prompt and examples
    conversation_obj = ConversationManager(interaction_type="protocol_conversion")
    # use the converation obj to add the protocol decsription
    conversation_obj.add_protocol_description(description=protocol_description)
    response = asyncio.run(get_llm_yaml_outline(llm_client=llm_client, conversation_obj=conversation_obj))
    # save as yaml 
    output_path = "nmdc_dp_utils/llm/llm_protocol_context/example_4/llm_generated_outline.yaml"
    with open(output_path, "w") as f:
        f.write(response)
    print(f"LLM generated YAML outline saved to: {output_path}")


