from llm.llm_client import LLMClient
from llm.llm_conversation_manager import ConversationManager


def get_llm_yaml_outline(llm_client:LLMClient, conversation_obj:ConversationManager):
    """
    Get the LLM generated YAML outline.
    
    Parameters
    ----------
    llm_client (LLMClient) : object that hold LLM configuration information.
    conversation_obj (ConversationManager) : object that contains currrent session conversation information.
    """
    conversation_obj.add_message(role="user", content="Generate the YAML outline for the provided protocol description.")

    response = llm_client.client.chat.completions.create(
        model=llm_client.model,
        messages=conversation_obj.messages
    )
    conversation_obj.add_message(role="assistant", content=response.choices[0])
    return response


if __name__ == "__main__":
    # read in the protocol description
    protocol_description_path = "path/to/description"
    with open(protocol_description_path, "r") as f:
        protocol_description = f.read()

    # create the client that contains configuration information
    llm_client = LLMClient()
    # create the conversation manager object that will handle adding the system prompt and examples
    conversation_obj = ConversationManager(interaction_type="protocol_conversion")
    # use the converation obj to add the protocol decsription
    conversation_obj.add_protocol_description(description=protocol_description)
    response = get_llm_yaml_outline(llm_client=llm_client, conversation_obj=conversation_obj)


