from llm_client import LLMClient
from llm_conversation_manager import ConversationManager
import asyncio
import logging
from nmdc_ms_metadata_gen.validate_yaml_outline import validate_yaml_outline
import yaml

def clean_yaml_response(response: str) -> str:
    """Remove markdown code fences from LLM response."""
    # Remove ```yaml and ``` markers
    response = response.strip()
    if response.startswith("```yaml"):
        response = response[7:]  # Remove ```yaml
    elif response.startswith("```"):
        response = response[3:]  # Remove ```
    if response.endswith("```"):
        response = response[:-3]  # Remove trailing ```
    return response.strip()


def validate_generated_yaml(yaml_outline: str) -> dict:
    """
    Validate the provided YAML outline against NMDC schema.
    You must call this function at least once after generating the outline to ensure compliance.

    Parameters
    ----------
        yaml_outline (str): The YAML outline as a string.

    Returns
    -------
    dict: Validation results including errors and warnings.
    """
    clean_yaml_res = clean_yaml_response(yaml_outline)
    logging.info("Within validate_yaml_outline tool.")
    # save the yaml outline to a temporary file
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".yaml") as temp_yaml_file:
        temp_yaml_file.write(clean_yaml_res)
        temp_yaml_file_path = temp_yaml_file.name
    logging.info(f"Temporary YAML outline saved to: {temp_yaml_file_path}")
    validation_results = validate_yaml_outline(temp_yaml_file_path, test=True)
    logging.info(f"Validation results: {validation_results}")
    return validation_results

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
    # conversation_obj.add_message(role="user", content="Now, validate the generated YAML outline against the NMDC schema using the `validate_generated_yaml` tool. If there are any validation errors, please fix them and provide a corrected YAML outline that passes validation.")
    # response = await llm_client.get_response(conversation_obj.messages)
    # validation_results = validate_generated_yaml(response)
    # while all('errors' in result for result in validation_results):
    #     # there are validation errors, ask the LLM to fix them
    #     error_messages = "\n".join([f"- {error}" for error in validation_results["errors"]])
    #     conversation_obj.add_message(role="user", content=f"The generated YAML outline has the following errors:\n{error_messages}\nPlease fix the YAML outline to comply with the NMDC schema.")
    #     response = await llm_client.get_response(conversation_obj.messages)
    #     conversation_obj.add_message(role="assistant", content=response)
    #     validation_results = validate_generated_yaml(response)
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
    conversation_obj.add_protocol_desc_and_json_examples()
    response = asyncio.run(get_llm_yaml_outline(llm_client=llm_client, conversation_obj=conversation_obj))
    # save as yaml 
    output_path = "nmdc_dp_utils/llm/llm_protocol_context/example_4/llm_generated_outline.yaml"
    with open(output_path, "w") as f:
        f.write(response)
    print(f"LLM generated YAML outline saved to: {output_path}")


