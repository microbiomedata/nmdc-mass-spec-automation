"""
Manager for handling conversation messages with the LLM.
They need to persist across multiple calls to the LLM API.
An instance of this class will need to be created for each conversation.
"""
from nmdc_dp_utils.llm.protocol_conversion.instructions import system_prompt as PROTOCOL_SYSTEM_PROMPT
from nmdc_dp_utils.llm.biosample_mapping.instructions import system_prompt as BIOSAMPLE_MAPPING_SYSTEM_PROMPT
import yaml

class ConversationManager:
    """
    Manages conversation messages for LLM interactions.
    Parameters
    ----------
    interaction_type (str): The type of conversation must be one of ('protocol_conversion', 'biosample_mapping').
    Attributes:
        messages (list): List of message dictionaries in the conversation.
    """
    def __init__(self, interaction_type: str):
        if interaction_type not in ['protocol_conversion', 'biosample_mapping']:
            raise ValueError("`interaction_type` not one of ('protocol_conversion', 'biosample_mapping')")
        self.messages = [{}]  # List to store the conversation messages
        # add the system prompt as the first message
        if interaction_type == "protocol_conversion":
            self.add_message(role="system", content=PROTOCOL_SYSTEM_PROMPT)
            self.add_protocol_desc_and_json_examples()
        if interaction_type == "biosample_mapping":
            self.add_message(role="system", content=BIOSAMPLE_MAPPING_SYSTEM_PROMPT)
            self.add_biosample_mapping_examples()

    def add_message(self, role: str, content: str):
        """
        Adds a message to the conversation.
        Parameters
        ----------
        role (str): The role of the message sender must be one of ('user', 'assistant', 'system').
        content (str): The content of the message.
        """
        self.messages.append({"role": role, "content": content})

    def add_protocol_description(self, description: str):
        """
        Adds a protocol description message to the conversation.
        Parameters
        ----------
        description (str) : The protocol description gathered from user input data.
        """
        self.add_message(role="system", content="Utilize this lab protocol provided by the user and convert it to a YAML outline:\n" + description)

    def add_protocol_desc_and_json_examples(self):
        """
        Add the currated description -> YAML examples to the context.
        """
        dirs = ["nmdc_dp_utils/llm/examples/example_1", "nmdc_dp_utils/llm/examples/example_2", "nmdc_dp_utils/llm/examples/example_3", "nmdc_dp_utils/llm/examples/example_4", "nmdc_dp_utils/llm/examples/example_5", "nmdc_dp_utils/llm/examples/example_6", "nmdc_dp_utils/llm/examples/example_7"]
        for dir in dirs: 
            with open(f"{dir}/extracted_text.txt", "r") as f:
                example = f.read()
            with open(f"{dir}/combined_outline.yaml", "r") as f:
                yaml = f.read()
            self.add_message(role="system", content="Here is an example of a lab protocol description that was translated to YAML:\n" + example )
            self.add_message(role="system", content="Here is the corresponding YAML outline:\n" + yaml )

    def add_biosample_mapping_examples(self):
        """
        Add curated biosample -> raw file -> processed sample mapping examples to the context.
        Uses example_8 which contains the vetted biosample mapping format.
        """
        dirs = ["nmdc_dp_utils/llm/examples/example_8"] # TODO KRH: add more examples when ready
        for dir in dirs: 
            with open(f"{dir}/biosample_attributes.csv", "r") as f:
                biosample_attributes = f.read()
            with open(f"{dir}/combined_yaml.yaml", "r") as f:
                material_processing_yaml = f.read()
            with open(f"{dir}/raw_files.csv", "r") as f:
                raw_files = f.read()
            with open(f"{dir}/combined_input.csv", "r") as f:
                mapped_output = f.read()
            self.add_message(role="system", content="Here is an example of biosample attributes for a study of interest:\n" + biosample_attributes )
            self.add_message(role="system", content="Here is the YAML outline describing the material processing steps for this study:\n" + material_processing_yaml )
            self.add_message(role="system", content="Here are the raw files associated with the study for our analysis type:\n" + raw_files )
            self.add_message(role="system", content="Here is the expected output mapping of raw files to processed samples based on the biosample attributes and material processing YAML:\n" + mapped_output )
