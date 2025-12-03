"""
Manager for handling conversation messages with the LLM.
They need to persist across multiple calls to the LLM API.
An instance of this class will need to be created for each conversation.
"""
from llm.llm_protocol_context.instructions import system_prompt as PROTOCOL_SYSTEM_PROMPT

class ConversationManager:
    """
    Manages conversation messages for LLM interactions.
    Parameters
    ----------
    type (str): The type of conversation must be one of ('protocol_conversion', 'biosample_mapping').
    Attributes:
        messages (list): List of message dictionaries in the conversation.
    """
    def __init__(self, interaction_type: str):
        self.messages = [{}]  # List to store the conversation messages
        # add the system prompt as the first message
        if interaction_type == "protocol_conversion":
            self.add_message(role="system", content=PROTOCOL_SYSTEM_PROMPT)
            self.add_protocol_desc_and_json_examples()

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
        """
        self.add_message(role="system", content="Utilize this lab protocol provided by the user and convert it to a YAML outline:\n" + description)

    def add_protocol_desc_and_json_examples(self):
        """
        Add the currated description -> YAML examples to the context.
        """
        dir = []
        for example in dir: 
            self.add_message(role="system", content="Here is an example of a lab protocol description that was translated to YAML:\n" + example )

    