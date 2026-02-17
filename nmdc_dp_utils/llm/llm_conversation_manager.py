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
        Uses ONLY combined_inputs_v2.csv which already contains raw files, biosamples, and mapping.
        Also includes simplified YAML protocol outline.
        """
        import pandas as pd
        import yaml as yaml_lib
        
        dirs = ["nmdc_dp_utils/llm/examples/example_3"]
        
        for dir in dirs:
            # Load and simplify YAML (only description, has_input, has_output, processedsamples)
            with open(f"{dir}/combined_outline.yaml", "r") as f:
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
            
            # Load combined_inputs_v2.csv which has everything
            with open(f"{dir}/combined_inputs_v2.csv", "r") as f:
                combined_inputs = f.read()
            
            self.add_message(role="system", content="Here is the YAML outline describing the material processing steps:\n" + yaml_minimal_str)
            self.add_message(role="system", content="Here is an example of the expected CSV mapping (includes raw files, biosamples, and processed samples):\n" + combined_inputs)
        
        # Calculate and print token estimate for examples
        example_chars = sum(len(msg.get('content', '')) for msg in self.messages if msg.get('role') == 'system')
        # Subtract system prompt from total (it's in messages[1])
        system_prompt_chars = len(self.messages[1].get('content', '')) if len(self.messages) > 1 else 0
        example_only_chars = example_chars - system_prompt_chars
        example_tokens = example_only_chars // 4
        print(f"  Examples loaded: ~{example_tokens:,} tokens (~{example_only_chars:,} characters)")
