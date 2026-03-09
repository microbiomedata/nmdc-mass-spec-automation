# Biosample Mapping with LLM (Code Generation Approach)

> **Parent Module**: [LLM Module](../README.md)

Maps raw mass spectrometry files to their corresponding biosamples and processed samples using an LLM to generate a Python mapping script.

## Approach

This module uses **code generation** for efficient biosample mapping:
1. Study data (biosamples, raw files, material processing YAML) is added to the LLM conversation context
2. LLM analyzes actual file naming patterns from the study's raw files list
3. LLM reviews the study-specific material processing YAML to identify protocols
4. LLM generates a Python script that implements mapping logic ONLY for patterns/protocols found in the study data
5. Script executes to map all files (typically < 1 second)
6. Output CSV is validated programmatically against NMDC format requirements and study YAML
7. If validation fails, LLM iteratively fixes the script based on error feedback and re-runs

**Performance**: ~30-60 seconds total for hundreds of files

**Key Feature**: The system prevents template-based copying by constraining the LLM to analyze only the actual study data provided, not patterns from examples.

## Files

- **[instructions.py](instructions.py)** - System prompt for Python script generation
- **[pipeline.py](pipeline.py)** - Main workflow (`get_llm_generated_script()`, `validate_and_fix_script()`, `add_study_data_to_conversation()`)
- **[validation.py](validation.py)** - Programmatic validation logic
- **[mcp_server.py](mcp_server.py)** - MCP tool for CSV validation (optional, used by older approach)

## Usage

See [parent README](../README.md#shared-components) for `LLMClient` and `ConversationManager` setup.

### Basic Usage

The simplest way to use this module is to run the complete pipeline:

```python
from nmdc_dp_utils.llm.llm_client import LLMClient
from nmdc_dp_utils.llm.llm_conversation_manager import ConversationManager
from nmdc_dp_utils.llm.biosample_mapping.pipeline import (
    add_study_data_to_conversation,
    get_llm_generated_script,
    validate_and_fix_script
)
import asyncio

# 1. Initialize LLM client and conversation manager
llm_client = LLMClient(mcp_servers=[])  # No MCP servers needed for code generation
conversation_obj = ConversationManager(interaction_type="biosample_mapping")
# Note: ConversationManager automatically loads system prompt and examples

# 2. Add study-specific data to conversation
asyncio.run(add_study_data_to_conversation(
    conversation_obj=conversation_obj,
    biosample_attributes_path="biosamples.csv",
    raw_files_path="raw_files.csv",
    material_processing_yaml_path="protocol.yaml",
    additional_context_path="extracted_text.txt"  # optional
))

# 3. Generate mapping script
script_code = asyncio.run(get_llm_generated_script(
    llm_client=llm_client,
    conversation_obj=conversation_obj,
    biosample_path="biosamples.csv",
    files_path="raw_files.csv",
    yaml_path="protocol.yaml",  # Required: tells LLM exact YAML file path
    output_path="mapping_output.csv"
))

# 4. Save the generated script
with open("generated_mapping_script.py", "w") as f:
    f.write(script_code)

# 5. Execute script with iterative validation and fixing
success = asyncio.run(validate_and_fix_script(
    llm_client=llm_client,
    conversation_obj=conversation_obj,
    script_path="generated_mapping_script.py",
    output_path="mapping_output.csv",
    biosample_path="biosamples.csv",
    files_path="raw_files.csv",
    yaml_path="protocol.yaml",
    max_iterations=3  # Will attempt up to 3 fix iterations
))

if success:
    print("Mapping completed successfully!")
else:
    print("Failed to generate valid mapping after max iterations")
```
