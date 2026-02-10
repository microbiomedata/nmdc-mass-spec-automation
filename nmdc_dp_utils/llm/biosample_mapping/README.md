# Biosample Mapping with LLM

> **Parent Module**: [LLM Module](../README.md)

Maps raw mass spectrometry files to their corresponding biosamples and processed samples by analyzing file naming conventions and metadata.

## Files

- **[instructions.py](instructions.py)** - System prompt defining mapping logic and validation requirements
- **[pipeline.py](pipeline.py)** - Main workflow (`get_llm_biosample_mapping()`, `add_study_data_to_conversation()`)
- **[validation.py](validation.py)** - Programmatic validation logic
- **[mcp_server.py](mcp_server.py)** - MCP tool for CSV validation

## MCP Tool

- `validate_biosample_mapping()` - Validates generated CSV against input data (biosample IDs, names, processed samples, protocols, file coverage)

## Usage

See [parent README](../README.md#shared-components) for `LLMClient` and `ConversationManager` setup.

```python
from nmdc_dp_utils.llm.biosample_mapping.pipeline import (
    get_llm_biosample_mapping,
    add_study_data_to_conversation
)
import asyncio

# Add study data to conversation
asyncio.run(add_study_data_to_conversation(
    conversation_obj=conversation_obj,
    biosample_attributes_path="biosamples.csv",
    raw_files_path="raw_files.csv",
    material_processing_yaml_path="protocol.yaml",
    study_id="nmdc:sty-11-xxxxx"  # optional
))

# Generate mapping (validation happens automatically via MCP tool)
response = asyncio.run(get_llm_biosample_mapping(llm_client, conversation_obj))
```
