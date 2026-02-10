# Protocol Conversion with LLM

> **Parent Module**: [LLM Module](../README.md)

Converts laboratory protocol text into NMDC-compliant YAML outlines representing material processing steps.

## Files

- **[instructions.py](instructions.py)** - System prompt defining task and validation requirements
- **[pipeline.py](pipeline.py)** - Main workflow (`get_llm_yaml_outline()`)
- **[mcp_server.py](mcp_server.py)** - MCP tools for schema retrieval and YAML validation

## MCP Tools

- `get_protocol_schema_context()` - Retrieves NMDC schema (classes, slots, enumerations)
- `validate_generated_yaml()` - Validates generated YAML against NMDC schema

## Usage

See [parent README](../README.md#shared-components) for `LLMClient` and `ConversationManager` setup.

```python
from nmdc_dp_utils.llm.protocol_conversion.pipeline import get_llm_yaml_outline
import asyncio

# Add protocol description to conversation
conversation_obj.add_protocol_description(description=protocol_text)

# Generate YAML (validation happens automatically via MCP tools)
response = asyncio.run(get_llm_yaml_outline(llm_client, conversation_obj))
```