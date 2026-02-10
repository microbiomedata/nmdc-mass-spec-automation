# Protocol Conversion with LLM

This module provides LLM-powered conversion of laboratory protocol text to NMDC-compliant YAML outlines.

## Overview

The protocol conversion pipeline analyzes laboratory protocol descriptions and converts them into structured YAML that represents material processing steps compliant with the NMDC schema.

## Components

### 1. System Instructions ([instructions.py](instructions.py))
- Defines the LLM system prompt for protocol conversion
- Specifies YAML structure requirements
- Requires use of `get_protocol_schema_context` and `validate_generated_yaml` MCP tools

### 2. MCP Server ([mcp_server.py](mcp_server.py))
- FastMCP server exposing two tools:
  - `get_protocol_schema_context()`: Retrieves NMDC schema classes, slots, and enumerations
  - `validate_generated_yaml()`: Validates generated YAML against NMDC schema
- Tool cleans markdown code fences from LLM responses

### 3. Pipeline ([pipeline.py](pipeline.py))
- `get_llm_yaml_outline()`: Main async function to generate YAML
- Example usage in `__main__` section

## Usage

### Running the Pipeline

```python
from nmdc_dp_utils.llm.llm_client import LLMClient
from nmdc_dp_utils.llm.llm_conversation_manager import ConversationManager
from nmdc_dp_utils.llm.protocol_conversion.pipeline import get_llm_yaml_outline
import asyncio

# Read protocol description
with open("protocol_description.txt", "r") as f:
    protocol_description = f.read()

# Create LLM client and conversation manager
llm_client = LLMClient()
conversation_obj = ConversationManager(interaction_type="protocol_conversion")

# Add protocol description
conversation_obj.add_protocol_description(description=protocol_description)

# Get YAML outline (validation happens automatically via MCP tools)
response = asyncio.run(get_llm_yaml_outline(
    llm_client=llm_client,
    conversation_obj=conversation_obj
))

# Save result
with open("protocol_outline.yaml", "w") as f:
    f.write(response)
```

### Running with MCP Server

The MCP server must be running and accessible to the LLM client for schema retrieval and validation:

```bash
# Start the MCP server
python -m nmdc_dp_utils.llm.protocol_conversion.mcp_server
```

## Example Data

See `nmdc_dp_utils/llm/examples/` for complete vetted examples including:
- `extracted_text.txt` - Protocol description (input)
- `combined_outline.yaml` - YAML outline (output)
- `raw_*.pdf/docx` - Original protocol documents

Examples 1-7 demonstrate protocol conversion. Example 8 includes biosample mapping data.
