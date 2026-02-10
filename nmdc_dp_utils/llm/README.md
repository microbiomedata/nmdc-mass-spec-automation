# LLM Module

This module provides LLM-powered tools for automating NMDC mass spectrometry data processing workflows.

## Two Main Workflows

1. **[Protocol Conversion](protocol_conversion/)**: Lab protocol text → YAML outline compliant with NMDC schema
2. **[Biosample Mapping](biosample_mapping/)**: Raw files + YAML + biosamples → CSV mapping

Both workflows share common infrastructure and use [curated examples](examples/) for few-shot learning.

## Module Structure

```
llm/
├── llm_client.py                 # Shared: LLM API client with MCP server support
├── llm_conversation_manager.py   # Shared: Conversation state management
├── examples/                     # Shared: Curated examples for both workflows
├── protocol_conversion/          # Pipeline 1: Protocol text → YAML
└── biosample_mapping/            # Pipeline 2: Raw files → Biosample mapping
```

See sub-directories for detailed documentation on each workflow.

## Shared Components

### LLMClient ([llm_client.py](llm_client.py))
- Configures OpenAI-compatible LLM API connection
- Manages MCP (Model Context Protocol) server connections
- Provides async interface for LLM interactions
- Default model: `gemini-2.5-flash-project`

### ConversationManager ([llm_conversation_manager.py](llm_conversation_manager.py))
- Manages conversation state across multiple LLM calls
- Loads system prompts and few-shot examples based on interaction type
- Supports two interaction types: `protocol_conversion` and `biosample_mapping`

### Setup Example

```python
from nmdc_dp_utils.llm import LLMClient, ConversationManager

# Initialize client and conversation
llm_client = LLMClient()
conversation_obj = ConversationManager(interaction_type="protocol_conversion")  # or "biosample_mapping"
```

### Examples ([examples/](examples/))
- Curated examples demonstrating complete workflows
- Each example represents a real study with vetted inputs/outputs
- Examples 1-7: Protocol conversion (text → YAML)
- Example 8: Full workflow including biosample mapping

## Usage

See workflow-specific READMEs for detailed usage examples:
- **[Protocol Conversion Usage](protocol_conversion/README.md#usage)** - Converting protocol text to YAML
- **[Biosample Mapping Usage](biosample_mapping/README.md#usage)** - Mapping raw files to biosamples

## Testing

Run LLM-related tests:
```bash
python -m pytest tests/ -k "llm or LLM" -v
```

## Further Reading

- **[Protocol Conversion](protocol_conversion/README.md)** - Converting lab protocols to YAML
- **[Biosample Mapping](biosample_mapping/README.md)** - Mapping raw files to biosamples
- **[Examples](examples/README.md)** - Understanding the example data structure