# Biosample Mapping with LLM (Code Generation Approach)

> **Parent Module**: [LLM Module](../README.md)

Maps raw mass spectrometry files to their corresponding biosamples and processed samples using an LLM to generate a Python mapping script.

## Approach

This module uses **code generation** for efficient biosample mapping:
1. LLM analyzes file naming patterns and study metadata
2. LLM generates a Python script that implements the mapping logic
3. Script executes to map all files (typically < 1 second)
4. Output CSV is validated automatically
5. If validation fails, LLM fixes the script and re-runs

**Performance**: ~30-60 seconds total for hundreds of files (vs hours with direct CSV generation)

## Files

- **[instructions.py](instructions.py)** - System prompt for Python script generation
- **[pipeline.py](pipeline.py)** - Main workflow (`get_llm_generated_script()`, `validate_and_fix_script()`, `add_study_data_to_conversation()`)
- **[validation.py](validation.py)** - Programmatic validation logic
- **[mcp_server.py](mcp_server.py)** - MCP tool for CSV validation (optional, used by older approach)

## Usage

See [parent README](../README.md#shared-components) for `LLMClient` and `ConversationManager` setup.

```python
from nmdc_dp_utils.llm.biosample_mapping.pipeline import (
    get_llm_generated_script,
    validate_and_fix_script,
    add_study_data_to_conversation
)
import asyncio

# Add study data to conversation
asyncio.run(add_study_data_to_conversation(
    conversation_obj=conversation_obj,
    biosample_attributes_path="biosamples.csv",
    raw_files_path="raw_files.csv",
    material_processing_yaml_path="protocol.yaml",
    study_id="nmdc:sty-11-xxxxx",  # optional
    additional_context_path="extracted_text.txt"  # optional
))

# Generate mapping script
script_code = asyncio.run(get_llm_generated_script(
    llm_client=llm_client,
    conversation_obj=conversation_obj,
    biosample_path="biosamples.csv",
    files_path="raw_files.csv",
    output_path="mapping_output.csv"
))

# Save and execute script with validation
success = asyncio.run(validate_and_fix_script(
    llm_client=llm_client,
    conversation_obj=conversation_obj,
    script_path="generated_script.py",
    output_path="mapping_output.csv",
    biosample_path="biosamples.csv",
    files_path="raw_files.csv",
    yaml_path="protocol.yaml",
    max_iterations=3
))
```

## Command Line Usage

```bash
# Run with default example
python nmdc_dp_utils/llm/biosample_mapping/pipeline.py

# Test code generation approach
python test_codegen_approach.py
```
