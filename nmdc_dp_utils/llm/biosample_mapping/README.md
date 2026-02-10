# Biosample Mapping with LLM

This module provides LLM-powered biosample-to-raw-file mapping for NMDC mass spectrometry data, with built-in validation via MCP tools.

## Overview

The biosample mapping pipeline analyzes raw mass spectrometry file names, biosample attributes, and material processing protocols to automatically map each raw file to its corresponding biosample and processed sample.

## Components

### 1. System Instructions ([instructions.py](instructions.py))
- Defines the LLM system prompt for biosample mapping
- Specifies mapping logic for parsing file names (sample codes, replicates, timepoints, analytical methods)
- Provides biosample matching rules and confidence levels (high/medium/low)
- Requires validation using the `validate_biosample_mapping` MCP tool

### 2. Validation Module ([validation.py](validation.py))
- `validate_biosample_mapping_csv()`: Validates generated CSV against input data
  - Checks biosample ID format (nmdc:bsm-XX-XXXXXXXX)
  - Verifies biosample names match IDs
  - Confirms processed samples exist in YAML
  - Ensures protocols match YAML top-level names
  - Validates all raw files are mapped
- `format_validation_errors()`: Formats validation errors for readability

### 3. MCP Server ([mcp_server.py](mcp_server.py))
- FastMCP server exposing the `validate_biosample_mapping` tool
- Requires validation context to be set before use via `set_validation_context()`
- Tool cleans markdown code fences from LLM responses
- Returns structured validation results

### 4. Pipeline ([pipeline.py](pipeline.py))
- `get_llm_biosample_mapping()`: Main async function to generate mapping
- `add_study_data_to_conversation()`: Helper to load study data into conversation
- Example usage in `__main__` section

### 5. Conversation Manager ([../llm_conversation_manager.py](../llm_conversation_manager.py))
- `add_biosample_mapping_examples()`: Loads curated examples (example_8)
- Manages conversation state with system prompts and examples

## Usage

### Running the Pipeline

```python
from nmdc_dp_utils.llm.llm_client import LLMClient
from nmdc_dp_utils.llm.llm_conversation_manager import ConversationManager
from nmdc_dp_utils.llm.biosample_mapping.pipeline import (
    get_llm_biosample_mapping,
    add_study_data_to_conversation
)
import asyncio

# Create LLM client and conversation manager
llm_client = LLMClient()
conversation_obj = ConversationManager(interaction_type="biosample_mapping")

# Add study data to conversation
asyncio.run(add_study_data_to_conversation(
    conversation_obj=conversation_obj,
    biosample_attributes_path="path/to/biosample_attributes.csv",
    raw_files_path="path/to/raw_files.csv",
    material_processing_yaml_path="path/to/material_processing.yaml",
    study_id="nmdc:sty-11-xxxxx"  # optional
))

# Get biosample mapping (validation happens automatically via MCP tool)
response = asyncio.run(get_llm_biosample_mapping(
    llm_client=llm_client,
    conversation_obj=conversation_obj
))

# Save result
with open("output_mapping.csv", "w") as f:
    f.write(response)
```

### Running with MCP Server

The MCP server must be running and accessible to the LLM client for validation to work:

```bash
# Start the MCP server
python -m nmdc_dp_utils.llm.biosample_mapping_server
```

The server needs validation context set programmatically:

```python
from nmdc_dp_utils.llm.biosample_mapping.mcp_server import set_validation_context

# Read input data
with open("biosample_attributes.csv", "r") as f:
    biosample_attributes = f.read()
with open("raw_files.csv", "r") as f:
    raw_files = f.read()
with open("material_processing.yaml", "r") as f:
    material_processing_yaml = f.read()

# Set context for validation
set_validation_context(
    biosample_attributes=biosample_attributes,
    raw_files=raw_files,
    material_processing_yaml=material_processing_yaml
)
```

## Input Data Format

### Biosample Attributes CSV
```csv
id,name,samp_name,analysis_type,gold_biosample_identifiers
nmdc:bsm-11-qn720m46,S16_A_D45 hydrophilic,S16_A_D45 hydrophilic,"['metabolomics']",['gold:Gb0290969']
```

### Raw Files CSV
```csv
file_path,file_name,file_size_bytes
/path/to/file.raw,20210819_..._S16-D45_A_...Run53.raw,95870496
```

### Material Processing YAML
```yaml
polar_metabolites:
  steps:
    - Step 1_polar_metabolites:
        SubSamplingProcess:
          # ... step details
  processedsamples:
    - ProcessedSample3_polar_metabolites:
        ProcessedSample:
          # ... sample details
```

## Output Format

```csv
raw_data_identifier,biosample_id,biosample_name,match_confidence,processedsample_placeholder,material_processing_protocol_id
20210819_..._Run53.raw,nmdc:bsm-11-qn720m46,S16_A_D45 hydrophilic,high,ProcessedSample3_polar_metabolites,polar_metabolites
```

## Validation Rules

The `validate_biosample_mapping` tool checks:

1. **CSV Structure**: Required columns present, no extra columns
2. **Biosample IDs**: 
   - Follow format `nmdc:bsm-XX-XXXXXXXX`
   - Exist in biosample attributes CSV
3. **Biosample Names**: Match the corresponding biosample ID
4. **Match Confidence**: Must be "high", "medium", "low", or empty
5. **Processed Samples**: Exist in material processing YAML
6. **Protocol IDs**: Match top-level protocol names in YAML
7. **Coverage**: All raw files from input CSV are mapped

## Match Confidence Levels

- **high**: All identifiers match (sample code, timepoint, replicate, sample type)
- **medium**: Major identifiers match but one component missing
- **low**: Only sample code matches, other identifiers uncertain

## File Naming Pattern Recognition

The LLM parses file names to extract:
- **Sample identifier**: S16, S1, etc.
- **Replicate**: A, B, C
- **Timepoint**: D30, D45, D89
- **Column type**: HILIC (polar), C18/RP (nonpolar)
- **Ionization mode**: POS, NEG
- **Sample type**: hydrophilic/hydrophobic

Example:
```
20210819_..._HILICZ_..._NEG_MSMS_10_S16-D45_A_...Run53.raw
→ Sample=S16, Timepoint=D45, Replicate=A, Type=hydrophilic
```

## Error Handling

- **Unparseable file names**: Best effort matching, mark as `low` confidence
- **QC/control samples**: May have empty biosample_id
- **Ambiguous protocols**: Select most likely, mark as `medium` confidence

## Example Data

See `nmdc_dp_utils/llm/examples/example_8/` for a complete vetted example including:
- `biosample_attributes.csv`
- `raw_files.csv`
- `combined_yaml.yaml`
- `combined_input.csv` (expected output)
