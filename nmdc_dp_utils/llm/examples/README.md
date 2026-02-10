# LLM Examples

> **Parent Module**: [LLM Module](../README.md)

Curated examples for few-shot prompting in LLM workflows. Each example represents a real study with protocol documents and metadata.

## Structure

- **example_1 - example_7**: Protocol conversion examples
- **example_8**: Biosample mapping + protocol conversion example

## Per-Example Files

### Protocol Conversion Files
- `study_id.txt` - NMDC study ID
- `raw_*.*` - Source protocol documents (PDF, DOCX, etc.)
- `extracted_text.txt` - Plaintext extracted from source, **Input** for LLM for protocol conversion
- `combined_outline.yaml` - **Output**: Structured YAML outline, also an **Input** for biosample mapping example

### Biosample Mapping Files
- `biosample_attributes.csv` - Biosample metadata from NMDC API, **Input** for LLM for biosample mapping
- `raw_files.csv` - Raw data file listing, **Input** for LLM for biosample mapping
- `combined_input.csv` - **Output**: Biosample-to-file mapping

Each subfolder includes a README with provenance notes.