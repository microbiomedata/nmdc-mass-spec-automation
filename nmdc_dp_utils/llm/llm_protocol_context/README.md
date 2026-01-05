Protocol context workspace

This directory holds per-study protocol materials and standardized derivatives used for metadata generation and LLM prompting. Each study lives in its own subfolder and includes the raw protocol documents plus normalized text and structure files.

Expected subfolder layout

llm_protocol_context/           # Superfolder
├── example_1/                  # Per-study folder (name may be anonymized later)
│   ├── raw_1.*                 # Original protocol document (pdf, docx, html, md, txt)
│   ├── raw_2.*                 # Additional source document(s)
│   ├── extracted_text.txt      # Plaintext extracted from the raw document(s)
│   ├── combined_outline.yaml   # Structured outline generated from the extracted text
│   ├── combined_input.csv      # Aggregated inputs used by downstream metadata scripts
│   └── README.md               # Subfolder note preserving the original folder name
├── example_2/
│   └── ...

File descriptions
- raw_*.*: Source protocol documents provided as-is (format may vary).
- extracted_text.txt: Unified, machine-readable text used as input to outline generation.
- combined_outline.yaml: Canonical, sectioned representation of the protocol derived from the extracted text.
- combined_input.csv: Consolidated inputs for metadata generation (e.g., material processing metadata).
- README.md: Brief description of the subfolder contents and the original (pre-anonymization) folder name.

Notes
- Folder names may be renamed to anonymized labels (e.g., example_1, example_2). Subfolder READMEs retain the original names for provenance.
- See the parent README in llm_protocol_context for high-level guidance across studies.
