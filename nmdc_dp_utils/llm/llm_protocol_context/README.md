Protocol context workspace

This directory holds per-study protocol materials and standardized derivatives used for metadata generation and LLM prompting. Each study lives in its own subfolder and includes the raw protocol documents plus normalized text and structure files.

Expected subfolder layout

llm_protocol_context/               # Superfolder
├── example_1/                      # Per-study folder (name may be anonymized later)
│   ├── study_id.txt                # NMDC study ID for the study (e.g., 'nmdc:sty-11-34xj1150')
│   ├── raw_1.*                     # Original protocol document (pdf, docx, html, md, txt)
│   ├── raw_2.*                     # Additional source document(s)
│   ├── extracted_text.txt          # Plaintext extracted from the raw document(s)
│   ├── combined_outline.yaml       # Structured outline generated from the extracted text
│   ├── biosample_attributes.csv    # Biosample attributes fetched from NMDC API for the study's biosamples
│   ├── raw_files.csv               # Raw data files resolved from NMDC IDs or filenames
│   ├── combined_input.csv          # Aggregated inputs used by downstream metadata scripts that maps nmdc biosample to raw file names and protocol sections (e.g., for material processing metadata)
│   └── README.md                   # Subfolder note preserving the original folder name
├── example_2/
│   └── ...

File descriptions
- study_id.txt: NMDC study ID for the study (e.g., 'nmdc:sty-11-34xj1150')
- raw_*.*: Source protocol documents provided as-is (format may vary).
- extracted_text.txt: Unified, machine-readable text used as input to outline generation.
- combined_outline.yaml: Canonical, sectioned representation of the protocol derived from the extracted text.
- combined_input.csv: Aggregated input mapping biosamples to raw files and protocol sections for downstream metadata generation.
- biosample_attributes.csv: Biosample attributes fetched from NMDC API for the study's biosamples.
- raw_files.csv: Resolved raw data files associated with the study's biosamples, mapped from NMDC IDs or filenames to standardized names for metadata generation.
- README.md: Brief description of the subfolder contents and the original (pre-anonymization) folder name.

Notes
- Folder names may be renamed to anonymized labels (e.g., example_1, example_2). Subfolder READMEs retain the original names for provenance.
- See the parent README in llm_protocol_context for high-level guidance across studies.

References
- Instructions: [nmdc_dp_utils/llm/llm_protocol_context/instructions.py](nmdc_dp_utils/llm/llm_protocol_context/instructions.py) — guidance for extracting text and generating outlines/inputs.
- Schema: [nmdc_dp_utils/llm/llm_protocol_context/schema.py](nmdc_dp_utils/llm/llm_protocol_context/schema.py) — definitions/utilities describing the expected `combined_outline.yaml` structure.
- Examples: See `combined_outline.yaml` within each `example_*` subfolder for concrete instances of the outline format.
