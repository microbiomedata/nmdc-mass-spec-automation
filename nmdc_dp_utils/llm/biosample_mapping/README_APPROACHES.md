# Biosample Mapping - Two Approaches

This directory contains two different approaches for using LLMs to map raw mass spectrometry files to biosamples and processed samples.

## Approach 1: Direct CSV Generation (Original)

**Files:**
- `instructions.py` - System prompt for direct CSV generation
- `pipeline.py` - Pipeline that asks LLM to generate CSV directly
- `mcp_server.py` - MCP tools for validation

**How it works:**
1. LLM analyzes file naming patterns
2. LLM generates CSV mapping row-by-row
3. MCP tool validates the output
4. LLM fixes errors if validation fails

**Performance:**
- ~20 seconds per file
- For 353 files: **~2 hours total**
- Good for small datasets (<50 files)

**Advantages:**
- Simple, straightforward
- LLM has full control over each mapping decision
- Easy to understand what's happening

**Disadvantages:**
- **Very slow** for large datasets
- LLM must manually generate hundreds of rows

---

## Approach 2: Code Generation (New, Recommended)

**Files:**
- `instructions_codegen.py` - System prompt for script generation
- `pipeline_codegen.py` - Pipeline that generates and executes mapping script

**How it works:**
1. LLM analyzes file naming patterns
2. LLM generates a **Python script** that does the mapping
3. Script executes in <1 second to map all files
4. Validate output with MCP tool (future enhancement)
5. If errors, LLM fixes the script and re-runs

**Performance:**
- Script generation: ~10-20 seconds
- Script execution: ~1 second (all 353 files)
- Error iterations: ~10-20 seconds each
- **Total: 30-60 seconds** for entire dataset

**Advantages:**
- **1000x faster** execution
- **Debuggable** - you can inspect/modify the script
- **Reusable** - same script works on future datasets
- **Transparent** - see the mapping logic
- Self-correcting via validation feedback

**Disadvantages:**
- Slightly more complex setup
- Requires script execution capability

---

## Usage

### Direct CSV Generation (Approach 1)
```bash
# Full dataset
python nmdc_dp_utils/llm/biosample_mapping/pipeline.py

# Small test (5 files)
python test_pipeline_small_subset.py
```

### Code Generation (Approach 2) - **RECOMMENDED**
```bash
# Full dataset
python nmdc_dp_utils/llm/biosample_mapping/pipeline_codegen.py

# Quick test
python test_codegen_approach.py
```

---

## Example Generated Script

The code generation approach produces scripts like:

```python
import pandas as pd
import re

biosamples = pd.read_csv('biosample_attributes.csv')
files = pd.read_csv('downloaded_files.csv')

def map_file_to_biosample(filename):
    # Extract sample ID (e.g., S32)
    sample_match = re.search(r'[_-](S\d+)', filename)
    timepoint = re.search(r'[_-](D\d+)', filename)
    replicate = re.search(r'[_-]([ABC])[_-]', filename)
    
    if sample_match:
        sample_id = sample_match.group(1)
        # Match to biosample...
        # Determine protocol from method...
        return {...}
    return None

results = []
for filename in files['raw_data_file_name']:
    mapping = map_file_to_biosample(filename)
    results.append({...})

pd.DataFrame(results).to_csv('output.csv', index=False)
```

The script maps 353 files in **milliseconds** instead of **hours**.

---

## Token Optimization

Both approaches use the same optimized context:
- **Examples**: ~36k tokens (using minimal example_3 with combined_inputs_v2.csv)
- **Study data**: ~13-25k tokens (filtered CSV columns, simplified YAML)
- **Total**: ~50-60k tokens (down from original 110-175k)

Optimizations applied:
1. Single example (example_3) instead of 2 examples
2. Biosample CSV: only `id` and `name` columns
3. Files CSV: only file name column
4. YAML: only `description`, `has_input`, `has_output`, and `processedsamples`
5. System prompt: condensed from ~925 to ~450 tokens

---

## Recommendation

**Use Approach 2 (Code Generation)** for:
- ✅ Large datasets (>50 files)
- ✅ Datasets with consistent naming patterns
- ✅ When you want fast results
- ✅ When you want reusable logic

**Use Approach 1 (Direct CSV)** for:
- Small datasets (<20 files)
- One-off mappings with unusual patterns
- When you want maximum LLM control over each decision
