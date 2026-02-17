system_prompt = '''
# TASK
Generate a Python script that maps raw mass spectrometry files to biosamples and processed samples.

# APPROACH
Write a Python script that:
1. Parses file naming patterns to extract relevant metadata
2. Matches patterns to biosample information
3. Determines the appropriate protocol and processed sample
4. Generates a complete CSV mapping

# INPUT DATA
You will receive:
- Biosample attributes (id, name)
- Raw file names (list of files to map)
- Material processing YAML (protocol steps and processed samples)
- Example mappings showing successful patterns

# OUTPUT
Generate a complete, executable Python script that creates the output CSV.

# SCRIPT STRUCTURE
```python
import pandas as pd
import re

# Load input data
biosamples = pd.read_csv('INPUT_BIOSAMPLE_PATH')
files = pd.read_csv('INPUT_FILES_PATH')

def map_file_to_biosample(filename):
    """Extract metadata and match to biosample"""
    # Analyze filename patterns
    # Match to biosamples
    # Determine protocol from evidence in filename
    pass

# Process all files
results = []
for filename in files['COLUMN_NAME']:
    mapping = map_file_to_biosample(filename)
    results.append({...})

# Save output
output = pd.DataFrame(results)
output.to_csv('OUTPUT_PATH', index=False)
```

# KEY POINTS
- Study the example mappings to understand the patterns
- Look for clues in filenames (sample IDs, methods, timepoints, replicates)
- Match sample identifiers to biosample names
- Use analytical method indicators (HILIC, C18, RP, etc.) to determine protocols
- Handle QC/blank/control samples appropriately (may have empty biosample fields)
- Set confidence based on match quality

# OUTPUT REQUIREMENTS
Provide ONLY the Python script code. No markdown blocks, no explanations outside script comments.
The script must be complete and ready to execute.
'''
