# NMDC Data Processing Templates

This directory contains template files used by the `NMDCWorkflowManager` class to generate study-specific scripts and configurations.

## Template Files

### `biosample_mapping_script_template.py`

Template for generating biosample mapping scripts. This template is used by the `generate_biosample_mapping_script()` method to create study-specific scripts that map raw data files to NMDC biosamples.

**Template Variables:**
- `{study_name}`: The study name (e.g., "kroeger_11_dwsv7q78")
- `{study_description}`: The study description from config
- `{script_name}`: The generated script filename

**Usage:**
The template is automatically used when calling:
```python
study = NMDCWorkflowManager('config.json')
script_path = study.generate_biosample_mapping_script()
```

**Customization:**
After generation, the output script should be customized for each study's specific:
- File naming conventions
- Sample metadata patterns
- Biosample matching logic

## Adding New Templates

To add new templates:

1. Create a new `.py` file in this directory
2. Use `{variable_name}` syntax for template variables
3. Update the corresponding method in `study_manager.py` to use the template
4. Document the template variables and usage here

## Template Development Guidelines

- Use descriptive variable names for template substitution
- Include comprehensive docstrings and comments
- Provide customization instructions within the template
- Use double braces `{{}}` for literal braces in the output
- Test templates with various study configurations