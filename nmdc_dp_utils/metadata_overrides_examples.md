# Metadata Overrides System

The NMDC Study Manager supports a flexible pattern-based metadata override system that allows you to apply different metadata values based on filename patterns. This is useful when a single study configuration needs to differentiate files based on experimental conditions, instrument settings, or other factors encoded in the filename.

## Example Configuration File

See `example_config.json` for comprehensive examples showing:
- Basic collision energy-based mass spec overrides
- Multi-instrument configurations 
- Multiple metadata field overrides
- Sample type and processing method differentiation

## Configuration Structure

```json
{
    "configurations": [
        {
            "name": "example_config",
            "file_filter": ["PATTERN1", "PATTERN2"],
            "default_metadata_field": "Default Value",
            "metadata_overrides": {
                "metadata_field_name": {
                    "filename_pattern": "override_value",
                    "another_pattern": "another_value"
                }
            }
        }
    ]
}
```

## Example 1: Collision Energy-Based Mass Spec Differentiation (Current Implementation)

```json
{
    "name": "hilic_pos",
    "file_filter": ["HILICZ", "_POS_"],
    "mass_spec_configuration_name": "ESI Orbitrap MS/MS Positive",
    "metadata_overrides": {
        "mass_spec_configuration_name": {
            "CE102040": "JGI/LBNL Standard Metabolomics Method, positive @10,20,40CE",
            "CE205060": "JGI/LBNL Standard Metabolomics Method, positive @20,50,60CE"
        }
    }
}
```

**Result**: Files with "CE102040" in the filename get the @10,20,40CE configuration, while files with "CE205060" get the @20,50,60CE configuration.

## Example 2: Multiple Field Overrides

```json
{
    "name": "multi_instrument",
    "file_filter": ["experiment"],
    "instrument_used": "Default Instrument",
    "mass_spec_configuration_name": "Default MS Config",
    "chromat_configuration_name": "Default Chromat Config",
    "metadata_overrides": {
        "instrument_used": {
            "QE_HF": "Q Exactive HF Orbitrap",
            "IDX": "Thermo Orbitrap IQ-X Tribrid"
        },
        "mass_spec_configuration_name": {
            "CE102040": "Method @10,20,40CE",
            "CE205060": "Method @20,50,60CE"
        },
        "chromat_configuration_name": {
            "_HILIC_": "HILIC Chromatography",
            "_C18_": "C18 Chromatography"
        }
    }
}
```

**Result**: Each file gets metadata values based on multiple patterns in its filename:
- Filename: `experiment_QE_HF_HILIC_CE102040_sample1.raw`
  - instrument_used: "Q Exactive HF Orbitrap" (matches "QE_HF")
  - mass_spec_configuration_name: "Method @10,20,40CE" (matches "CE102040")
  - chromat_configuration_name: "HILIC Chromatography" (matches "_HILIC_")

## Example 3: Study-Specific Overrides

```json
{
    "name": "soil_study",
    "file_filter": ["soil"],
    "sample_type": "Unknown",
    "processing_method": "Standard",
    "metadata_overrides": {
        "sample_type": {
            "bulk": "Bulk Soil",
            "rhizo": "Rhizosphere",
            "root": "Root Endosphere"
        },
        "processing_method": {
            "freeze_dry": "Freeze Drying",
            "air_dry": "Air Drying",
            "fresh": "Fresh Processing"
        }
    }
}
```

## Pattern Matching Rules

1. **Case Sensitive**: Patterns are matched exactly as specified
2. **First Match Wins**: If multiple patterns match a filename, the first match in the configuration order is used
3. **Fallback**: If no patterns match, the default value from the configuration is used
4. **Substring Matching**: Patterns are matched as substrings within the filename

## Usage Tips

1. **Order Matters**: Place more specific patterns before general ones
2. **Test Patterns**: Use descriptive patterns that won't accidentally match unintended files
3. **Default Values**: Always provide default values for fields that use overrides
4. **Validation**: The system will report how many files match each pattern during processing

## Migration from Legacy Systems

If you have existing collision energy mappings using `mass_spec_ce_mapping`, they can be converted to the new system:

**Old Format**:
```json
"mass_spec_ce_mapping": {
    "CE102040": "Method @10,20,40CE",
    "CE205060": "Method @20,50,60CE"
}
```

**New Format**:
```json
"metadata_overrides": {
    "mass_spec_configuration_name": {
        "CE102040": "Method @10,20,40CE",
        "CE205060": "Method @20,50,60CE"
    }
}
```

The functionality is identical, but the new system is more flexible and can handle any metadata field.