#!/usr/bin/env python3
"""
Extract specific classes from NMDC LinkML schema and convert to LLM-friendly format
"""

import json
import os
from linkml_runtime.utils.schemaview import SchemaView
import nmdc_schema

def get_protocol_schema_context(output_path: str = None):
    """
    Extract classes related to 'MaterialProcessing' from NMDC schema
    and convert them to a JSON format suitable for LLM context.
    """

    # Initialize SchemaView from NMDC schema package
    nmdc_path = os.path.dirname(nmdc_schema.__file__)
    schema_path = os.path.join(nmdc_path, "nmdc_materialized_patterns.yaml")
    schema_view = SchemaView(schema_path)

    # Get all classes that are subclasses of 'MaterialProcessing'
    all_classes = schema_view.all_classes()
    relevant_classes = {
        class_name: class_def
        for class_name, class_def in all_classes.items()
        if class_def.is_a and "MaterialProcessing" in schema_view.get_class(class_def.is_a).name
    }

    # Recursively find all related classes and enums
    # For each slot in each relevant class, if the range is an enum or class, add it
    # Continue until no new classes or enums are found
    enums = {}
    new_found = True
    while new_found:
        new_found = False
        for class_name, class_def in list(relevant_classes.items()):
            for slot_name in class_def.slots:
                slot_def = schema_view.get_slot(slot_name)
                slot_range = slot_def.range
                
                # Check if range is an enum
                enum_def = schema_view.get_enum(slot_range)
                if enum_def and slot_range not in enums:
                    enums[slot_range] = enum_def
                    new_found = True
                
                # Check if range is a class
                class_range_def = schema_view.get_class(slot_range)
                if class_range_def and slot_range not in relevant_classes:
                    relevant_classes[slot_range] = class_range_def
                    new_found = True

    # Convert classes and enums to LLM-friendly format
    schema_output = {
        "classes": {name: class_def._as_json_obj() for name, class_def in relevant_classes.items()},
        "enums": {name: enum_def._as_json_obj() for name, enum_def in enums.items()}
    }
    return schema_output

if __name__ == "__main__":
    schema_output = get_protocol_schema_context()
    if schema_output:
        output_file = "nmdc_material_processing_llm_context.json"
        with open(output_file, "w") as f:
            json.dump(schema_output, f, indent=2)
        print(f"LLM protocol schema context saved to {output_file}")



