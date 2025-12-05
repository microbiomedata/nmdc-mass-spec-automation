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
        if class_def.is_a and "MaterialProcessing" in schema_view.get_class(class_def.is_a).name or
              class_name == "ProcessedSample"
    }

    # Recursively find all related classes and enums
    # For each slot in each relevant class, if the range is an enum or inline class, add it
    # Only include classes that are used inline (not just referenced by ID)
    # Continue until no new classes or enums are found
    enums = {}
    new_found = True
    while new_found:
        new_found = False
        for class_name, class_def in list(relevant_classes.items()):
            # Check only slots defined in this class (not inherited) for inline usage
            for slot_name in class_def.slots:
                # Get the induced slot (which includes slot_usage overrides)
                slot_def = schema_view.induced_slot(slot_name, class_name)
                slot_range = slot_def.range
                
                # Check if range is an enum
                enum_def = schema_view.get_enum(slot_range)
                if enum_def and slot_range not in enums:
                    enums[slot_range] = enum_def
                    new_found = True
                
                # Check if range is a class that's used inline, if so, add it to relevant_classes
                class_range_def = schema_view.get_class(slot_range)
                if class_range_def and slot_range not in relevant_classes:
                    # Only include if the slot is inlined or inlined_as_list
                    if slot_def.inlined or slot_def.inlined_as_list:
                        relevant_classes[slot_range] = class_range_def
                        new_found = True

    # Convert classes and enums to LLM-friendly format
    schema_output = {
        "classes": {},
        "slots": {},
        "enums": {name: enum_def._as_json_obj() for name, enum_def in enums.items()}
    }
    
    # Collect all unique slot definitions across all classes
    all_slot_definitions = {}
    
    # For each class, include slot names and collect slot definitions
    for class_name, class_def in relevant_classes.items():
        class_data = class_def._as_json_obj()
        
        # Get all induced slots for this class (includes inherited slots)
        class_slot_names = []
        for slot_name in schema_view.class_slots(class_name):
            class_slot_names.append(slot_name)
            
            # Collect slot definition if not already captured
            if slot_name not in all_slot_definitions:
                induced_slot = schema_view.induced_slot(slot_name, class_name)
                slot_info = {
                    "range": induced_slot.range,
                }
                # Only add non-null values for these fields
                for attr in ["description", "required", "multivalued"]:
                    value = getattr(induced_slot, attr, None)
                    if value is not None:
                        slot_info[attr] = value
                
                all_slot_definitions[slot_name] = slot_info
        
        # Store just the slot names in the class
        class_data["class_slots"] = class_slot_names
        # Remove the class_data["slots"] since we are replacing it with class_slots
        if "slots" in class_data:
            del class_data["slots"]
        schema_output["classes"][class_name] = class_data
    
    # Add all collected slot definitions
    schema_output["slots"] = all_slot_definitions
    
    return schema_output


