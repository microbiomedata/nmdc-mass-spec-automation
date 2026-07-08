#!/bin/bash

# WDL Runner Script for mcmahon_11_fkbnah04
# Generated automatically by NMDC Study Manager

# Base directory for the JSON files
BASE_DIR="/Users/heal742/LOCAL/05_NMDC/07_automation/nmdc_mass_spec_automation/workflows/mcmahon_11_fkbnah04_lcms_metab/wdl_jsons"

# Count total batch files
NUM_BATCHES=$(find "${BASE_DIR}" -type f -name '*.json' | wc -l)

echo "Found $NUM_BATCHES JSON files to process for study: mcmahon_11_fkbnah04"
echo "Study ID: ['nmdc:sty-11-fkbnah04']"
echo "========================"

# Check if WDL file exists in current directory
WDL_FILE="wdl/metaMS_lcms_metabolomics.wdl"
if [ ! -f "$WDL_FILE" ]; then
    echo "ERROR: WDL file not found: $WDL_FILE"
    echo "Please run this script from a directory containing the wdl/ subdirectory"
    exit 1
fi

echo "Using WDL workflow: $WDL_FILE"
echo "========================"

# Initialize counters
SUCCESS_COUNT=0
FAILED_COUNT=0

# Iterate over all JSON files, sorted by name
for JSON_FILE in $(find "${BASE_DIR}" -type f -name '*.json' | sort); do
    BATCH_NAME=$(basename "$JSON_FILE")
    echo "Processing batch: $BATCH_NAME"
    echo "File: $JSON_FILE"
    
    # Run miniwdl with the JSON file
    if miniwdl run "$WDL_FILE" -i "$JSON_FILE" --verbose --no-cache --copy-input-files; then
        echo "✓ SUCCESS: Completed batch $BATCH_NAME"
        ((SUCCESS_COUNT++))
    else
        echo "✗ FAILED: Batch $BATCH_NAME failed with exit code $?"
        ((FAILED_COUNT++))
        echo "Continuing with next batch..."
    fi
    
    echo "------------------------"
done

echo "========================"
echo "WORKFLOW SUMMARY:"
echo "  Total batches: $NUM_BATCHES"
echo "  Successful: $SUCCESS_COUNT"
echo "  Failed: $FAILED_COUNT"
echo "  Study: mcmahon_11_fkbnah04 (['nmdc:sty-11-fkbnah04'])"

if [ $FAILED_COUNT -eq 0 ]; then
    echo "🎉 All batches completed successfully!"
    exit 0
else
    echo "⚠️  Some batches failed. Check logs above for details."
    exit 1
fi
