#!/bin/bash

# Base directory for the JSON files
BASE_DIR="/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_lcms_metabolomics/wdl_jsons"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Count the number of JSON files in BASE_DIR
NUM_BATCHES=$(ls -1 "${BASE_DIR}"/run_metaMS_lcms_metabolomics_batch*.json 2>/dev/null | wc -l)

echo "Found $NUM_BATCHES batch files to process"

# Iterate over all available batch files
for i in $(seq 1 $NUM_BATCHES)
do
    JSON_FILE="${BASE_DIR}/run_metaMS_lcms_metabolomics_batch${i}.json"
    if [ -f "$JSON_FILE" ]; then
        echo "Processing $JSON_FILE"
        miniwdl run wdl/metaMS_lcms_metabolomics.wdl -i "$JSON_FILE" --verbose --no-cache --copy-input-files
                
        echo "Completed batch $i"
        echo "------------------------"
        
    else
        echo "File $JSON_FILE does not exist"
    fi
done

echo "All batches completed!"