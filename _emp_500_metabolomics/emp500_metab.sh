#!/bin/bash

# Base directory for the JSON files
BASE_DIR="/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_emp_500_metabolomics"

# Iterate over batch 1 to batch 15
for i in {1..15}
do
    JSON_FILE="${BASE_DIR}/run_metaMS_gcms_batch_${i}.json"
    if [ -f "$JSON_FILE" ]; then
        echo "Processing $JSON_FILE"
        miniwdl run wdl/metaMS_gcms.wdl -i "$JSON_FILE" --verbose --no-cache --copy-input-files
    else
        echo "File $JSON_FILE does not exist"
    fi
done