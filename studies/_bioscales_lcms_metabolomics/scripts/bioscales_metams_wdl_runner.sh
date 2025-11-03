#!/bin/bash

# Base directory for the JSON files
BASE_DIR="/Users/heal742/LOCAL/05_NMDC/02_MetaMS/data_processing/_bioscales_lcms_metabolomics/wdl_jsons"

NUM_BATCHES=$(find "${BASE_DIR}" -type f -name 'run_metaMS_lcms_metabolomics_batch*.json' | wc -l)

echo "Found $NUM_BATCHES batch files to process"

# Iterate over all available batch files
for JSON_FILE in $(find "${BASE_DIR}" -type f -name 'run_metaMS_lcms_metabolomics_batch*.json' | sort); do
    echo "Processing $JSON_FILE"
    miniwdl run wdl/metaMS_lcms_metabolomics.wdl -i "$JSON_FILE" --verbose --no-cache --copy-input-files

    echo "Completed batch: $JSON_FILE"
    echo "------------------------"
done

echo "All batches completed!"