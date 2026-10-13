#!/bin/bash

cd /home/bmeluch/NMDC/nmdc-mass-spec-automation
source .venv/bin/activate
sudo mount -t drvfs 'X:' /mnt/proto-9_SciMax01/

# control batch num here
BATCH_NUM=9

echo "=== Running batch $BATCH_NUM ==="

# # # Keep only SRFA .zip files and SRFA .d directories (with their contents) in raw; delete all other .zip files and .d directories
# # RAW_DIR="/home/bmeluch/NMDC/nom_processing/monet_nom/raw"
# # find "$RAW_DIR" -maxdepth 1 -type f -iname '*.zip' ! -iname '*SRFA*' -delete
# # find "$RAW_DIR" -maxdepth 1 -type d -iname '*.d' ! -iname '*SRFA*' -prune -exec rm -rf {} +

# Get batch $BATCH_NUM
python workflows/monet_nom_metadata/get_data/batch_and_download.py /home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom_metadata/get_data/dms_results_manual_list_to_download.xlsx --batch-num "$BATCH_NUM" --output-dir /home/bmeluch/NMDC/nom_processing/monet_nom/raw || { echo "batch_and_download.py failed on batch $BATCH_NUM"; exit 1; }

# Delete /home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom/raw_file_info/raw_file_inspection_results.csv
rm -f /home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom/raw_file_info/raw_file_inspection_results.csv

# Edit /home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom/monet_nom_config.json to set the following values to false:
CONFIG_FILE="/home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom/monet_nom_config.json"
jq --arg today "$(date +%Y%m%d)" \
    '.skip_triggers.raw_data_inspected = false
    | .skip_triggers.metadata_mapping_generated = false
    | .skip_triggers.data_processed = false
    | .skip_triggers.processed_data_uploaded_to_minio = false
    | .skip_triggers.metadata_packages_generated = false
    | .workflow.processed_data_date_tag = $today' \
    "$CONFIG_FILE" > "${CONFIG_FILE}.tmp" && mv "${CONFIG_FILE}.tmp" "$CONFIG_FILE"

# Run the monet_nom workflow
python workflows/monet_nom/run_workflow.py || { echo "run_workflow.py failed on batch $BATCH_NUM"; exit 1; }

# Back up generated JSONs
cp /home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom/metadata/nmdc_submission_packages/workflow_metadata_di_nom_bruker.json "/home/bmeluch/NMDC/nom_processing/monet_nom_json_backup/workflow_metadata_di_nom_bruker_${BATCH_NUM}.json"

cp /home/bmeluch/NMDC/nmdc-mass-spec-automation/workflows/monet_nom/metadata/nmdc_submission_packages/workflow_metadata_di_nom_bruker.json "/mnt/c/Users/melu925/OneDrive - PNNL/Documents/NMDC/MONet_ingest/json_backups/workflow_metadata_di_nom_bruker_${BATCH_NUM}.json"
