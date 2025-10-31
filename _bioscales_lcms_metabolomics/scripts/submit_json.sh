curl -X 'POST' \
  'https://api.microbiomedata.org/metadata/json:submit' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer my_token' \
  -H 'Content-Type: application/json' \
  -d @_bioscales_lcms_metabolomics/metadata/workflow_metadata_HILIC_neg.json

curl -X 'POST' \
  'https://api.microbiomedata.org/metadata/json:submit' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer my_token' \
  -H 'Content-Type: application/json' \
  -d @_bioscales_lcms_metabolomics/metadata/workflow_metadata_HILIC_pos.json

curl -X 'POST' \
  'https://api.microbiomedata.org/metadata/json:submit' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer my_token \
  -H 'Content-Type: application/json' \
  -d @_bioscales_lcms_metabolomics/metadata/workflow_metadata_RP_neg.json

curl -X 'POST' \
  'https://api.microbiomedata.org/metadata/json:submit' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer my_token' \
  -H 'Content-Type: application/json' \
  -d @_bioscales_lcms_metabolomics/metadata/workflow_metadata_RP_pos.json
