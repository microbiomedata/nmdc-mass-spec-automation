curl -X 'POST' \
  'https://api-dev.microbiomedata.org/metadata/json:submit' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyOmthdGhlcmluZS5oZWFsIiwiZXhwIjoxNzYxOTUyMTc2fQ.Sw6mo2bJOI36qq7_J-iu7IICpot8m3ERf3Wpr7Q5MQE' \
  -H 'Content-Type: application/json' \
  -d @_bioscales_lcms_metabolomics/metadata/workflow_metadata_HILIC_neg.json

curl -X 'POST' \
  'https://api-dev.microbiomedata.org/metadata/json:submit' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyOmthdGhlcmluZS5oZWFsIiwiZXhwIjoxNzYxOTUyMTc2fQ.Sw6mo2bJOI36qq7_J-iu7IICpot8m3ERf3Wpr7Q5MQE' \
  -H 'Content-Type: application/json' \
  -d @_bioscales_lcms_metabolomics/metadata/workflow_metadata_HILIC_pos.json

curl -X 'POST' \
  'https://api-dev.microbiomedata.org/metadata/json:submit' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyOmthdGhlcmluZS5oZWFsIiwiZXhwIjoxNzYxOTUyMTc2fQ.Sw6mo2bJOI36qq7_J-iu7IICpot8m3ERf3Wpr7Q5MQE' \
  -H 'Content-Type: application/json' \
  -d @_bioscales_lcms_metabolomics/metadata/workflow_metadata_RP_neg.json

curl -X 'POST' \
  'https://api-dev.microbiomedata.org/metadata/json:submit' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyOmthdGhlcmluZS5oZWFsIiwiZXhwIjoxNzYxOTUyMTc2fQ.Sw6mo2bJOI36qq7_J-iu7IICpot8m3ERf3Wpr7Q5MQE' \
  -H 'Content-Type: application/json' \
  -d @_bioscales_lcms_metabolomics/metadata/workflow_metadata_RP_pos.json
