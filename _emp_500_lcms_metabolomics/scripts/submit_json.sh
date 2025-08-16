curl -X 'POST' \
  'https://api-dev.microbiomedata.org/metadata/json:submit' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer your_token_here' \
  -H 'Content-Type: application/json' \
  -d @./workflow_metadata_batch1_202508.json
