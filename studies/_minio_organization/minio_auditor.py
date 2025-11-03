# This script is a helper script to see what data are on minio

# You will need to have the following environment variables set:
# MINIO_ACCESS_KEY
# MINIO_SECRET_KEY

import os
from minio import Minio
from minio.error import S3Error
from tqdm import tqdm

def get_list_of_files_on_minio(minio_client, bucket_name):
    """Retrieve and print the list of files in the specified Minio bucket."""
    files = minio_client.list_objects(bucket_name, recursive=True)
    return files

if __name__ == '__main__':
    minio_client = Minio("admin.nmdcdemo.emsl.pnl.gov",
                         access_key=os.environ["MINIO_ACCESS_KEY"],
                         secret_key=os.environ["MINIO_SECRET_KEY"],
                         secure=True)
    bucket_name = "metabolomics"  # Specify the bucket name
    # Check if the bucket exists
    if not minio_client.bucket_exists(bucket_name):
        print(f"Bucket {bucket_name} does not exist.")
    else:
        files = get_list_of_files_on_minio(minio_client, bucket_name)
    files_out = [x.object_name for x in files]
    print("Finished")
