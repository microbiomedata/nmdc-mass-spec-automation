# This script is a helper script to load the metadata files to the minio server
# Point it to the directory where the data you'd like to upload is and to the bucket you'd like to upload it to

# You will need to have the following environment variables set:
# MINIO_ACCESS_KEY
# MINIO_SECRET_KEY

import os
from minio import Minio
from minio.error import S3Error
from tqdm import tqdm

def upload_files(minio_client, bucket_name, directory, folder_name):
    # Collect all files
    all_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, directory)
            object_name = os.path.join(folder_name, relative_path)
            all_files.append((file_path, object_name))

    # Upload files with a single tqdm progress bar
    for file_path, object_name in tqdm(all_files, desc="Uploading files"):
        # Check if object already exists with this name
        try:
            minio_client.stat_object(bucket_name, object_name)
            tqdm.write(f"Object {object_name} already exists. Skipping.")
            continue
        except S3Error as e:
            if e.code != 'NoSuchKey':
                tqdm.write(f"Error checking if object exists: {e}")
                continue
        try:
            minio_client.fput_object(bucket_name, object_name, file_path)
            tqdm.write(f"Uploaded {file_path} to {bucket_name}/{object_name}")
        except S3Error as e:
            tqdm.write(f"Failed to upload {file_path} to {bucket_name}/{object_name}")
            tqdm.write(str(e))
    print("Upload complete or skipped all files")

if __name__ == '__main__':
    minio_client = Minio("admin.nmdcdemo.emsl.pnl.gov",
                         access_key=os.environ["MINIO_ACCESS_KEY"],
                         secret_key=os.environ["MINIO_SECRET_KEY"],
                         secure=True)
    bucket_name = "metabolomics"  # Specify the bucket name
    folder_name = "bioscales_11_r2h77870/processed_20251013"  # Specify the folder name within the bucket
    directory = "/Users/heal742/LOCAL/staging_processed/processed_20251013"
    # Check if the bucket exists
    if not minio_client.bucket_exists(bucket_name):
        print(f"Bucket {bucket_name} does not exist.")
    else:
        upload_files(minio_client, bucket_name, directory, folder_name)
