"""
This script will point to a folder and get the size (in bytes) and checksum (md5) for each file in the folder.
"""

import os
import hashlib

def get_file_size_and_checksum(file_path):
    """Get the size and md5 checksum of a file."""
    size = os.path.getsize(file_path)
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    checksum = hash_md5.hexdigest()
    return size, checksum

if __name__ == "__main__":
    folder_path = "/Users/heal742/LOCAL/staging_processed/emp500_lcms_fix_1sample/4E8_1_68_thomas-18-s057-a03.corems"  # Change this to your target folder
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            size, checksum = get_file_size_and_checksum(file_path)
            print(f"File: {filename}, Size: {size} bytes, MD5 Checksum: {checksum}")
