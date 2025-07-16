"""
# This script will walk over all directories in a directory and look for folder names that end in .corems

Then it will move the whole directory that ends in .corems to a new directory set in the script, including the contents
"""

import os
from pathlib import Path
import shutil

def move_corems_dirs(input_dir, output_dir):
    """
    Move directories ending with .corems from input_dir to output_dir.
    
    :param input_dir: Directory to search for .corems directories.
    :param output_dir: Directory where .corems directories will be moved.
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    if not output_path.exists():
        output_path.mkdir(parents=True, exist_ok=True)

    i=1
    for dirpath in input_path.rglob('*'):
        if dirpath.is_dir() and dirpath.name.endswith('.corems'):
            # Check that there is a .csv within the directory
            csv_files = list(dirpath.glob('*.csv'))
            if not csv_files:
                print(f"No .csv files found in {dirpath}, skipping.")
                continue
            new_location = output_path / dirpath.name
            shutil.move(str(dirpath), str(new_location))
            print(f"Moved {dirpath} to {new_location}, folder number {i}")
            i += 1

if __name__ == "__main__":
    # Must be set for the script to run
    input_dir = "/Users/heal742/LOCAL/05_NMDC/02_MetaMS/metams"
    output_dir = "/Users/heal742/Library/CloudStorage/OneDrive-PNNL/Documents/_DMS_data/_NMDC/_massive/_emp500_lcms/processed_20250716"

    move_corems_dirs(input_dir, output_dir)
