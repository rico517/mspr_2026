import shutil
import os
from .debug import debug_print

def export_dataset_to_csv(dataset, filename, output_path):
    """
    Export the cleaned dataset to a CSV file.
    """
    debug_print(f"Exporting dataset to {output_path}/{filename}", level=1)
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    dataset.to_csv(f"{output_path}/{filename}", index=False)
    debug_print("Dataset exported successfully.", level=1)

def zip_folder(path ,zip_name=None):
    """
    Zip the output folder containing the cleaned datasets.
    """
    debug_print(f"\nZipping the output folder: {path}", level=1)
    if zip_name is None:
        zip_name = f"{path}.zip"
    shutil.make_archive(zip_name, 'zip', path)
    debug_print("Output folder zipped successfully.", level=1)