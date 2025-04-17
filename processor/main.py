import os
import logging

from config import Config
from processor.importer import import_viewer_assets

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

log = logging.getLogger()

if __name__ == "__main__":
    config = Config()

    bytes_per_mb = pow(2, 20)
    bytes_per_sample = 8 # 64-bit floating point value
    chunk_size = int(config.CHUNK_SIZE_MB * bytes_per_mb / bytes_per_sample)

    input_files = [
        f.path
        for f in os.scandir(config.INPUT_DIR)
    ]

    assert len(input_files) == 1, "Viewer Asset post processor only supports a single file as input"

    importer = import_viewer_assets(config.API_HOST, config.API_HOST2, config.API_KEY, config.API_SECRET, config.WORKFLOW_INSTANCE_ID, config.OUTPUT_DIR)
