import os
import logging

from config import Config
from processor.importer import import_viewer_assets

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

log = logging.getLogger()

if __name__ == "__main__":
    config = Config()
    importer = import_viewer_assets(config.API_HOST, config.API_HOST2, config.API_KEY, config.API_SECRET, config.WORKFLOW_INSTANCE_ID, config.INPUT_DIR)
