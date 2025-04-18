import backoff
import logging
import os
import requests
import uuid

from clients import AuthenticationClient, SessionManager
from clients import ImportClient, ImportFile
from clients import SessionManager
from clients import WorkflowClient, WorkflowInstance
from constants import VIEWER_ASSET_CONFIG_FILE


from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Value, Lock

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

log = logging.getLogger()

"""
Uses the Pennsieve API to initialize and upload viewer asset files
for import into Pennsieve data ecosystem.

"""

def import_viewer_assets(api_host, api2_host, api_key, api_secret, workflow_instance_id, file_directory):
    # gather all the time series files from the output directory
    viewer_asset_files = []
    viewer_config_file = ""

    for root, _, files in os.walk(file_directory):
        for file in files:
            log.info(file)
            if file == VIEWER_ASSET_CONFIG_FILE:
                viewer_config_file = os.path.join(root, file)
            else:
                viewer_asset_files.append(os.path.join(root, file))

    log.info(len(viewer_asset_files))
    if len(viewer_asset_files) == 0 or len(viewer_config_file) == 0:
        log.error("Incorrect input data")
        return None

    # authentication against the Pennsieve API
    authorization_client = AuthenticationClient(api_host)
    session_manager = SessionManager(authorization_client, api_key, api_secret)

    # fetch workflow instance for parameters (dataset_id, package_id, etc.)
    workflow_client = WorkflowClient(api2_host, session_manager)
    workflow_instance  = workflow_client.get_workflow_instance(workflow_instance_id)

    # constraint until we implement (upstream) performing imports over directories
    # and specifying how to group time series files together into an imported package
    assert len(workflow_instance.package_ids) == 1, "Viewer Asset Import processor only supports a single package for import"
    package_id = workflow_instance.package_ids[0]

    log.info(f"dataset_id={workflow_instance.dataset_id} package_id={package_id} starting import of viewer assets")


    import_files = []
    for file_path in viewer_asset_files:
        import_file = ImportFile(
            upload_key = uuid.uuid4(),
            file_path  = os.path.basename(file_path),
            local_path = file_path
        )
        import_files.append(import_file)

    # initialize import
    import_client = ImportClient(api2_host, session_manager)
    import_id = import_client.create(workflow_instance.id, workflow_instance.dataset_id, package_id, import_files)

    log.info(f"import_id={import_id} initialized import with {len(import_files)} viewer asset files for upload")

    # track time series file upload count
    upload_counter = Value('i', 0)
    upload_counter_lock = Lock()

    # upload time series files to Pennsieve S3 import bucket
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5
    )
    def upload_asset_file(viewer_asset_file):
        try:
            with upload_counter_lock:
                upload_counter.value += 1
                log.info(f"import_id={import_id} upload_key={viewer_asset_file.upload_key} uploading {upload_counter.value}/{len(import_files)} {viewer_asset_file.local_path}")
            upload_url = import_client.get_presign_url(import_id, workflow_instance.dataset_id, viewer_asset_file.upload_key)
            with open(viewer_asset_file.local_path, 'rb') as f:
                response = requests.put(upload_url, data=f)
                response.raise_for_status()  # raise an error if the request failed
            return True
        except Exception as e:
            with upload_counter_lock:
                upload_counter.value -= 1
            log.error(f"import_id={import_id} upload_key={viewer_asset_file.upload_key} failed to upload {viewer_asset_file.local_path}: %s", e)
            raise e

    successful_uploads = list()
    with ThreadPoolExecutor(max_workers=4) as executor:
        # wrapping in a list forces the executor to wait for all threads to finish uploading time series files
        successful_uploads = list(executor.map(upload_asset_file, import_files))

    log.info(f"import_id={import_id} uploaded {upload_counter.value} time series files")

    assert sum(successful_uploads) == len(import_files), "Failed to upload all time series files"
