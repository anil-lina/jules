import oci
from dagster import resource
from .. import config

@resource
def oci_resource(_):
    """
    A Dagster resource for connecting to OCI Object Storage.
    It uses the configuration from the project's config.py file.
    """
    oci_config = {
        "user": config.OCI_USER_OCID,
        "fingerprint": config.OCI_FINGERPRINT,
        "key_file": config.OCI_KEY_FILE,
        "tenancy": config.OCI_TENANCY_OCID,
        "region": config.OCI_REGION,
    }

    # Validate the config
    oci.config.validate_config(oci_config)

    # Create and return the client
    object_storage_client = oci.object_storage.ObjectStorageClient(oci_config)

    yield object_storage_client
