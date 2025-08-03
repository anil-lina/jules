import os
import yaml
from dotenv import load_dotenv

# Load environment variables from a .env file for local development
load_dotenv()

# Oracle Database Credentials
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_HOST = os.getenv("ORACLE_HOST")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", 1521))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME")

# OCI Credentials
OCI_USER_OCID = os.getenv("OCI_USER_OCID")
OCI_FINGERPRINT = os.getenv("OCI_FINGERPRINT")
OCI_KEY_FILE = os.getenv("OCI_KEY_FILE")
OCI_TENANCY_OCID = os.getenv("OCI_TENANCY_OCID")
OCI_REGION = os.getenv("OCI_REGION")

# OCI Bucket
OCI_BUCKET_NAME = os.getenv("OCI_BUCKET_NAME")
OCI_NAMESPACE = os.getenv("OCI_NAMESPACE")

# Asset Configuration
TABLES_CONFIG_PATH = "tables.yaml"

def get_table_configs():
    """Loads table configurations from tables.yaml."""
    with open(TABLES_CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)
