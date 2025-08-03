import os
import json
import yaml
from datetime import datetime,timezone
from pathlib import Path
import tempfile
from src.utils.state_manager import get_last_timestamp, update_last_timestamp
from dagster import asset, AssetExecutionContext, DagsterInvariantViolationError,resources
import oracledb
from src.resources import oracle,oci
from oci.object_storage import ObjectStorageClient, UploadManager
from src.utils.state_manager import get_last_timestamp, update_last_timestamp
from  src.utils import config
from  src.utils.config import get_table_configs
#from .. import config

def build_oracle_asset(table_name, is_incremental, cursor_column):
    @asset(
    name=f"{table_name}_oracle_to_oci_sync",
    description=f"Syncs {table_name} from Oracle to OCI Object Storage.",
    required_resource_keys={"oracle", "oci"},
)
    def sync_asset(context: AssetExecutionContext):
        """
        This asset connects to an Oracle DB, retrieves data in chunks,
        and uploads it to an OCI bucket as JSON files.
        """
        oracle = context.resources.oracle
        oci = context.resources.oci

        # Load table configurations from YAML
        with open(config.TABLES_CONFIG_PATH, 'r') as f:
            tables_config = yaml.safe_load(f)

        tables = tables_config.get("tables", [])

        oci_bucket = config.OCI_BUCKET_NAME
        namespace = config.OCI_NAMESPACE
        state_dir = Path("state") # Assuming state is in a 'state' directory at project root.

        #namespace = oci.get_namespace().data
        upload_manager = UploadManager(oci)

        for table_info in tables:
            table_name = table_info["name"]
            is_incremental = table_info.get("incremental", False)
            incremental_column = table_info.get("incremental_column")

            context.log.info(f"Processing table: {table_name}")

            # Build query
            query = f"SELECT * FROM {table_name}"
            last_inc_value = None
            if is_incremental and incremental_column:
                last_inc_value = get_last_timestamp(table_name)
                if last_inc_value:
                    incremental_column_type = table_info.get("incremental_column_type")
                    if incremental_column_type == "timestamp":
                        query += f" WHERE {incremental_column} > to_timestamp(:last_inc_value, 'YYYY-MM-DD HH24:MI:SS.FF6')"
                    else:
                        query += f" WHERE {incremental_column} > :last_inc_value"

                #query += f" ORDER BY {incremental_column}"
            context.log.info(f"{query} for table {table_name}")
            cursor = oracle.cursor()
            if last_inc_value:
                cursor.execute(query, last_inc_value=last_inc_value)
            else:
                cursor.execute(query)

            cursor.arraysize = 300000
            columns = [col[0] for col in cursor.description]

            max_incremental_value = last_inc_value

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path("c:\\tmp\\current\\")
                while True:
                    rows = cursor.fetchmany()
                    if not rows:
                        break

                    context.log.info(f"Fetched {len(rows)} rows from {table_name}.")
                    data = [dict(zip(columns, row)) for row in rows]

                    if is_incremental and incremental_column and data:
                        # Convert to string for comparison to handle both numbers and dates uniformly
                        max_incremental_value = max(str(row[incremental_column]) for row in data if row[incremental_column] is not None)

                    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                    file_name = f"{timestamp}_{table_name}.json"
                    local_file_path = temp_path / file_name

                    with open(local_file_path, 'w') as f:
                        json.dump(data, f, indent=4, default=str, ensure_ascii=False)

                    context.log.info(f"Wrote data to {local_file_path}")

                    context.log.info(f"Uploading {file_name} to OCI bucket {oci_bucket}...")
                    upload_manager.upload_file(
                        namespace_name=namespace,
                        bucket_name=oci_bucket,
                        object_name=file_name,
                        file_path=str(local_file_path)
                    )
                    context.log.info(f"Successfully uploaded {file_name}.")
                    os.remove(local_file_path)
                    context.log.info(f"Successfully Deleted {file_name} and freed up you lots of space")

                if is_incremental and max_incremental_value:
                    update_last_timestamp(table_name, max_incremental_value)
                    context.logger.info(f"Updated last timestamp for table {table_name} to {max_incremental_value}.")

            cursor.close()
        context.log.info("Sync complete.")
        context.log.info("Sir, Done Sir")
    return sync_asset

def load_oracle_assets():
    table_configs = get_table_configs()
    return [build_oracle_asset(config["name"], config["incremental"], config.get("cursor_column")) for config in table_configs]
