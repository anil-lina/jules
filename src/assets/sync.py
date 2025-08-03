import os
import json
import yaml
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from dagster import asset, AssetExecutionContext
from oci.object_storage import UploadManager
from src.utils.state_manager import get_last_timestamp, update_last_timestamp
from src.utils import config
from src.utils.config import get_table_configs

def build_oracle_asset(table_name: str, is_incremental: bool, cursor_column: str = None):
    @asset(
        name=f"{table_name}_oracle_to_oci_sync",
        description=f"Syncs {table_name} from Oracle to OCI Object Storage.",
        required_resource_keys={"oracle", "oci"},
    )
    def sync_asset(context: AssetExecutionContext):
        """
        This asset connects to an Oracle DB for a specific table, retrieves data in chunks,
        and uploads it to an OCI bucket as JSON files.
        """
        oracle = context.resources.oracle
        oci = context.resources.oci
        upload_manager = UploadManager(oci)

        oci_bucket = config.OCI_BUCKET_NAME
        namespace = config.OCI_NAMESPACE

        context.log.info(f"Processing table: {table_name}")

        # Build query
        query = f"SELECT * FROM {table_name}"
        last_inc_value = None
        if is_incremental and cursor_column:
            last_inc_value = get_last_timestamp(table_name)
            if last_inc_value and last_inc_value != "1970-01-01 00:00:00":
                # Assuming cursor_column is a date/timestamp for simplicity in this example
                # A more robust solution would check column type
                query += f" WHERE {cursor_column} > TO_TIMESTAMP(:last_inc_value, 'YYYY-MM-DD HH24:MI:SS.FF6')"
            query += f" ORDER BY {cursor_column}"

        context.log.info(f"Executing query for table {table_name}: {query}")

        cursor = oracle.cursor()
        if last_inc_value and last_inc_value != "1970-01-01 00:00:00":
            cursor.execute(query, last_inc_value=last_inc_value)
        else:
            cursor.execute(query)

        cursor.arraysize = 300000
        columns = [col[0] for col in cursor.description]
        max_incremental_value = last_inc_value

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            part_num = 0
            while True:
                rows = cursor.fetchmany()
                if not rows:
                    break

                part_num += 1
                context.log.info(f"Fetched {len(rows)} rows from {table_name} (Part {part_num}).")
                data = [dict(zip(columns, row)) for row in rows]

                if is_incremental and cursor_column and data:
                    current_max = max(row[cursor_column] for row in data if row[cursor_column] is not None)
                    if max_incremental_value is None or current_max > datetime.strptime(max_incremental_value, "%Y-%m-%d %H:%M:%S"):
                         max_incremental_value = current_max.strftime("%Y-%m-%d %H:%M:%S")


                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                file_name = f"{timestamp}_{table_name}_part_{part_num}.json"
                local_file_path = temp_path / file_name

                with open(local_file_path, 'w') as f:
                    json.dump(data, f, indent=4, default=str, ensure_ascii=False)

                context.log.info(f"Uploading {file_name} to OCI bucket {oci_bucket}...")
                upload_manager.upload_file(
                    namespace_name=namespace,
                    bucket_name=oci_bucket,
                    object_name=file_name,
                    file_path=str(local_file_path)
                )
                context.log.info(f"Successfully uploaded {file_name}.")
                os.remove(local_file_path)

            if is_incremental and max_incremental_value:
                update_last_timestamp(table_name, max_incremental_value)
                context.log.info(f"Updated last timestamp for table {table_name} to {max_incremental_value}.")

        cursor.close()
        context.log.info(f"Sync complete for table {table_name}.")

    return sync_asset

def load_oracle_assets():
    table_configs = get_table_configs()
    return [build_oracle_asset(config["name"], config.get("incremental", False), config.get("cursor_column")) for config in table_configs]
