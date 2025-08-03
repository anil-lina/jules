import json
from datetime import datetime

STATE_FILE = "state.json"

def get_last_timestamp(table_name: str) -> str:
    """
    Reads the state file and returns the last timestamp for the given table.
    If the table is not found, it returns a default old timestamp.
    """
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
            return state.get(table_name, "1970-01-01 00:00:00")
    except FileNotFoundError:
        return "1970-01-01 00:00:00"

def update_last_timestamp(table_name: str, timestamp: str):
    """
    Updates the state file with the new timestamp for the given table.
    """
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
    except FileNotFoundError:
        state = {}

    state[table_name] = timestamp

    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)
