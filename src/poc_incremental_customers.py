from datetime import datetime
from src.common.config_loader import load_config
from src.extract.mysql_extractor import fetch_full_table, fetch_incremental_rows
from src.load.snowflake_loader import full_load_to_raw, incremental_upsert_to_raw
from src.load.watermark_utils import get_last_loaded_at, update_last_loaded_at

if __name__ == "__main__":
    config = load_config()
    mysql_cfg = config["mysql"]
    sf_cfg = config["snowflake"]

    # Get table config for customers
    table_cfg = next(t for t in config["tables"] if t["name"] == "customers")
    table_name = table_cfg["name"]
    incr_col = table_cfg["incremental_column"]

    # 1. Read existing watermark
    last_loaded = get_last_loaded_at(table_name, sf_cfg)
    print(f"Current watermark for {table_name}: {last_loaded}")

    if last_loaded is None:
        print("No watermark found. Performing FULL LOAD for initial run...")
        columns, rows = fetch_full_table(table_cfg, mysql_cfg)
        print(f"Fetched {len(rows)} rows from MySQL (full).")
        full_load_to_raw(columns, rows, table_cfg, sf_cfg)

        # Compute max(updated_at) from full data
        if rows:
            idx = columns.index(incr_col)
            max_ts = max((r[idx] for r in rows if r[idx] is not None), default=None)
            if max_ts:
                update_last_loaded_at(table_name, max_ts, sf_cfg)
                print(f"Updated watermark for {table_name} to {max_ts}")
        else:
            print("No rows to set watermark for.")
    else:
        print("Watermark exists. Performing INCREMENTAL LOAD...")
        columns, rows = fetch_incremental_rows(table_cfg, mysql_cfg, last_loaded)
        print(f"Fetched {len(rows)} incremental rows from MySQL since {last_loaded}.")

        if rows:
            incremental_upsert_to_raw(columns, rows, table_cfg, sf_cfg)

            idx = columns.index(incr_col)
            max_ts = max((r[idx] for r in rows if r[idx] is not None), default=last_loaded)
            update_last_loaded_at(table_name, max_ts, sf_cfg)
            print(f"Updated watermark for {table_name} to {max_ts}")
        else:
            print(f"No new rows found for {table_name}. Watermark remains {last_loaded}.")
