# src/load/manual_full_load_customers.py

from typing import Dict, Any, List, Tuple

from src.common.config_loader import load_config
from src.extract.mysql_extractor import fetch_full_table
from src.load.snowflake_loader import full_load_to_raw


def main() -> None:
    # 1. Load configuration
    config: Dict[str, Any] = load_config("configs/config.yaml")

    mysql_cfg: Dict[str, Any] = config["mysql"]
    sf_cfg: Dict[str, Any] = config["snowflake"]

    # 2. Get table config for 'customers'
    table_cfg: Dict[str, Any] = next(
        t for t in config["tables"] if t["name"] == "customers"
    )

    print("[TEST] MySQL config:")
    print(mysql_cfg)
    print("\n[TEST] Snowflake config (partial):")
    print({
        "account": sf_cfg["account"],
        "user": sf_cfg["user"],
        "warehouse": sf_cfg["warehouse"],
        "database": sf_cfg["database"],
        "schema_raw": sf_cfg["schema_raw"],
    })
    print("\n[TEST] Table config for 'customers':")
    print(table_cfg)

    # 3. Fetch full data from MySQL
    print("\n[TEST] Fetching full customers table from MySQL...")
    columns: List[str]
    rows: List[Tuple]
    columns, rows = fetch_full_table(table_cfg, mysql_cfg)

    print(f"[TEST] Number of rows fetched from MySQL customers: {len(rows)}")
    if rows:
        print("[TEST] First row sample:")
        print(dict(zip(columns, rows[0])))

    # 4. Perform full load into Snowflake RAW
    print("\n[TEST] Loading data into Snowflake RAW.RAW_CUSTOMERS ...")
    full_load_to_raw(columns, rows, table_cfg, sf_cfg)

    print("\n[TEST] Full load complete. Verify row counts in Snowflake.")


if __name__ == "__main__":
    main()
