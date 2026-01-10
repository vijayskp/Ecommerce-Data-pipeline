# src/extract/manual_extract_test.py

from typing import Dict, Any, List, Tuple

from src.common.config_loader import load_config
from src.extract.mysql_extractor import fetch_full_table


def print_sample_rows(columns: List[str], rows: List[Tuple], sample_size: int = 5) -> None:
    """
    Pretty-print the first few rows returned from MySQL.
    """
    print("\nColumns:")
    print(", ".join(columns))

    print(f"\nShowing first {min(sample_size, len(rows))} rows:\n")

    for i, row in enumerate(rows[:sample_size], start=1):
        print(f"Row {i}:")
        for col_name, value in zip(columns, row):
            print(f"  {col_name}: {value}")
        print("-" * 40)


def main() -> None:
    # 1. Load config
    config: Dict[str, Any] = load_config("configs/config.yaml")

    # 2. Get MySQL config and table config for 'customers'
    mysql_cfg: Dict[str, Any] = config["mysql"]

    # find table config where name == "customers"
    table_cfg: Dict[str, Any] = next(
        t for t in config["tables"] if t["name"] == "customers"
    )

    print("Using MySQL configuration:")
    print(mysql_cfg)
    print("\nUsing table configuration for 'customers':")
    print(table_cfg)

    # 3. Call fetch_full_table
    print("\nFetching full table data from MySQL (customers)...")
    columns, rows = fetch_full_table(table_cfg, mysql_cfg)

    # 4. Print row count
    print(f"\nTotal rows fetched from customers: {len(rows)}")

    # 5. Print a few sample rows
    if rows:
        print_sample_rows(columns, rows, sample_size=5)
    else:
        print("\nNo rows returned. Please check if the customers table has data.")


if __name__ == "__main__":
    main()
