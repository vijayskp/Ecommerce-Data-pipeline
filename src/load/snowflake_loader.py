# src/load/snowflake_loader.py

from typing import List, Tuple, Dict, Any
from src.common.db_connections import get_snowflake_conn


def full_load_to_raw(
    columns: List[str],
    rows: List[Tuple],
    table_cfg: Dict[str, Any],
    sf_cfg: Dict[str, Any],
) -> None:
    """
    Perform a full load into a Snowflake RAW table:

    1. TRUNCATE the target table (remove all existing data).
    2. Insert all rows from the given dataset.

    Args:
        columns: list of column names from MySQL (must align with Snowflake RAW table columns).
        rows: list of tuples, each tuple is one record.
        table_cfg: table configuration dict with 'target_table', etc.
        sf_cfg: Snowflake configuration dict from config.yaml.
    """
    target_table = table_cfg["target_table"]

    if not rows:
        print(f"[FULL LOAD] No rows provided for table {target_table}. Skipping.")
        return

    print(f"[FULL LOAD] Starting full load into {target_table} ...")
    print(f"[FULL LOAD] Number of rows to insert: {len(rows)}")

    conn = get_snowflake_conn(sf_cfg)
    cur = conn.cursor()

    try:
        # 1. Truncate the target table
        truncate_sql = f"TRUNCATE TABLE {target_table}"
        print(f"[FULL LOAD] Executing: {truncate_sql}")
        cur.execute(truncate_sql)

        # 2. Insert all rows
        col_list = ", ".join(columns)
        # snowflake-connector-python uses %s placeholders
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {target_table} ({col_list}) VALUES ({placeholders})"

        print(f"[FULL LOAD] Inserting rows into {target_table} ...")
        cur.executemany(insert_sql, rows)

        conn.commit()
        print(f"[FULL LOAD] Completed full load into {target_table}. Rows inserted: {len(rows)}")

    except Exception as e:
        conn.rollback()
        print(f"[FULL LOAD] Error during full load into {target_table}: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def incremental_upsert_to_raw(
    columns: List[str],
    rows: List[Tuple],
    table_cfg: Dict[str, Any],
    sf_cfg: Dict[str, Any],
) -> None:
    """
    Upsert incremental rows into RAW table using a TEMP staging table + MERGE.

    - columns: list of column names from source/MySQL
    - rows: list of tuples with data
    - table_cfg: config entry for this table (includes target_table, primary_key)
    - sf_cfg: Snowflake config
    """
    if not rows:
        print(f"[INCREMENTAL] No rows to load for {table_cfg['name']}.")
        return

    conn = get_snowflake_conn(sf_cfg)
    cur = conn.cursor()

    target_table = table_cfg["target_table"]
    pk = table_cfg["primary_key"]
    temp_table = f"{target_table}_STAGE"

    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    try:
        print(f"[INCREMENTAL] Creating temp staging table {temp_table}...")
        # 1. Create temp table with same structure as target (no data)
        cur.execute(
            f"CREATE OR REPLACE TEMP TABLE {temp_table} AS "
            f"SELECT * FROM {target_table} WHERE 1=0"
        )

        # 2. Insert new/changed rows into temp table
        insert_sql = f"INSERT INTO {temp_table} ({col_list}) VALUES ({placeholders})"
        cur.executemany(insert_sql, rows)

        print(f"[INCREMENTAL] Inserted {len(rows)} rows into {temp_table}.")

        # 3. Build MERGE statement
        #    ON condition = primary key
        on_cond = f"t.{pk} = s.{pk}"

        #    SET clause for all non-PK columns
        set_clause_list = []
        for c in columns:
            if c == pk:
                continue
            set_clause_list.append(f"t.{c} = s.{c}")
        set_clause = ", ".join(set_clause_list)

        #    INSERT column list + VALUES from source
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"s.{c}" for c in columns])

        merge_sql = f"""
            MERGE INTO {target_table} AS t
            USING {temp_table} AS s
            ON {on_cond}
            WHEN MATCHED THEN
              UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_columns})
              VALUES ({insert_values});
        """

        print(f"[INCREMENTAL] Running MERGE into {target_table}...")
        cur.execute(merge_sql)
        conn.commit()
        print(f"[INCREMENTAL] Upsert completed for {target_table}. Rows processed: {len(rows)}")

    finally:
        cur.close()
        conn.close()
