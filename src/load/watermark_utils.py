from typing import Optional
from datetime import datetime
from src.common.db_connections import get_snowflake_conn


def get_last_loaded_at(table_name: str, sf_cfg: dict) -> Optional[datetime]:
    """
    Returns the last_loaded_at timestamp for a given table_name from ETL_WATERMARK,
    or None if no record exists yet.
    """
    conn = get_snowflake_conn(sf_cfg)
    cur = conn.cursor()

    try:
        sql = "SELECT LAST_LOADED_AT FROM ETL_WATERMARK WHERE TABLE_NAME = %s"
        cur.execute(sql, (table_name,))
        row = cur.fetchone()
        if row:
            return row[0]
        return None
    finally:
        cur.close()
        conn.close()


def update_last_loaded_at(table_name: str, last_loaded_at: datetime, sf_cfg: dict) -> None:
    """
    Upserts (MERGE) a record into ETL_WATERMARK for the given table_name.
    If the table exists, update LAST_LOADED_AT; else insert new row.
    """
    conn = get_snowflake_conn(sf_cfg)
    cur = conn.cursor()

    try:
        # Snowflake MERGE using parameters
        merge_sql = """
            MERGE INTO ETL_WATERMARK AS tgt
            USING (SELECT %s AS TABLE_NAME, %s AS LAST_LOADED_AT) AS src
            ON tgt.TABLE_NAME = src.TABLE_NAME
            WHEN MATCHED THEN
              UPDATE SET LAST_LOADED_AT = src.LAST_LOADED_AT
            WHEN NOT MATCHED THEN
              INSERT (TABLE_NAME, LAST_LOADED_AT)
              VALUES (src.TABLE_NAME, src.LAST_LOADED_AT);
        """
        cur.execute(merge_sql, (table_name, last_loaded_at))
        conn.commit()
    finally:
        cur.close()
        conn.close()
