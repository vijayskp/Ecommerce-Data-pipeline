from typing import Dict, Any, List, Tuple
from datetime import datetime
from src.common.db_connections import get_mysql_conn

def fetch_full_table(table_cfg: Dict[str, Any], mysql_cfg: Dict[str, Any]) -> List[Tuple]:
    conn = get_mysql_conn(mysql_cfg)
    cur = conn.cursor()
    sql = f"SELECT * FROM {table_cfg['source_table']}"
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return columns, rows

def fetch_incremental_rows(
    table_cfg: Dict[str, Any],
    mysql_cfg: Dict[str, Any],
    last_loaded_at: datetime,
) -> List[Tuple]:
    conn = get_mysql_conn(mysql_cfg)
    cur = conn.cursor()
    incr_col = table_cfg["incremental_column"]
    sql = f"""
        SELECT * FROM {table_cfg['source_table']}
        WHERE {incr_col} > %s
    """
    cur.execute(sql, (last_loaded_at,))
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return columns, rows
