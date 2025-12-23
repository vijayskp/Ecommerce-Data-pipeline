from typing import Any, Dict
import mysql.connector
import snowflake.connector

def get_mysql_conn(mysql_cfg: Dict[str, Any]):
    return mysql.connector.connect(
        host=mysql_cfg["host"],
        port=mysql_cfg["port"],
        user=mysql_cfg["user"],
        password=mysql_cfg["password"],
        database=mysql_cfg["database"],
    )

def get_snowflake_conn(sf_cfg: Dict[str, Any]):
    return snowflake.connector.connect(
        user=sf_cfg["user"],
        password=sf_cfg["password"],
        account=sf_cfg["account"],
        warehouse=sf_cfg["warehouse"],
        database=sf_cfg["database"],
        schema=sf_cfg["schema_raw"],
    )
