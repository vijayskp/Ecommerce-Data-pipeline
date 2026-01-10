# src/load/test_snowflake_connection.py

from src.common.config_loader import load_config
from src.common.db_connections import get_snowflake_conn

def test_snowflake_connection():
    """
    Simple connectivity test for Snowflake.

    Steps:
    - Load config
    - Open connection
    - Run SELECT 1
    - Print result
    """
    config = load_config()
    sf_cfg = config["snowflake"]

    print("Testing Snowflake connection with config:", {k: v for k, v in sf_cfg.items() if k != "password"})

    conn = None
    try:
        conn = get_snowflake_conn(sf_cfg)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        print("Snowflake SELECT 1 result:", result)

        if result and result[0] == 1:
            print("Snowflake connection test SUCCESS.")
        else:
            print("Snowflake connection test returned unexpected result:", result)

        cur.close()
    except Exception as e:
        print("Snowflake connection test FAILED.")
        print("Error:", e)
    finally:
        if conn is not None:
            conn.close()
            print("Snowflake connection closed.")

if __name__ == "__main__":
    test_snowflake_connection()
