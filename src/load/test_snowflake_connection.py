import snowflake.connector
import os

def get_snowflake_conn():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="WH_ETL_DEV",
        database="CUSTOMER360_DEV",
        schema="RAW"
    )
    return conn

if __name__ == "__main__":
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_TIMESTAMP()")
    print(cur.fetchone())
    cur.close()
    conn.close()
