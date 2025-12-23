import yaml
import mysql.connector
from pathlib import Path


def load_config():
    config_path = Path(__file__).resolve().parents[2] / "configs" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def test_mysql_connection():
    config = load_config()
    mysql_cfg = config["mysql"]

    conn = mysql.connector.connect(
        host=mysql_cfg["host"],
        port=mysql_cfg["port"],
        user=mysql_cfg["user"],
        password=mysql_cfg["password"],
        database=mysql_cfg["database"]
    )

    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()

    print("MySQL connection successful. Result:", result)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    test_mysql_connection()
