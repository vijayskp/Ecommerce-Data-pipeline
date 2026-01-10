from datetime import datetime
from src.common.config_loader import load_config
from src.load.watermark_utils import get_last_loaded_at, update_last_loaded_at

if __name__ == "__main__":
    config = load_config()
    sf_cfg = config["snowflake"]

    table_name = "customers"

    print("Before update:")
    current = get_last_loaded_at(table_name, sf_cfg)
    print("Existing watermark:", current)

    now_ts = datetime.utcnow()
    print("Updating watermark to:", now_ts)
    update_last_loaded_at(table_name, now_ts, sf_cfg)

    print("After update:")
    current = get_last_loaded_at(table_name, sf_cfg)
    print("New watermark:", current)
