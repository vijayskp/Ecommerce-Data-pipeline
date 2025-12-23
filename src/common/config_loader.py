import yaml
from pathlib import Path

def load_config(config_path: str = "configs/config.yaml") -> dict:
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")
    with open(config_file, "r") as f:
        return yaml.safe_load(f)
