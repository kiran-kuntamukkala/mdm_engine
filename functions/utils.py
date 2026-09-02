import json
import logging
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT_DIR / "config"


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Create and configure a reusable logger for MDM processing."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def load_json_config(filename: str):
    """Load a JSON config file from the configuration directory."""
    config_path = CONFIG_DIR / filename
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def safe_string(value):
    """Convert a value to a meaningful string while handling nulls."""
    if value is None:
        return ""
    return str(value).strip()
