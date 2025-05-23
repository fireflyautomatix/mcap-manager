import os
import json
from pathlib import Path

DEFAULT_CONFIG_DIR = os.path.expanduser("~/.config/mcap_manager")
DEFAULT_CONFIG_FILE = os.path.join(DEFAULT_CONFIG_DIR, "defaults")
DEFAULT_ROOT_DIR = "/var/lib/bags/snapshot_bags"


def ensure_config_dir():
    """Ensure the configuration directory exists."""
    os.makedirs(DEFAULT_CONFIG_DIR, exist_ok=True)


def load_config():
    """Load configuration from the defaults file."""
    ensure_config_dir()

    if not os.path.exists(DEFAULT_CONFIG_FILE):
        # Create default configuration
        config = {"root_dir": DEFAULT_ROOT_DIR}
        save_config(config)
        return config

    try:
        with open(DEFAULT_CONFIG_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        # If there's an error reading the config, create a new one
        config = {"root_dir": DEFAULT_ROOT_DIR}
        save_config(config)
        return config


def save_config(config):
    """Save configuration to the defaults file."""
    ensure_config_dir()
    with open(DEFAULT_CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)


def get_root_dir():
    """Get the configured root directory."""
    config = load_config()
    return config.get("root_dir", DEFAULT_ROOT_DIR)


def set_root_dir(root_dir):
    """Set the root directory in the configuration."""
    config = load_config()
    config["root_dir"] = root_dir
    save_config(config)
