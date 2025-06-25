import os
import yaml
from pathlib import Path
from typing import Dict, Any

# Path to config file
CONFIG_FILE_PATH = Path(os.path.dirname(os.path.dirname(__file__))) / "config.yaml"

# Default configuration values
DEFAULT_CONFIG = {
    "search_area_granularity": "район",  # district level
    "area_search_radius": 7,  # km
    "daily_check_enabled": True,
    "gpt_analysis_enabled": True,
    "advantage_price_threshold": -30,  # % threshold for highlighting profitable lots
    "market_yield_threshold": 10,  # % threshold for recommendation
    "telegram_bot_token": "7927196434:AAFFuvxIGSI3IWnkYbyNrEUPUAhdVsvoEnQ",  # Telegram bot token
    "telegram_notifications_enabled": True,  # Enable/disable telegram notifications
    "market_deviation_threshold": -15,  # % threshold for market deviation notifications
    "min_capitalization_threshold": 500000,  # Minimum capitalization for notifications (rubles)
    "gpt_prompt_template": """
You are a real estate classification expert. Based on the property description and details, classify it into one of these categories:

1. Street retail (with entrance from street):
   - 70-120 m²
   - 120-250 m²
   - 250-500 m²
   - 500-1000 m²
   - 1000-1500 m²
   - 1500+ m²

2. Offices:
   - 1000-3500 m²

3. Standalone building:
   - Any size

4. Industrial premises:
   - Up to 1000 m²
   - 1000-3000 m²
   - 3000-5000 m²

5. Commercial land:
   - 1+ hectare

Additionally, indicate:
- If there's any mention of basement or semi-basement
- If there's any indication this is the top floor (keywords: "last floor", "top floor", "mansard")

Property details:
- Name: {name}
- Description: {description}
- Area: {area} m²
- Property category: {category}

Format your answer as JSON:
{{"category": "category_name", "size_category": "size_range", "has_basement": true/false, "is_top_floor": true/false}}
"""
}

def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file or create with defaults if it doesn't exist."""
    if CONFIG_FILE_PATH.exists():
        with open(CONFIG_FILE_PATH, "r") as f:
            try:
                config = yaml.safe_load(f)
                # Merge with defaults to ensure all keys exist
                return {**DEFAULT_CONFIG, **config}
            except yaml.YAMLError as e:
                print(f"Error parsing config file: {e}")
                return DEFAULT_CONFIG
    else:
        # Create config file with defaults
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG

def save_config(config: Dict[str, Any]) -> None:
    """Save configuration to YAML file."""
    with open(CONFIG_FILE_PATH, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

def update_config(key: str, value: Any) -> Dict[str, Any]:
    """Update a specific config value and save."""
    config = load_config()
    config[key] = value
    save_config(config)
    return config

# Export the configuration
CONFIG = load_config()