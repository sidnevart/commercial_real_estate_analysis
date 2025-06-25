#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CIAN Street ID Parser

This script extracts street IDs from CIAN for Moscow and Moscow Oblast addresses
using CIAN's geocoding API.
"""

from multiprocessing import process
import os
import re
import time
import logging
import json
import random
from pathlib import Path
import argparse
from typing import Dict, Optional, List, Tuple, Any

import fuzzywuzzy
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
from fake_useragent import UserAgent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("cian_street_parser.log")
    ]
)
logger = logging.getLogger("cian_street_parser")
log = logging.getLogger("cian_street_parser")

# Add these imports to the top of cian_minimal.py
from pathlib import Path
from typing import Dict, Optional, Union

# Add these constants after the other CIAN URLs
STREETS_FILE = Path("parser/data/cian_street_ids_simple.json")
STREETS_DETAILED_FILE = Path("parser/data/cian_streets_database.json")

# Global variable to store street mapping
street_name_to_id_mapping: Dict[str, str] = {}

def load_street_ids() -> Dict[str, str]:
    """Load street ID mapping from file"""
    global street_name_to_id_mapping
    
    if street_name_to_id_mapping:
        # Already loaded
        return street_name_to_id_mapping
    
    try:
        if STREETS_FILE.exists():
            with open(STREETS_FILE, "r", encoding="utf-8") as f:
                street_name_to_id_mapping = json.load(f)
                log.info(f"Loaded {len(street_name_to_id_mapping)} street IDs from {STREETS_FILE}")
        else:
            log.warning(f"Street ID mapping file not found: {STREETS_FILE}")
            street_name_to_id_mapping = {}
    except Exception as e:
        log.error(f"Error loading street IDs: {e}")
        street_name_to_id_mapping = {}
        
    return street_name_to_id_mapping

# Output directories and files
OUTPUT_DIR = Path("parser/data")
STREETS_FILE = OUTPUT_DIR / "cian_streets_database.json"

# CIAN API URLs
CIAN_MAIN_URL = "https://cian.ru/"
CIAN_GEOCODE = "https://www.cian.ru/api/geo/geocode-cached/?request={}"
CIAN_GEOCODE_FOR_SEARCH = "https://www.cian.ru/api/geo/geocoded-for-search/"

# Test addresses for parsing
TEST_ADDRESSES = [
    "г Москва, Пресненская набережная, дом 12",
    "Московская область, городской округ Химки, город Химки, квартал Международный, ул. Загородная, дом 4",
    "Москва, СВАО, район Останкинский, проспект Мира, 119с536",
    "г Москва, ул Тверская, дом 7",
    "обл Московская, г Домодедово, мкр Северный, ул Советская, дом 50",
    "Зеленоград, корпус 847",
    "г. Москва, улица Обручева, 27к1",
    "Москва, улица 1905 года, 10с1",
    "Московская область, г. Королев, проспект Космонавтов, 20А",
]

# Collection to store street IDs
streets_data = {
    "moscow": {
        "streets": {}  # Maps street name to ID
    },
    "mo": {
        "streets": {}  # Maps street name to ID
    },
    "metadata": {
        "last_updated": "",
        "streets_count": 0
    }
}


class CianStreetParser:
    def __init__(self, headless=True):
        """Initialize the CIAN parser with a Chrome driver"""
        self.driver = None
        self.initialize_driver(headless)
        
    def __del__(self):
        """Clean up resources when the object is destroyed"""
        self.close_driver()
    
    def initialize_driver(self, headless=True):
        """Initialize Chrome driver with appropriate settings"""
        logger.info("Initializing Chrome driver...")
        
        try:
            # Close existing driver if there is one
            self.close_driver()
            
            # Configure Chrome options
            options = uc.ChromeOptions()
            options.add_argument(f"--user-agent={UserAgent(browsers=['Chrome']).random}")
            options.add_argument("--disable-blink-features=AutomationControlled")
            
            if headless:
                options.add_argument("--headless")
            
            options.page_load_strategy = "eager"
            
            # Create driver
            self.driver = uc.Chrome(options=options)
            self.driver.set_page_load_timeout(30)
            
            # Load CIAN main page to initialize session
            self.driver.get(CIAN_MAIN_URL)
            time.sleep(2)
            
            logger.info("Chrome driver initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing driver: {e}")
            # Add recursive retry with longer delay
            time.sleep(5)
            self.initialize_driver(headless)
    
    def close_driver(self):
        """Close the browser driver"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                logger.info("Driver closed")
            except:
                pass
    
    def get_json(self, url: str, retries: int = 0) -> dict:
        """Get JSON data from a URL with retry mechanism"""
        try:
            self.driver.get(url)
            
            # Wait for JSON response
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "pre"))
            )
            
            # Parse JSON data
            try:
                raw = self.driver.find_element(By.TAG_NAME, "pre").text
                return json.loads(raw)
            except Exception as e:
                logger.error(f"Error parsing JSON from {url}: {e}")
                
                # Retry
                if retries < 3:
                    time.sleep(2)
                    return self.get_json(url, retries + 1)
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching data from {url}: {e}")
            
            # Re-initialize driver and retry
            if retries < 3:
                self.initialize_driver()
                time.sleep(2)
                return self.get_json(url, retries + 1)
            return {}
    def find_street_id(self, street_name: str) -> Optional[str]:
        """
        Find street ID from the cached mapping
        
        Args:
            street_name: Street name to look up
            
        Returns:
            Street ID if found, None otherwise
        """
        if not street_name:
            return None
        
        # Load street mapping if not already loaded
        street_mapping = load_street_ids()
        if not street_mapping:
            return None
        
        # Normalize street name for lookup
        street_name_lower = street_name.lower().strip()
        
        # Try direct match first
        for full_name, street_id in street_mapping.items():
            # Extract street name without region
            name_part = full_name.split(" (")[0].lower()
            
            # Check for exact match
            if name_part == street_name_lower:
                log.info(f"Found exact street match: '{street_name}' -> ID: {street_id}")
                return street_id
        
        # Try partial match if exact match failed
        for full_name, street_id in street_mapping.items():
            name_part = full_name.split(" (")[0].lower()
            
            # Check for inclusion
            if street_name_lower in name_part or name_part in street_name_lower:
                log.info(f"Found partial street match: '{street_name}' ~ '{name_part}' -> ID: {street_id}")
                return street_id
                
        # Try fuzzy matching
        try:
            best_match = process.extractOne(
                street_name_lower,
                [name.split(" (")[0].lower() for name in street_mapping.keys()],
                scorer=fuzzywuzzy.token_sort_ratio,
                score_cutoff=85
            )
            
            if best_match:
                matched_name = best_match[0]
                # Find the original key with this name
                for full_name, street_id in street_mapping.items():
                    if full_name.split(" (")[0].lower() == matched_name:
                        log.info(f"Found fuzzy street match: '{street_name}' ~ '{matched_name}' -> ID: {street_id}")
                        return street_id
        except Exception as e:
            log.warning(f"Error in fuzzy matching for '{street_name}': {e}")
            
        return None
    def post_json(self, url: str, body: dict, retries: int = 0) -> dict:
        """Send POST request and get JSON response"""
        try:
            # Execute JavaScript to submit the form
            self.driver.execute_script(
                """
                function post(path, params) {
                    const form = document.createElement('form');
                    form.method = 'post';
                    form.action = path;
                    
                    for (const key in params) {
                        if (params.hasOwnProperty(key)) {
                            const hiddenField = document.createElement('input');
                            hiddenField.type = 'hidden';
                            hiddenField.name = key;
                            hiddenField.value = params[key];
                            form.appendChild(hiddenField);
                        }
                    }
                    
                    document.body.appendChild(form);
                    form.submit();
                }
                
                post(arguments[1], arguments[0]);
                """, 
                body, url
            )
            
            # Wait for response
            time.sleep(2)
            
            # Parse JSON response
            try:
                raw = self.driver.find_element(By.TAG_NAME, "pre").text
                return json.loads(raw)
            except Exception as e:
                logger.error(f"Error parsing JSON response: {e}")
                
                if retries < 3:
                    time.sleep(2)
                    return self.post_json(url, body, retries + 1)
                return {}
                
        except Exception as e:
            logger.error(f"Error sending POST request to {url}: {e}")
            
            if retries < 3:
                self.initialize_driver()
                time.sleep(2)
                return self.post_json(url, body, retries + 1)
            return {}
    
    def extract_street_id_from_address(self, address: str) -> Optional[str]:
        """
        Extract street ID from an address using CIAN's geocoding API
        
        Args:
            address: Address text to process
            
        Returns:
            Street ID if found, None otherwise
        """
        try:
            # Step 1: Extract street name from address
            street_name = None
            
            # Try common street patterns
            street_patterns = [
                r'(?:улица|ул\.|ул)\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
                r'(?:проспект|пр-т|пр-кт)\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
                r'(?:бульвар|б-р)\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
                r'(?:шоссе|ш\.)\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
                r'(?:переулок|пер\.|пер)\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
                r'(?:набережная|наб\.|наб)\s+([А-Яа-я\-\s]+?)(?:,|\д|$)',
                r'(?:проезд|пр-д)\s+([А-Яа-я\-\s]+?)(?:,|\д|$)',
                r'(?:площадь|пл\.)\s+([А-Яа-я\-\s]+?)(?:,|\д|$)'
            ]
            
            for pattern in street_patterns:
                match = re.search(pattern, address)
                if match:
                    street_name = match.group(1).strip()
                    break
            
            # If we found a street name directly in the address, try to look it up
            if street_name:
                street_id = self.find_street_id(street_name)
                if street_id:
                    return street_id
            
            # Step 2: If direct lookup failed, use geocoding
            geocoding_response = self.get_json(CIAN_GEOCODE.format(address))
            
            if not geocoding_response or "items" not in geocoding_response:
                log.warning(f"No geocoding results for address: {address}")
                return None
            
            # Find first result for Moscow or Moscow Oblast
            geocoding_result = None
            for item in geocoding_response.get("items", []):
                item_text = item.get("text", "")
                if item_text.startswith("Россия, Моск"):  # Moscow or Moscow Oblast
                    geocoding_result = item
                    break
            
            if not geocoding_result:
                log.warning(f"No Moscow/MO results for address: {address}")
                return None
            
            # Get coordinates
            lon, lat = geocoding_result.get("coordinates", [0, 0])
            
            if lon == 0 or lat == 0:
                log.warning(f"Invalid coordinates for address: {address}")
                return None
            
            # Step 3: Get street ID from coordinates
            api_result = self.post_json(
                CIAN_GEOCODE_FOR_SEARCH,
                {"lat": lat, "lng": lon, "kind": "street"}
            )
            
            if not api_result or "details" not in api_result:
                log.warning(f"No street details for address: {address}")
                return None
            
            # Get details from the result
            details = api_result["details"]
            
            # Find street information (usually the last element)
            for i in range(len(details)-1, -1, -1):
                if "id" in details[i] and "fullName" in details[i]:
                    name_lower = details[i]["fullName"].lower()
                    
                    # Check if this is a street
                    street_markers = ["улица", "переулок", "проспект", "проезд", 
                                    "шоссе", "бульвар", "площадь", "набережная"]
                    
                    if any(marker in name_lower for marker in street_markers):
                        street_id = details[i]["id"]
                        street_name = details[i]["fullName"]
                        
                        log.info(f"Found street via geocoding: {street_name} (ID: {street_id})")
                        return street_id
            
            # If no specific street found, try the last element
            if details and "id" in details[-1]:
                street_id = details[-1]["id"]
                street_name = details[-1].get("fullName", "Unknown")
                
                log.info(f"Using last element as street: {street_name} (ID: {street_id})")
                return street_id
                
        except Exception as e:
            log.error(f"Error extracting street ID for address {address}: {e}")
        
        return None


def save_streets_data():
    """Save the collected streets data to a JSON file"""
    # Create output directory if it doesn't exist
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Update metadata
    streets_data["metadata"]["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
    streets_data["metadata"]["streets_count"] = (
        len(streets_data["moscow"]["streets"]) + 
        len(streets_data["mo"]["streets"])
    )
    
    # Save the data
    with open(STREETS_FILE, "w", encoding="utf-8") as f:
        json.dump(streets_data, f, ensure_ascii=False, indent=2)
    
    # Create a simplified version with just street name -> ID mapping
    simplified = {}
    for street_name, street_id in streets_data["moscow"]["streets"].items():
        simplified[f"{street_name} (Москва)"] = street_id
    
    for street_name, street_id in streets_data["mo"]["streets"].items():
        simplified[f"{street_name} (МО)"] = street_id
    
    with open(OUTPUT_DIR / "cian_street_ids_simple.json", "w", encoding="utf-8") as f:
        json.dump(simplified, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Saved {streets_data['metadata']['streets_count']} streets to {STREETS_FILE}")


def load_streets_data():
    """Load existing streets data from the JSON file"""
    global streets_data
    
    if not STREETS_FILE.exists():
        logger.info(f"No existing streets data file found at {STREETS_FILE}")
        return False
    
    try:
        with open(STREETS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if not isinstance(data, dict) or "moscow" not in data or "mo" not in data:
            logger.warning("Invalid data format in streets data file")
            return False
        
        streets_data = data
        logger.info(f"Loaded {streets_data['metadata']['streets_count']} streets from {STREETS_FILE}")
        return True
    
    except Exception as e:
        logger.error(f"Error loading streets data: {e}")
        return False


def process_addresses(addresses: List[str], parser: CianStreetParser):
    """Process a list of addresses to extract street IDs"""
    for address in addresses:
        # Sleep to avoid rate limiting
        time.sleep(random.uniform(1, 3))
        
        street_id, street_name, region_type = parser.extract_street_id_from_address(address)
        
        if street_id and street_name and region_type:
            # Store in the appropriate region
            if region_type == "moscow":
                streets_data["moscow"]["streets"][street_name] = street_id
                logger.info(f"Added Moscow street: {street_name} (ID: {street_id})")
            else:  # mo
                streets_data["mo"]["streets"][street_name] = street_id
                logger.info(f"Added MO street: {street_name} (ID: {street_id})")
        else:
            logger.warning(f"Failed to extract street ID for address: {address}")
    
    # Save after processing all addresses
    save_streets_data()


def main():
    parser = argparse.ArgumentParser(description="Extract CIAN street IDs from addresses")
    parser.add_argument("--file", type=str, help="Path to file with addresses (one per line)")
    parser.add_argument("--address", type=str, help="Single address to process")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    args = parser.parse_args()
    
    # Load existing data
    load_streets_data()
    
    # Create parser
    cian_parser = CianStreetParser(headless=args.headless)
    
    try:
        addresses_to_process = []
        
        if args.file:
            # Process addresses from file
            try:
                with open(args.file, "r", encoding="utf-8") as f:
                    addresses_to_process = [line.strip() for line in f if line.strip()]
                    
                logger.info(f"Loaded {len(addresses_to_process)} addresses from file")
            except Exception as e:
                logger.error(f"Error loading addresses from file: {e}")
                return
        elif args.address:
            # Process single address
            addresses_to_process = [args.address]
        else:
            # Use test addresses
            addresses_to_process = TEST_ADDRESSES
            logger.info("Using test addresses")
        
        # Process addresses
        process_addresses(addresses_to_process, cian_parser)
        
    finally:
        # Clean up
        cian_parser.close_driver()


if __name__ == "__main__":
    main()