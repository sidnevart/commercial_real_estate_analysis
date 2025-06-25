#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CIAN Street ID Collector

This script systematically collects all Moscow streets and their corresponding CIAN IDs,
creating a comprehensive mapping for use in address filtering.
"""

import asyncio
import logging
import time
import json
import os
import string
import random
from typing import Dict, List, Set, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("cian_street_collector")

# Output file
STREETS_JSON_PATH = "parser/data/cian_moscow_streets.json"

class CianStreetCollector:
    def __init__(self, headless=True):
        """Initialize the collector with a Chrome driver"""
        options = Options()
        if headless:
            options.add_argument("--headless")
            
        # Use undetected_chromedriver to bypass detection
        self.driver = uc.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 10)
        
        # Storage for collected streets
        self.streets_map = {}  # Maps street name to ID
        self.processed_prefixes = set()  # Track which prefixes we've processed
        
    def __del__(self):
        """Close the browser when the object is destroyed"""
        try:
            self.driver.quit()
        except:
            pass
    
    def save_streets_json(self):
        """Save the collected streets to JSON file"""
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(STREETS_JSON_PATH), exist_ok=True)
        
        with open(STREETS_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump({
                "streets": self.streets_map,
                "count": len(self.streets_map),
                "updated": time.strftime("%Y-%m-%d %H:%M:%S")
            }, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Saved {len(self.streets_map)} streets to {STREETS_JSON_PATH}")
    
    def extract_street_ids_from_suggestions(self, search_prefix: str) -> Dict[str, str]:
        """
        Extract street IDs from the suggestions dropdown
        Returns a dictionary mapping street names to IDs
        """
        results = {}
        
        try:
            # Navigate to CIAN commercial real estate page
            self.driver.get("https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=offices&region=1")
            time.sleep(2)
            
            # Try to find the address input
            address_input = self.driver.find_element(By.CSS_SELECTOR, "[data-name='GeoSuggestInput'] input")
            address_input.clear()
            
            # Enter our search prefix
            address_input.send_keys(search_prefix)
            
            # Wait for suggestions to appear
            time.sleep(2)
            
            # Collect all street suggestions
            suggestions = self.driver.find_elements(By.CSS_SELECTOR, "[data-name='GeoSuggestItem']")
            
            for suggestion in suggestions:
                suggestion_text = suggestion.text
                
                # Only process street suggestions
                if "улица" in suggestion_text.lower() or "набережная" in suggestion_text.lower() or \
                   "проспект" in suggestion_text.lower() or "бульвар" in suggestion_text.lower() or \
                   "шоссе" in suggestion_text.lower() or "переулок" in suggestion_text.lower() or \
                   "проезд" in suggestion_text.lower() or "площадь" in suggestion_text.lower():
                    
                    try:
                        # Click on the suggestion to get to the search page with street ID
                        suggestion.click()
                        time.sleep(1)
                        
                        # Get the current URL which should now have the street ID
                        current_url = self.driver.current_url
                        street_id_match = None
                        
                        # Try different possible URL formats
                        patterns = [
                            r'street\[0\]=(\d+)',
                            r'street=(\d+)',
                            r'streetId=(\d+)'
                        ]
                        
                        for pattern in patterns:
                            import re
                            match = re.search(pattern, current_url)
                            if match:
                                street_id_match = match
                                break
                        
                        if street_id_match:
                            street_id = street_id_match.group(1)
                            results[suggestion_text] = street_id
                            logger.info(f"Found street: {suggestion_text} = {street_id}")
                        
                        # Go back to search page
                        self.driver.back()
                        time.sleep(1)
                        
                        # Re-find the input and re-enter search prefix
                        address_input = self.driver.find_element(By.CSS_SELECTOR, "[data-name='GeoSuggestInput'] input")
                        address_input.clear()
                        address_input.send_keys(search_prefix)
                        time.sleep(1.5)
                    
                    except Exception as e:
                        logger.error(f"Error processing suggestion '{suggestion_text}': {e}")
                        # Try to get back to search page
                        self.driver.get("https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=offices&region=1")
                        time.sleep(2)
                        break
            
            return results
            
        except Exception as e:
            logger.error(f"Error extracting street IDs for prefix '{search_prefix}': {e}")
            return {}
    
    def collect_streets_systematically(self):
        """Systematically collect street IDs using various prefixes"""
        # Define all prefixes to try
        # Russian alphabet prefixes
        russian_letters = list('АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЭЮЯ')
        
        # Add а-я for more comprehensive coverage
        lowercase_letters = list('абвгдеёжзийклмнопрстуфхцчшщэюя')
        
        # Add common street name prefixes
        common_prefixes = [
            "улица", "проспект", "бульвар", "набережная", 
            "шоссе", "переулок", "проезд", "площадь",
            "мал", "бол", "верх", "нижн", "стар", "нов"
        ]
        
        # Combine all prefixes
        all_prefixes = russian_letters + common_prefixes
        
        logger.info(f"Starting systematic collection with {len(all_prefixes)} prefixes")
        
        try:
            # Process each prefix
            for prefix in all_prefixes:
                if prefix in self.processed_prefixes:
                    logger.info(f"Skipping already processed prefix: {prefix}")
                    continue
                    
                logger.info(f"Processing prefix: {prefix}")
                new_streets = self.extract_street_ids_from_suggestions(prefix)
                
                # Add to our collection
                self.streets_map.update(new_streets)
                self.processed_prefixes.add(prefix)
                
                # Save progress after each prefix
                self.save_streets_json()
                
                # Random pause to avoid detection
                pause = random.uniform(1.5, 3.0)
                logger.info(f"Collected {len(new_streets)} streets with prefix '{prefix}'. Pausing for {pause:.1f}s...")
                time.sleep(pause)
                
            # Try lowercase variants if we have fewer than 2000 streets
            if len(self.streets_map) < 2000:
                for prefix in lowercase_letters:
                    if prefix in self.processed_prefixes:
                        continue
                        
                    logger.info(f"Processing lowercase prefix: {prefix}")
                    new_streets = self.extract_street_ids_from_suggestions(prefix)
                    
                    self.streets_map.update(new_streets)
                    self.processed_prefixes.add(prefix)
                    
                    self.save_streets_json()
                    
                    pause = random.uniform(1.5, 3.0)
                    logger.info(f"Collected {len(new_streets)} streets with prefix '{prefix}'. Pausing for {pause:.1f}s...")
                    time.sleep(pause)
            
        except Exception as e:
            logger.error(f"Error in collection process: {e}")
        finally:
            # Make sure we save our progress
            self.save_streets_json()
            
        logger.info(f"Collection complete. Total streets collected: {len(self.streets_map)}")

    def collect_popular_streets(self):
        """Collect IDs for the most popular Moscow streets"""
        popular_streets = [
            "Тверская", "Арбат", "Пресненская набережная", "Ленинский проспект",
            "Кутузовский проспект", "Профсоюзная", "Ленинградское шоссе", "Садовое кольцо",
            "Новый Арбат", "Мясницкая", "Большая Никитская", "Пятницкая",
            "Проспект Мира", "Вернадского", "Шаболовка", "Большая Дмитровка"
        ]
        
        logger.info(f"Collecting {len(popular_streets)} popular Moscow streets")
        
        for street in popular_streets:
            if street in self.streets_map:
                continue
                
            logger.info(f"Processing popular street: {street}")
            new_streets = self.extract_street_ids_from_suggestions(street)
            
            self.streets_map.update(new_streets)
            
            # Save after each popular street
            self.save_streets_json()
            time.sleep(1.0)
            
        logger.info(f"Popular streets collection complete.")

async def main():
    # Create collector
    collector = CianStreetCollector(headless=False)  # Set to False to see browser
    
    try:
        # First collect popular streets
        collector.collect_popular_streets()
        
        # Then do systematic collection
        collector.collect_streets_systematically()
        
        # Final save
        collector.save_streets_json()
        
    finally:
        del collector
        
    logger.info("Street collection process complete")

if __name__ == "__main__":
    asyncio.run(main())