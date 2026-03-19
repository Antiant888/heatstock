# -*- coding: utf-8 -*-
"""
Gelonghui HK Stock Live Data Scraper

A robust web scraper for fetching live stock data from Gelonghui API.
Supports incremental fetching with checkpointing and duplicate detection.

Created on Tue Mar 17 19:13:09 2026
Author: jasperchan
"""

import requests
import time
import pandas as pd
import os
import logging
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────
BASE_URL = "https://www.gelonghui.com/api/live-channels/all/lives/v4"
PARAMS_BASE = {
    "category": "all",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://www.gelonghui.com/live?channel=HKStock",
    "Accept": "application/json",
}

# Use relative paths based on current working directory
OUTPUT_CSV = "gelonghui_hkstock_lives.csv"
CHECKPOINT_FILE = "last_timestamp.txt"

def load_last_timestamp():
    """Load the last timestamp from checkpoint file."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    timestamp = int(content)
                    logger.info(f"Loaded checkpoint timestamp: {timestamp} ({datetime.fromtimestamp(timestamp)})")
                    return timestamp
        except (ValueError, OSError) as e:
            logger.warning(f"Failed to load checkpoint file: {e}")
    return None

def save_last_timestamp(ts):
    """Save the timestamp to checkpoint file."""
    try:
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            f.write(str(ts))
        logger.info(f"Saved checkpoint timestamp: {ts}")
    except OSError as e:
        logger.error(f"Failed to save checkpoint file: {e}")

def fetch_page(timestamp=None, max_retries=3):
    """Fetch a page of data from the API with retry logic."""
    params = PARAMS_BASE.copy()
    if timestamp is not None:
        params["timestamp"] = timestamp

    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching page with timestamp: {timestamp}")
            r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=15)
            r.raise_for_status()
            data = r.json()
            
            # Validate response structure
            if not isinstance(data, dict):
                logger.warning(f"Invalid response format: expected dict, got {type(data)}")
                return None
            
            # Check for successful response (some APIs use 200 for success)
            status_code = data.get("statusCode")
            if status_code not in [0, 200]:
                logger.warning(f"API returned unexpected status: {status_code}")
                return None
                
            return data
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error("Max retries exceeded")
                return None
        except ValueError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            return None
    
    return None

def validate_item(item):
    """Validate that an item has the required fields."""
    if not isinstance(item, dict):
        return False
    
    item_id = str(item.get("id", "")).strip()
    if not item_id:
        return False
    
    # Check for at least one timestamp field
    create_ts = item.get("createTimestamp")
    update_ts = item.get("updateTimestamp")
    if not create_ts and not update_ts:
        return False
    
    return True

def main():
    """Main scraping function with improved error handling and validation."""
    logger.info("Starting Gelonghui HK Stock scraper")
    
    # Load existing data (for duplicate check + append)
    df_existing = pd.DataFrame()
    seen_ids = set()
    
    if os.path.exists(OUTPUT_CSV):
        try:
            df_existing = pd.read_csv(OUTPUT_CSV, dtype=str, low_memory=False)
            seen_ids = set(df_existing["id"].dropna().astype(str))
            logger.info(f"Loaded {len(df_existing)} existing records from CSV")
        except Exception as e:
            logger.error(f"Cannot read existing CSV: {e}")
            logger.info("Starting with empty dataset")
    else:
        logger.info("No existing CSV found, starting fresh")

    # Load checkpoint timestamp
    last_ts = load_last_timestamp()
    if last_ts is None:
        # Start from current time to get newest data first (convert to milliseconds)
        current_time = datetime.now()
        last_ts = int(current_time.timestamp() * 1000)  # Convert to milliseconds
        logger.info(f"Starting from current timestamp: {last_ts}")
    else:
        logger.info(f"Resuming from checkpoint timestamp: {last_ts} ({datetime.fromtimestamp(last_ts/1000)})")

    # Main scraping loop with 5-minute intervals
    while True:
        try:
            logger.info("="*60)
            logger.info("Starting new scraping cycle...")
            logger.info(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            total_new = 0
            consecutive_empty_pages = 0
            max_empty_pages = 3  # Stop after 3 consecutive empty pages

            # Scrape data for this cycle using timestamp-based pagination
            while True:
                logger.info(f"Fetching data with timestamp: {last_ts}, seen {len(seen_ids)} unique items")

                # Fetch data with retry logic
                data = fetch_page(last_ts)
                if not data:
                    logger.warning("Bad response or end of data, stopping this cycle")
                    break

                items = data.get("result", [])
                if not items:
                    consecutive_empty_pages += 1
                    logger.warning(f"No items returned (empty fetch {consecutive_empty_pages}/{max_empty_pages})")
                    if consecutive_empty_pages >= max_empty_pages:
                        logger.info("Reached maximum consecutive empty fetches, stopping this cycle")
                        break
                    continue
                else:
                    consecutive_empty_pages = 0  # Reset counter on successful fetch

                logger.info(f"Received {len(items)} items from API")

                # Process items with validation
                new_items_this_fetch = []
                for item in items:
                    if not validate_item(item):
                        logger.debug(f"Skipping invalid item: {item.get('id', 'unknown')}")
                        continue
                    
                    item_id = str(item.get("id", "")).strip()
                    if item_id in seen_ids:
                        continue

                    seen_ids.add(item_id)
                    new_items_this_fetch.append(item)

                if new_items_this_fetch:
                    total_new += len(new_items_this_fetch)
                    logger.info(f"Found {len(new_items_this_fetch)} new valid items")

                    # Append to existing DataFrame
                    df_new = pd.DataFrame(new_items_this_fetch)
                    df_existing = pd.concat([df_existing, df_new], ignore_index=True, sort=False)

                    # Save with error handling
                    try:
                        df_existing.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
                        logger.info(f"Saved {len(df_existing)} total records to CSV")
                    except Exception as e:
                        logger.error(f"Failed to save CSV: {e}")
                        raise
                else:
                    logger.info("No new valid items found in this fetch")

                # Prepare next fetch (move to older items using timestamp-based pagination)
                oldest_item = items[-1]
                next_ts = oldest_item.get("createTimestamp") or oldest_item.get("updateTimestamp")
                if not next_ts:
                    logger.warning("Cannot find timestamp in last item, stopping this cycle")
                    break

                # Check if we're going backwards in time (already processed this timestamp)
                if int(next_ts) >= last_ts:
                    logger.info("Reached end of available data, stopping this cycle")
                    break

                last_ts = int(next_ts) - 1
                save_last_timestamp(last_ts)

                # Polite delay to avoid rate limiting
                time.sleep(3.2)

            # End of cycle summary
            logger.info("="*60)
            logger.info(f"Cycle completed. Total records: {len(df_existing)}")
            logger.info(f"New items added in this cycle: {total_new}")
            if total_new == 0 and consecutive_empty_pages == 0:
                logger.info("Most likely reached the end of available history.")

            # Wait 3 minutes before next cycle
            logger.info("Waiting 3 minutes before next scraping cycle...")
            print("waiting next cycle")
            time.sleep(180)  # 3 minutes = 180 seconds

        except KeyboardInterrupt:
            logger.info("Interrupted by user, stopping...")
            break
        except Exception as e:
            logger.error(f"Unexpected error during scraping cycle: {e}")
            logger.info("Waiting 2 minutes before retrying...")
            time.sleep(120)  # Wait 2 minutes before retrying
            continue

    # Final save on exit
    try:
        if not df_existing.empty:
            df_existing.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
            logger.info(f"Final save: {len(df_existing)} total records")
    except Exception as e:
        logger.error(f"Failed to save final state: {e}")

if __name__ == "__main__":
    main()
        