# -*- coding: utf-8 -*-
"""
Gelonghui HK Stock Live Data Scraper (MySQL Version)

A robust web scraper for fetching live stock data from Gelonghui API.
Supports incremental fetching with checkpointing and duplicate detection.
Saves data to MySQL database instead of CSV.

Created on Tue Mar 17 19:13:09 2026
Author: jasperchan
"""

import requests
import time
import json
import logging
from datetime import datetime
from database import (
    create_database_engine, 
    init_database, 
    get_session, 
    HKStockLive,
    load_last_timestamp_db
)

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

# ────────────────────────────────────────────────
# Helper Functions
# ────────────────────────────────────────────────

def safe_json_dumps(data):
    """Safely convert data to JSON string"""
    if data is None:
        return None
    try:
        return json.dumps(data, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(data)

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
            
            # Check for successful response
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

def item_to_model(item):
    """Convert API item to database model"""
    return HKStockLive(
        id=str(item.get("id", "")).strip(),
        title=item.get("title"),
        create_timestamp=int(item["createTimestamp"]) if item.get("createTimestamp") else None,
        update_timestamp=int(item["updateTimestamp"]) if item.get("updateTimestamp") else None,
        count=safe_json_dumps(item.get("count")),
        statistic=safe_json_dumps(item.get("statistic")),
        content=item.get("content"),
        content_prefix=item.get("contentPrefix"),
        related_stocks=safe_json_dumps(item.get("relatedStocks")),
        related_infos=safe_json_dumps(item.get("relatedInfos")),
        pictures=safe_json_dumps(item.get("pictures")),
        related_articles=safe_json_dumps(item.get("relatedArticles")),
        source=safe_json_dumps(item.get("source")),
        interpretation=item.get("interpretation"),
        level=item.get("level"),
        route=item.get("route"),
        close_comment=item.get("closeComment")
    )

# ────────────────────────────────────────────────
# Main Scraping Function
# ────────────────────────────────────────────────

def main():
    """Main scraping function with MySQL storage."""
    logger.info("="*60)
    logger.info("Starting Gelonghui HK Stock scraper (MySQL version)")
    logger.info("="*60)
    
    # Initialize database
    try:
        engine = create_database_engine()
        init_database(engine)
        session = get_session(engine)
        logger.info("Database connection established")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return
    
    # Load existing IDs for duplicate check
    seen_ids = set()
    try:
        existing_ids = session.query(HKStockLive.id).all()
        seen_ids = set([row[0] for row in existing_ids])
        logger.info(f"Loaded {len(seen_ids)} existing records from database")
    except Exception as e:
        logger.warning(f"Failed to load existing IDs: {e}")
    
    # Load checkpoint timestamp
    last_ts = load_last_timestamp_db(session)
    if last_ts is None:
        # Start from current time to get newest data first
        current_time = datetime.now()
        last_ts = int(current_time.timestamp() * 1000)
        logger.info(f"Starting from current timestamp: {last_ts}")
    else:
        logger.info(f"Resuming from checkpoint timestamp: {last_ts} ({datetime.fromtimestamp(last_ts/1000)})")

    # Main scraping loop
    cycle_count = 0
    while True:
        try:
            cycle_count += 1
            logger.info("="*60)
            logger.info(f"Starting scraping cycle #{cycle_count}")
            logger.info(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            total_new = 0
            consecutive_empty_pages = 0
            max_empty_pages = 3

            # Scrape data for this cycle
            while True:
                logger.info(f"Fetching with timestamp: {last_ts}, seen {len(seen_ids)} unique items")

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
                    consecutive_empty_pages = 0

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

                    # Save to database
                    try:
                        models = [item_to_model(item) for item in new_items_this_fetch]
                        session.add_all(models)
                        session.commit()
                        
                        logger.info(f"Saved {len(models)} records to database")
                        
                    except Exception as e:
                        session.rollback()
                        logger.error(f"Failed to save to database: {e}")
                        # Try inserting one by one to identify problematic records
                        for item in new_items_this_fetch:
                            try:
                                model = item_to_model(item)
                                session.add(model)
                                session.commit()
                            except Exception as e2:
                                session.rollback()
                                logger.warning(f"Failed to save item {item.get('id')}: {e2}")
                else:
                    logger.info("No new valid items found in this fetch")

                # Prepare next fetch (move to older items)
                oldest_item = items[-1]
                next_ts = oldest_item.get("createTimestamp") or oldest_item.get("updateTimestamp")
                if not next_ts:
                    logger.warning("Cannot find timestamp in last item, stopping this cycle")
                    break

                # Check if we're going backwards in time
                if int(next_ts) >= last_ts:
                    logger.info("Reached end of available data, stopping this cycle")
                    break

                last_ts = int(next_ts) - 1
                
                # Polite delay to avoid rate limiting
                time.sleep(3.2)

            # End of cycle summary
            logger.info("="*60)
            total_records = session.query(HKStockLive).count()
            logger.info(f"Cycle #{cycle_count} completed")
            logger.info(f"Total records in database: {total_records}")
            logger.info(f"New items added in this cycle: {total_new}")
            
            if total_new == 0 and consecutive_empty_pages == 0:
                logger.info("Most likely reached the end of available history.")

            # Wait 3 minutes before next cycle
            logger.info("Waiting 3 minutes before next scraping cycle...")
            print(f"[Cycle #{cycle_count}] Waiting 3 minutes...")
            time.sleep(180)

        except KeyboardInterrupt:
            logger.info("Interrupted by user, stopping...")
            break
        except Exception as e:
            logger.error(f"Unexpected error during scraping cycle: {e}")
            logger.info("Waiting 2 minutes before retrying...")
            time.sleep(120)
            continue

    # Final summary
    try:
        total_records = session.query(HKStockLive).count()
        logger.info("="*60)
        logger.info(f"Scraping session ended")
        logger.info(f"Total records in database: {total_records}")
        logger.info("="*60)
        session.close()
    except Exception as e:
        logger.error(f"Failed to get final summary: {e}")

if __name__ == "__main__":
    main()