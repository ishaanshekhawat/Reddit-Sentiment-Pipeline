import os
import sys
import logging
from datetime import datetime, timezone
from curl_cffi import requests as curl_requests

# Allow imports from project root (utils/)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from utils.db_connect import get_connection
from utils.cleaner import clean
from utils.language_filter import is_english

load_dotenv()

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config (from .env with sensible defaults)
# ─────────────────────────────────────────────
REDDIT_JSON_URL = os.getenv(
    "REDDIT_JSON_URL",
    "https://www.reddit.com/r/indiasocial/new/.json"
)
REDDIT_FETCH_LIMIT = int(os.getenv("REDDIT_FETCH_LIMIT", 100))
REQUEST_TIMEOUT = 15  # seconds


# ─────────────────────────────────────────────
# Fetch
# ─────────────────────────────────────────────
def fetch_posts() -> list[dict]:
    """
    Fetches the latest posts from r/indiasocial using Reddit's public
    .json endpoint. No authentication required.

    Only returns text posts (is_self=True). Link posts are filtered out
    here before any further processing.

    Returns:
        List of raw post dicts from Reddit's JSON response.
        Empty list if the request fails or Reddit returns no posts.
    """

    url = f"{REDDIT_JSON_URL}?limit={REDDIT_FETCH_LIMIT}"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "dnt": "1",
        "referer": "https://www.reddit.com/r/indiasocial/",
        "sec-ch-ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    }

    try:
        response = curl_requests.get(
            url,
            headers=headers,
            impersonate="chrome116",   # TLS fingerprint of Chrome 116
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code != 200:
            logger.error(
                f"Reddit returned HTTP {response.status_code}. "
                f"Body: {response.text[:300]}"
            )
            return []

        data = response.json()
        children = data.get("data", {}).get("children", [])

        if not children:
            logger.warning("Reddit returned 0 posts.")
            return []
        # Extract raw post data dicts
        posts = [child["data"] for child in children]

        # Filter: text posts only (is_self=True)
        text_posts = [p for p in posts if p.get("is_self", False)]
        link_count = len(posts) - len(text_posts)

        logger.info(
            f"Fetched {len(posts)} posts. "
            f"Text posts: {len(text_posts)} | Link posts skipped: {link_count}"
        )
        return text_posts

    except requests.exceptions.Timeout:
        logger.error(f"Request timed out after {REQUEST_TIMEOUT}s. Reddit may be slow.")
        return []
    except requests.exceptions.ConnectionError:
        logger.error("Connection error — check network connectivity on the VM.")
        return []
    except (ValueError, KeyError) as e:
        logger.error(f"Failed to parse Reddit JSON response: {e}")
        return []
    


# ─────────────────────────────────────────────
# Transform
# ─────────────────────────────────────────────
def transform_post(raw: dict) -> dict | None:
    """
    Transforms a single raw Reddit post dict into a clean record
    ready for database insertion.

    Transformations applied:
        - Extracts the 5 Reddit fields we care about
        - Converts created_utc (Unix float) to a UTC datetime object
        - Adds collected_at timestamp (now, UTC)
        - Combines title + selftext for cleaning and language detection
        - Runs text through cleaner.clean()
        - Runs cleaned text through language_filter.is_english()

    Returns:
        Transformed dict ready for INSERT, or None if the post
        should be skipped (e.g. missing required fields).
    """
    post_id = raw.get("id")
    title = raw.get("title", "").strip()

    # Skip posts with no ID or no title — shouldn't happen but be defensive
    if not post_id or not title:
        logger.warning(f"Skipping post with missing id or title: {raw.get('id', 'UNKNOWN')}")
        return None

    selftext = raw.get("selftext", "") or ""
    score = raw.get("score", 0)
    created_utc_raw = raw.get("created_utc")

    # Convert Unix timestamp → UTC datetime
    if created_utc_raw is not None:
        created_utc = datetime.fromtimestamp(created_utc_raw, tz=timezone.utc).replace(tzinfo=None)
    else:
        created_utc = None

    collected_at = datetime.utcnow()

    # Combine title + selftext for richer language detection and cleaning.
    # Title alone is often too short for langdetect to work reliably.
    combined_text = f"{title} {selftext}".strip()
    cleaned = clean(combined_text)
    english = is_english(cleaned)

    return {
        "id": post_id,
        "title": title,
        "selftext": selftext,
        "score": score,
        "created_utc": created_utc,
        "collected_at": collected_at,
        "is_english": english,
        "cleaned_text": cleaned,
    }


# ─────────────────────────────────────────────
# Store
# ─────────────────────────────────────────────
INSERT_SQL = """
    INSERT INTO reddit_posts (
        id,
        title,
        selftext,
        score,
        created_utc,
        collected_at,
        is_english,
        cleaned_text
    )
    VALUES (
        %(id)s,
        %(title)s,
        %(selftext)s,
        %(score)s,
        %(created_utc)s,
        %(collected_at)s,
        %(is_english)s,
        %(cleaned_text)s
    )
    ON CONFLICT (id) DO NOTHING;
"""


def store_posts(records: list[dict]) -> tuple[int, int]:
    """
    Inserts a list of transformed post records into PostgreSQL.

    Uses ON CONFLICT (id) DO NOTHING for idempotency — if a post ID
    already exists in the table (fetched in a previous hourly run),
    the insert is silently skipped at the database level.

    Returns:
        Tuple of (inserted_count, duplicate_count).
        inserted_count + duplicate_count == len(records).

    """
    if not records:
        return 0, 0

    inserted = 0
    duplicates = 0

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for record in records:
                cur.execute(INSERT_SQL, record)
                # rowcount == 1 means a new row was inserted
                # rowcount == 0 means ON CONFLICT triggered (duplicate)
                if cur.rowcount == 1:
                    inserted += 1
                else:
                    duplicates += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error during insert: {e}")
        raise
    finally:
        conn.close()

    return inserted, duplicates


# ─────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────
def run():
    """
    Full fetch → transform → store pipeline.

    Exit codes:
        0 — success (even if 0 new posts — duplicates are expected)
        1 — fetch returned nothing at all (network/Reddit issue)
        2 — database error
    """
    logger.info("─" * 50)
    logger.info("Starting fetch_and_store run")
    logger.info("─" * 50)

    # Step 1 — Fetch
    raw_posts = fetch_posts()

    if not raw_posts:
        logger.error("No posts fetched — exiting with error code 1.")
        sys.exit(1)

    # Step 2 — Transform
    records = []
    skipped_transform = 0
    non_english_count = 0

    for raw in raw_posts:
        record = transform_post(raw)
        if record is None:
            skipped_transform += 1
            continue
        if not record["is_english"]:
            non_english_count += 1
        records.append(record)

    logger.info(
        f"Transform complete. "
        f"Valid records: {len(records)} | "
        f"Non-English: {non_english_count} | "
        f"Skipped (bad data): {skipped_transform}"
    )

    # Step 3 — Store
    try:
        inserted, duplicates = store_posts(records)
    except Exception:
        logger.error("Pipeline failed at store step — exiting with error code 2.")
        sys.exit(2)

    # Step 4 — Summary log
    logger.info("─" * 50)
    logger.info(
        f"Run complete. "
        f"Fetched: {len(raw_posts)} | "
        f"New inserted: {inserted} | "
        f"Already seen (skipped): {duplicates} | "
        f"Non-English (stored, not scored): {non_english_count}"
    )
    logger.info("─" * 50)


if __name__ == "__main__":
    run()
