import yaml
from mastodon import Mastodon
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timezone

# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(path: str = "config/credentials.yaml"):
    with open(path) as f:
        cfg = yaml.safe_load(f)
    return cfg

def get_client(cfg):
    return Mastodon(
        client_id=cfg["client_id"],
        client_secret=cfg["client_secret"],
        access_token=cfg["access_token"],
        api_base_url=cfg["api_base_url"]
    )

def fetch_my_account(client, out_path="data/raw/my_account.csv"):
    account = client.account_verify_credentials()
    df = pd.json_normalize(account)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    logger.info(f"Saved account info to {out_path}")

def fetch_local_statuses_in_date_range(
    client,
    since: datetime,
    until: datetime,
    out_path: str = "data/raw/statuses_may2025.csv",
    page_size: int = 40,
    max_posts: int = None
):
    """
    Fetch all local/public statuses with created_at between `since` and `until`.
    Stops once statuses older than `since` are reached.
    """
    all_statuses = []
    max_id = None

    while True:
        batch = client.timeline_public(limit=page_size, max_id=max_id)
        if not batch:
            logger.info("No more statuses to fetch.")
            break

        for status in batch:
            created = status["created_at"]  # datetime.datetime
            # extract just the hashtag names for easier analysis
            status["tag_list"] = [t["name"] for t in status.get("tags", [])]
            if created < since:
                # We've paged past our window — done.
                logger.info(f"Reached statuses older than {since.isoformat()}.")
                df = pd.json_normalize(all_statuses)
                Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(out_path, index=False)
                logger.info(f"Saved {len(all_statuses)} statuses to {out_path}")
                return

            if since <= created <= until:
                all_statuses.append(status)
                if max_posts and len(all_statuses) >= max_posts:
                    logger.info(f"Reached max_posts limit of {max_posts}.")
                    df = pd.json_normalize(all_statuses)
                    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                    df.to_csv(out_path, index=False)
                    logger.info(f"Saved {len(all_statuses)} statuses to {out_path}")
                    return

        # prepare for next page
        last_id = int(batch[-1]["id"])
        max_id = str(last_id - 1)
        logger.info(f"Fetched {len(all_statuses)} total; paging with max_id={max_id}")

    # In case all statuses are still newer than `since`
    df = pd.json_normalize(all_statuses)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    logger.info(f"Saved {len(all_statuses)} statuses to {out_path}")

def fetch_followers_for_account(
    client,
    account_id: int,
    out_path: str = None,
    page_size: int = 80
):
    """
    Fetch *all* followers of the given account_id.
    Writes out CSV of account objects.
    """
    if out_path is None:
        out_path = f"data/raw/followers_{account_id}.csv"

    all_followers = []
    max_id = None

    while True:
        batch = client.account_followers(account_id, limit=page_size, max_id=max_id)
        if not batch:
            break

        all_followers.extend(batch)
        max_id = batch[-1]["id"] - 1
        logger.info(f"Fetched {len(all_followers)} followers so far (next max_id={max_id})")

    df = pd.json_normalize(all_followers)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    logger.info(f"Saved {len(all_followers)} followers to {out_path}")

if __name__ == "__main__":
    cfg = load_config()
    masto = get_client(cfg)

    # fetch_my_account(masto)

    since = datetime(2025, 5, 1, tzinfo=timezone.utc) 
    until = datetime(2025, 5, 31, 23, 59, 59, tzinfo=timezone.utc)
    fetch_local_statuses_in_date_range(masto, since, until, out_path="data/raw/statuses_may2025_3k.csv", max_posts=3000)

    # example: fetch my own followers
    # my_id = masto.account_verify_credentials()["id"]
    # fetch_followers_for_account(masto, my_id)


