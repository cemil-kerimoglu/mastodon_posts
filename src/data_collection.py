import yaml
from mastodon import Mastodon
import random
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

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
    out_path: str = "data/raw/statuses_may2025_streamed.csv",
    page_size: int = 40,       # Mastodon’s max for public timeline is 40
    flush_every: int = 500     # write every 500 records
):
    """
    Stream statuses between `since` and `until` to disk in small batches.
    Keeps memory use flat and writes only the columns you need.
    """
    # open the file and write header once
    first_write = True
    buffer = []
    max_id = None
    cols = [
        "id", "created_at",
        "account.id", "account.acct",
        "reblogs_count", "favourites_count", "replies_count",
        "tag_list"
    ]

    while True:
        batch = client.timeline_public(limit=page_size, max_id=max_id)
        if not batch:
            break

        for status in batch:
            ts = status["created_at"]
            if ts < since:
                # done: flush and return
                if buffer:
                    pd.DataFrame(buffer).to_csv(
                        out_path, mode="a", header=first_write, index=False
                    )
                return
            if since <= ts <= until:
                # shrink to only the fields you need
                rec = {
                    "id": status["id"],
                    "created_at": ts,
                    "account.id": status["account"]["id"],
                    "account.acct": status["account"]["acct"],
                    "reblogs_count": status["reblogs_count"],
                    "favourites_count": status["favourites_count"],
                    "replies_count": status.get("replies_count", 0),
                    "tag_list": [t["name"] for t in status.get("tags", [])]
                }
                buffer.append(rec)

                # flush every N records
                if len(buffer) >= flush_every:
                    pd.DataFrame(buffer).to_csv(
                        out_path, mode="a", header=first_write, index=False
                    )
                    first_write = False
                    buffer.clear()

        # page backward
        last_id = int(batch[-1]["id"])
        max_id = str(last_id - 1)

    # final flush
    if buffer:
        pd.DataFrame(buffer).to_csv(
            out_path, mode="a", header=first_write, index=False
        )

def fetch_fixed_per_day(
    client,
    since: datetime,
    until: datetime,
    out_path: str = "data/raw/statuses_may2025_1500_per_day.csv",
    page_size: int = 40,
    per_day: int = 1500
):
    """
    Stream the public timeline newest→oldest, and for each calendar day
    in [since, until], collect the first per_day posts encountered.
    Flush each day immediately to disk once per_day is reached.
    Stop once all days have per_day posts or the timeline is exhausted.
    """
    # 1) Build the set of dates we need
    start_date = since.date()
    end_date   = until.date()
    num_days   = (end_date - start_date).days + 1
    needed_dates = { start_date + timedelta(days=i) for i in range(num_days) }

    # 2) Prepare in-memory reservoirs and file
    reservoirs = defaultdict(list)  # date -> list of records
    first_write = True
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    max_id = None
    # 3) Page through timeline until every date is done
    while needed_dates:
        batch = client.timeline_public(limit=page_size, max_id=max_id)
        if not batch:
            logger.warning("Timeline exhausted before completing all days.")
            break

        for st in batch:
            ts = st["created_at"]
            if ts < since:
                # we've gone past our window
                needed_dates.clear()
                break

            d = ts.date()
            if d not in needed_dates:
                continue

            # build minimal record
            rec = {
                "id": st["id"],
                "created_at": ts,
                "account.id": st["account"]["id"],
                "account.acct": st["account"]["acct"],
                "account.username": st["account"]["username"],
                "account.followers_count": st["account"]["followers_count"],
                "account.following_count": st["account"]["following_count"],
                "account.statuses_count": st["account"]["statuses_count"],
                "reblogs_count": st["reblogs_count"],
                "favourites_count": st["favourites_count"],
                "replies_count": st.get("replies_count", 0),
                "tag_list": [t["name"] for t in st.get("tags", [])]
            }

            reservoirs[d].append(rec)
            # once we hit per_day for this date, flush and drop it
            if len(reservoirs[d]) == per_day:
                df_day = pd.DataFrame(reservoirs[d])
                df_day.to_csv(out_path, mode="a", header=first_write, index=False)
                logger.info(f"Flushed {per_day} posts for {d} to {out_path}")
                first_write = False
                del reservoirs[d]
                needed_dates.remove(d)

        # prepare next page
        if not needed_dates or not batch:
            break
        last_id = int(batch[-1]["id"])
        max_id   = str(last_id - 1)

    logger.info("Finished collecting fixed-per-day posts.")

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
    until = datetime(2025, 5, 28, 23, 59, 59, tzinfo=timezone.utc)
    fetch_local_statuses_in_date_range(masto, since, until, out_path="data/raw/statuses_may2025_all.csv", page_size=40, flush_every=500)
    # fetch_fixed_per_day(masto, since, until, out_path="data/raw/statuses_may2025_1500_per_day.csv", page_size=40, per_day=1500)

    # example: fetch my own followers
    # my_id = masto.account_verify_credentials()["id"]
    # fetch_followers_for_account(masto, my_id)


