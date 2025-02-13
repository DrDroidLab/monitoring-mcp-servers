import time

from datetime import datetime, timezone


def current_milli_time():
    return round(time.time() * 1000)


def current_epoch_timestamp():
    return int(time.time())

def format_to_github_timestamp(timestamp: int) -> str:
    dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')