import os
from datetime import datetime, timezone
from typing import List, Set, Optional
from dateutil import parser


def find_mcap_files(root_dir: str) -> List[str]:
    """Find all .mcap files in the given directory and its subdirectories."""
    mcap_files = []
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".mcap"):
                mcap_files.append(os.path.join(root, file))
    return mcap_files


def parse_iso_time(time_str: str) -> int:
    """Convert ISO 8601 time string to nanoseconds since epoch."""
    dt = parser.parse(time_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1e9)


def check_topic_filters(
    topic: str,
    include_topics: Optional[Set[str]] = None,
    exclude_topics: Optional[Set[str]] = None,
) -> bool:
    """Check if a topic passes the include/exclude filters."""
    if include_topics and topic not in include_topics:
        return False
    if exclude_topics and topic in exclude_topics:
        return False
    return True


def format_timestamp(ns: int) -> str:
    """Convert nanoseconds since epoch to ISO 8601 string."""
    dt = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
    return dt.isoformat()
