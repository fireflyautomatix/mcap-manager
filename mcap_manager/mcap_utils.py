from typing import List, Optional, Set, Iterator, Tuple
from mcap.reader import make_reader
from mcap.writer import Writer
from mcap.exceptions import RecordLengthLimitExceeded
from .utils import check_topic_filters
import logging
from pathlib import Path
import os
import sys


def setup_logging(debug: bool = False) -> logging.Logger:
    """Configure logging based on debug flag."""
    if debug:
        logging.basicConfig(
            level=logging.DEBUG, format="%(message)s", stream=sys.stdout
        )
    else:
        logging.basicConfig(
            level=logging.WARNING, format="%(message)s", stream=sys.stdout
        )
    return logging.getLogger(__name__)


def parse_topics(values: Optional[List[str]]) -> List[str]:
    """Parse a list of topic strings, splitting comma-separated values."""
    if not values:
        return []
    topics = []
    for val in values:
        topics.extend([v.strip() for v in val.split(",") if v.strip()])
    return topics


def process_mcap_file(
    file_path: str,
    include_topics: Optional[Set[str]] = None,
    exclude_topics: Optional[Set[str]] = None,
    logger: Optional[logging.Logger] = None,
) -> Iterator[Tuple[str, int]]:
    """
    Process an MCAP file and yield (topic, timestamp) pairs for matching messages.
    Handles common file operations and error cases.
    """
    if logger:
        logger.debug(f"Processing file: {file_path}")

    try:
        with open(file_path, "rb") as f:
            if f.read(1) == b"":
                if logger:
                    logger.debug(f"Skipping empty file: {file_path}")
                return
            f.seek(0)  # Reset file pointer to beginning

            reader = make_reader(f)
            try:
                for schema, channel, message in reader.iter_messages():
                    if logger:
                        logger.debug(
                            f"  Found message: topic={channel.topic}, timestamp={message.log_time}"
                        )

                    if not check_topic_filters(
                        channel.topic, include_topics, exclude_topics
                    ):
                        if logger:
                            logger.debug(f"  Message filtered out by topic filters")
                        continue

                    yield channel.topic, message.log_time
            except RecordLengthLimitExceeded as e:
                if logger:
                    logger.debug(
                        f"Record length limit exceeded in {file_path}: {str(e)}"
                    )
            except Exception as e:
                if logger:
                    logger.debug(f"Error reading messages from {file_path}: {str(e)}")
    except Exception as e:
        if logger:
            logger.debug(f"Error opening file {file_path}: {str(e)}")


def find_mcap_files(root_dir: str) -> List[str]:
    """Find all MCAP files in the given directory and its subdirectories."""
    return [str(p) for p in Path(root_dir).rglob("*.mcap")]


def ensure_output_dir(output_path: str):
    """Ensure the output directory exists."""
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)


def read_topics_from_file(file_path: str) -> List[str]:
    """
    Read topics from a text file where each line contains a topic.

    Args:
        file_path: Path to the text file containing topics

    Returns:
        List of topics read from the file

    Raises:
        FileNotFoundError: If the topics file doesn't exist
        IOError: If there are issues reading the file
    """
    try:
        with open(file_path, "r") as f:
            # Read lines and strip whitespace, filtering out empty lines
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        raise FileNotFoundError(f"Topics file not found: {file_path}")
    except IOError as e:
        raise IOError(f"Error reading topics file {file_path}: {str(e)}")
