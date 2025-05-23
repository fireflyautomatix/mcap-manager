from dataclasses import dataclass
from typing import List, Optional, Set
from .mcap_utils import setup_logging, process_mcap_file, find_mcap_files
from .utils import parse_iso_time
import time
import logging
from tqdm import tqdm


@dataclass
class QueryResult:
    """Result of an MCAP query."""

    file_path: str
    matching_topics: Set[str]
    start_time: int
    end_time: int


def query_mcap_files(
    root_dir: str,
    start_time: str,
    end_time: str,
    logger: logging.Logger,
    include_topics: Optional[List[str]] = None,
    exclude_topics: Optional[List[str]] = None,
) -> List[QueryResult]:
    """
    Query MCAP files in the given directory for messages within the time range
    and matching topic filters.
    """
    start_ns = parse_iso_time(start_time)
    end_ns = parse_iso_time(end_time)
    include_set = set(include_topics) if include_topics else None
    exclude_set = set(exclude_topics) if exclude_topics else None

    logger.debug(f"Query parameters:")
    logger.debug(f"  Start time: {start_time} ({start_ns})")
    logger.debug(f"  End time: {end_time} ({end_ns})")
    logger.debug(f"  Include topics: {include_set}")
    logger.debug(f"  Exclude topics: {exclude_set}")

    results = []
    mcap_files = find_mcap_files(root_dir)

    logger.info(f"Querying {len(mcap_files)} MCAP files...")
    logger.debug(f"Found files: {mcap_files}")
    start_time_total = time.time()

    with tqdm(total=len(mcap_files), desc="Querying files", unit="file") as pbar:
        for file_path in mcap_files:
            matching_topics = set()
            file_start_time = None
            file_end_time = None

            logger.debug(f"Processing file: {file_path}")
            for topic, timestamp in process_mcap_file(
                file_path,
                include_topics=include_set,
                exclude_topics=exclude_set,
                logger=logger,
            ):
                if start_ns <= timestamp <= end_ns:
                    matching_topics.add(topic)
                    if file_start_time is None or timestamp < file_start_time:
                        file_start_time = timestamp
                    if file_end_time is None or timestamp > file_end_time:
                        file_end_time = timestamp
                else:
                    logger.debug(
                        f"  Message outside time range: {timestamp} not in [{start_ns}, {end_ns}]"
                    )

            if matching_topics:
                logger.debug(f"  File has matching topics: {matching_topics}")
                results.append(
                    QueryResult(
                        file_path=file_path,
                        matching_topics=matching_topics,
                        start_time=file_start_time,
                        end_time=file_end_time,
                    )
                )
            else:
                logger.debug(f"  No matching topics found in file")

            pbar.update(1)

    elapsed_time = time.time() - start_time_total
    logger.info(f"Query completed in {elapsed_time:.2f} seconds")
    logger.info(f"Found {len(results)} matching files")

    return results
