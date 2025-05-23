from typing import List, Optional, Dict, Tuple
from mcap.writer import Writer
from mcap.reader import make_reader
from mcap.exceptions import RecordLengthLimitExceeded
from .query import QueryResult
from .utils import check_topic_filters
import logging
from tqdm import tqdm


def merge_mcap_files(
    results: List[QueryResult],
    output_path: str,
    logger: logging.Logger,
    include_topics: Optional[List[str]] = None,
    exclude_topics: Optional[List[str]] = None,
) -> None:
    """
    Merge multiple MCAP files into a single output file.

    Args:
        results: List of QueryResult objects containing file paths and matching topics
        output_path: Path to write the merged MCAP file
        include_topics: Optional list of topics to include
        exclude_topics: Optional list of topics to exclude
        logger: Optional logger for debug messages
    """
    # Track registered schemas and channels
    schema_ids: Dict[str, int] = {}  # schema_name -> schema_id
    channel_ids: Dict[Tuple[int, str], int] = {}  # (schema_id, topic) -> channel_id

    with open(output_path, "wb") as f:
        writer = Writer(f)
        writer.start()

        for result in tqdm(results, desc="Merging files", unit="file"):
            try:
                with open(result.file_path, "rb") as input_file:
                    if input_file.read(1) == b"":
                        if logger:
                            logger.debug(f"Skipping empty file: {result.file_path}")
                        continue
                    input_file.seek(0)  # Reset file pointer to beginning

                    reader = make_reader(input_file)
                    try:
                        for schema, channel, message in reader.iter_messages():
                            if not check_topic_filters(
                                channel.topic, set(include_topics), set(exclude_topics)
                            ):
                                continue

                            # Register schema if not already registered
                            if schema.name not in schema_ids:
                                schema_ids[schema.name] = writer.register_schema(
                                    name=schema.name,
                                    encoding=schema.encoding,
                                    data=schema.data,
                                )

                            # Register channel if not already registered
                            schema_id = schema_ids[schema.name]
                            channel_key = (schema_id, channel.topic)
                            if channel_key not in channel_ids:
                                channel_ids[channel_key] = writer.register_channel(
                                    schema_id=schema_id,
                                    topic=channel.topic,
                                    message_encoding=channel.message_encoding,
                                    metadata=channel.metadata,
                                )

                            # Write the message
                            writer.add_message(
                                channel_id=channel_ids[channel_key],
                                log_time=message.log_time,
                                data=message.data,
                                publish_time=message.publish_time,
                            )
                    except RecordLengthLimitExceeded as e:
                        if logger:
                            logger.debug(
                                f"Record length limit exceeded in {result.file_path}: {str(e)}"
                            )
                    except Exception as e:
                        if logger:
                            logger.debug(
                                f"Error reading messages from {result.file_path}: {str(e)}"
                            )
            except Exception as e:
                if logger:
                    logger.debug(f"Error opening file {result.file_path}: {str(e)}")

        writer.finish()
