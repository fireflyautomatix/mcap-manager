from typing import List, Optional, Dict, Tuple
from mcap.writer import Writer
from mcap.reader import make_reader
from mcap.exceptions import RecordLengthLimitExceeded
from .query import QueryResult
from .utils import check_topic_filters
import logging
from tqdm import tqdm
from datetime import datetime


def merge_mcap_files(
    results: List[QueryResult],
    output_path: str,
    logger: logging.Logger,
    include_topics: Optional[List[str]] = None,
    exclude_topics: Optional[List[str]] = None,
    latched_transient_output_msgs: int = 1,
    start_ns: Optional[int] = None,
) -> None:
    """
    Merge multiple MCAP files into a single output file.

    Args:
        results: List of QueryResult objects containing file paths and matching topics
        output_path: Path to write the merged MCAP file
        include_topics: Optional list of topics to include
        exclude_topics: Optional list of topics to exclude
        logger: Optional logger for debug messages
        latched_transient_output_msgs: Number of transient messages to include before start timestamp
        start_ns: Start time in nanoseconds (for transient message timestamp adjustment)
    """
    # Track registered schemas and channels
    schema_ids: Dict[str, int] = {}  # schema_name -> schema_id
    channel_ids: Dict[Tuple[int, str], int] = {}  # (schema_id, topic) -> channel_id

    # Track transient messages for each topic
    transient_messages: Dict[str, List[Tuple[int, bytes]]] = {}

    with open(output_path, "wb") as f:
        writer = Writer(f)
        writer.start()

        # First pass: collect transient messages
        for result in tqdm(results, desc="Collecting transient messages", unit="file"):
            if "transient_output" not in result.file_path:
                continue

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

                            if channel.topic not in transient_messages:
                                transient_messages[channel.topic] = []
                            transient_messages[channel.topic].append(
                                (message.log_time, message.data)
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

        # Sort transient messages by timestamp for each topic
        for topic in transient_messages:
            transient_messages[topic].sort(key=lambda x: x[0])

        # Second pass: merge all files
        for result in tqdm(results, desc="Merging files", unit="file"):
            # Skip transient files in the second pass
            if "transient_output" in result.file_path:
                continue

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

                            # Always write the regular message
                            writer.add_message(
                                channel_id=channel_ids[channel_key],
                                log_time=message.log_time,
                                data=message.data,
                                publish_time=message.publish_time,
                            )

                            # For each topic in transient_messages, add the last N messages before this timestamp
                            for t_topic, t_messages in transient_messages.items():
                                if not check_topic_filters(
                                    t_topic, set(include_topics), set(exclude_topics)
                                ):
                                    continue
                                # Register channel for this topic if not already registered
                                t_schema_id = schema_id
                                t_channel_key = (t_schema_id, t_topic)
                                if t_channel_key not in channel_ids:
                                    channel_ids[t_channel_key] = (
                                        writer.register_channel(
                                            schema_id=t_schema_id,
                                            topic=t_topic,
                                            message_encoding=channel.message_encoding,
                                            metadata=channel.metadata,
                                        )
                                    )
                                # Find messages before this timestamp
                                before_messages = [
                                    (ts, data)
                                    for ts, data in t_messages
                                    if ts < message.log_time
                                ]
                                # Take the last N messages
                                for ts, data in before_messages[
                                    -latched_transient_output_msgs:
                                ]:
                                    # Set timestamp to start time if before start, else to regular message's timestamp
                                    if start_ns is not None and ts < start_ns:
                                        adjusted_log_time = start_ns
                                    else:
                                        adjusted_log_time = message.log_time
                                    writer.add_message(
                                        channel_id=channel_ids[t_channel_key],
                                        log_time=adjusted_log_time,
                                        data=data,
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

        # If we only have transient messages and no regular messages, we need to write at least one message
        if not any("transient_output" not in r.file_path for r in results):
            for topic, messages in transient_messages.items():
                if not check_topic_filters(
                    topic, set(include_topics), set(exclude_topics)
                ):
                    continue

                if not messages:
                    continue

                # Get the first message to use as a base
                first_ts, first_data = messages[0]

                # Register schema and channel if not already done
                if "transient_schema" not in schema_ids:
                    schema_ids["transient_schema"] = writer.register_schema(
                        name="transient_schema",
                        encoding="jsonschema",
                        data=b'{"type": "object", "properties": {"value": {"type": "string"}}}',
                    )

                schema_id = schema_ids["transient_schema"]
                channel_key = (schema_id, topic)
                if channel_key not in channel_ids:
                    channel_ids[channel_key] = writer.register_channel(
                        schema_id=schema_id,
                        topic=topic,
                        message_encoding="json",
                        metadata={},
                    )

                # Write the first message
                writer.add_message(
                    channel_id=channel_ids[channel_key],
                    log_time=first_ts,
                    data=first_data,
                    publish_time=first_ts,
                )

        writer.finish()
