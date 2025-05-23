import json
import os
import sys
import tempfile
from pathlib import Path
from time import time_ns
from datetime import datetime, timedelta, timezone

import pytest
from mcap.writer import Writer

from mcap_manager.query import query_mcap_files
from mcap_manager.mcap_utils import setup_logging


def create_test_mcap(file_path: str, topic: str, timestamp: int, data: dict):
    """Helper function to create a test MCAP file with a single message."""
    print(f"Creating MCAP file: {file_path}")
    print(f"  Topic: {topic}")
    print(f"  Timestamp: {timestamp}")
    print(f"  Data: {data}")

    mode = "wb" if not os.path.exists(file_path) else "ab"
    with open(file_path, mode) as stream:
        writer = Writer(stream)
        if mode == "wb":
            writer.start()

        schema_id = writer.register_schema(
            name="test_schema",
            encoding="jsonschema",
            data=json.dumps(
                {"type": "object", "properties": {"value": {"type": "string"}}}
            ).encode(),
        )

        channel_id = writer.register_channel(
            schema_id=schema_id, topic=topic, message_encoding="json", metadata={}
        )

        writer.add_message(
            channel_id=channel_id,
            log_time=timestamp,
            data=json.dumps(data).encode("utf-8"),
            publish_time=timestamp,
        )

        if mode == "wb":
            writer.finish()


@pytest.fixture
def base_time():
    """Provide a fixed timestamp for testing."""
    return int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1e9)


@pytest.fixture
def temp_mcap_dir(base_time):
    """Create a temporary directory with test MCAP files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test files with different timestamps

        # File 1: Single message within time range
        file1 = os.path.join(temp_dir, "test1.mcap")
        create_test_mcap(file1, "topic1", base_time, {"value": "test1"})

        # File 2: Multiple messages, some within range
        file2 = os.path.join(temp_dir, "test2.mcap")
        create_test_mcap(
            file2, "topic2", base_time - 1_000_000_000, {"value": "test2"}
        )  # 1 second before
        create_test_mcap(
            file2, "topic2", base_time + 1_000_000_000, {"value": "test2"}
        )  # 1 second after

        # File 3: Message outside time range
        file3 = os.path.join(temp_dir, "test3.mcap")
        create_test_mcap(
            file3, "topic3", base_time - 2_000_000_000, {"value": "test3"}
        )  # 2 seconds before

        yield temp_dir


def test_query_mcap_files(temp_mcap_dir, base_time):
    """Test querying MCAP files with time range and topic filters."""
    logger = setup_logging(debug=True)

    # Convert base time to ISO format, ensuring UTC timezone
    base_dt = datetime.fromtimestamp(base_time / 1e9, tz=timezone.utc)
    start_time = (base_dt - timedelta(seconds=1)).isoformat()
    end_time = (base_dt + timedelta(seconds=1)).isoformat()

    # Test 1: Query all topics within time range
    results = query_mcap_files(temp_mcap_dir, start_time, end_time, logger)
    assert len(results) == 2  # Should find test1.mcap and test2.mcap
    assert any("test1.mcap" in r.file_path for r in results)
    assert any("test2.mcap" in r.file_path for r in results)

    # Test 2: Query specific topic
    results = query_mcap_files(
        temp_mcap_dir, start_time, end_time, logger, include_topics=["topic1"]
    )
    assert len(results) == 1
    assert "test1.mcap" in results[0].file_path
    assert results[0].matching_topics == {"topic1"}

    # Test 3: Exclude specific topic
    results = query_mcap_files(
        temp_mcap_dir, start_time, end_time, logger, exclude_topics=["topic1"]
    )
    assert len(results) == 1
    assert "test2.mcap" in results[0].file_path
    assert results[0].matching_topics == {"topic2"}


def test_query_empty_directory():
    """Test querying an empty directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        logger = setup_logging(debug=True)
        results = query_mcap_files(
            temp_dir, "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", logger
        )
        assert len(results) == 0


def test_query_invalid_time_range():
    """Test querying with invalid time range."""
    with tempfile.TemporaryDirectory() as temp_dir:
        logger = setup_logging(debug=True)
        with pytest.raises(ValueError):
            query_mcap_files(temp_dir, "invalid_time", "2024-01-02T00:00:00Z", logger)
