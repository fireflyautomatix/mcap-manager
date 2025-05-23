import pytest

from mcap_manager.mcap_utils import (
    setup_logging,
    process_mcap_file,
    read_topics_from_file,
)
from mcap_manager.utils import check_topic_filters

import tempfile
import os
import json
from mcap.writer import Writer


def test_process_mcap_file():
    """Test processing MCAP files with topic filters."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test MCAP file
        file_path = os.path.join(temp_dir, "test.mcap")

        with open(file_path, "wb") as stream:
            writer = Writer(stream)
            writer.start()

            # Register schema and channel
            schema_id = writer.register_schema(
                name="test_schema",
                encoding="jsonschema",
                data=json.dumps(
                    {"type": "object", "properties": {"value": {"type": "string"}}}
                ).encode(),
            )

            channel_id = writer.register_channel(
                schema_id=schema_id,
                topic="test_topic",
                message_encoding="json",
                metadata={},
            )

            # Add test message
            writer.add_message(
                channel_id=channel_id,
                log_time=1000,
                data=json.dumps({"value": "test"}).encode("utf-8"),
                publish_time=1000,
            )

            writer.finish()

        logger = setup_logging(debug=True)

        # Test without filters
        messages = list(process_mcap_file(file_path, logger=logger))
        assert len(messages) == 1
        assert messages[0][0] == "test_topic"
        assert messages[0][1] == 1000

        # Test with include filter
        messages = list(
            process_mcap_file(file_path, include_topics={"test_topic"}, logger=logger)
        )
        assert len(messages) == 1

        # Test with exclude filter
        messages = list(
            process_mcap_file(file_path, exclude_topics={"test_topic"}, logger=logger)
        )
        assert len(messages) == 0


def test_check_topic_filters():
    """Test topic filtering logic."""
    # Test without filters
    assert check_topic_filters("test_topic") is True

    # Test include filter
    assert check_topic_filters("test_topic", include_topics={"test_topic"}) is True
    assert check_topic_filters("other_topic", include_topics={"test_topic"}) is False

    # Test exclude filter
    assert check_topic_filters("test_topic", exclude_topics={"test_topic"}) is False
    assert check_topic_filters("other_topic", exclude_topics={"test_topic"}) is True

    # Test both filters
    assert (
        check_topic_filters(
            "test_topic", include_topics={"test_topic"}, exclude_topics={"test_topic"}
        )
        is False
    )


def test_read_topics_from_file(tmp_path):
    """Test reading topics from a file."""
    # Create a test file with topics
    topics_file = tmp_path / "topics.txt"
    topics_file.write_text("topic1\ntopic2\n\ntopic3\n  topic4  \n")

    # Test reading topics
    topics = read_topics_from_file(str(topics_file))
    assert len(topics) == 4
    assert "topic1" in topics
    assert "topic2" in topics
    assert "topic3" in topics
    assert "topic4" in topics


def test_read_topics_from_file_empty(tmp_path):
    """Test reading topics from an empty file."""
    # Create an empty file
    topics_file = tmp_path / "empty.txt"
    topics_file.write_text("")

    # Test reading topics
    topics = read_topics_from_file(str(topics_file))
    assert len(topics) == 0


def test_read_topics_from_file_not_found():
    """Test reading topics from a non-existent file."""
    with pytest.raises(FileNotFoundError):
        read_topics_from_file("nonexistent.txt")
