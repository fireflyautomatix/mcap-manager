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


def test_process_mcap_file(sample_mcap_files, base_time):
    """Test processing MCAP files with topic filters."""
    logger = setup_logging(debug=True)

    # Test without filters
    messages = list(process_mcap_file(str(sample_mcap_files["file1"]), logger=logger))
    assert len(messages) == 1
    assert messages[0][0] == "topic1"
    assert messages[0][1] == base_time

    # Test with include filter
    messages = list(
        process_mcap_file(
            str(sample_mcap_files["file1"]), include_topics={"topic1"}, logger=logger
        )
    )
    assert len(messages) == 1
    assert messages[0][0] == "topic1"

    # Test with exclude filter
    messages = list(
        process_mcap_file(
            str(sample_mcap_files["file1"]), exclude_topics={"topic1"}, logger=logger
        )
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


def test_read_topics_from_file(topics_file):
    """Test reading topics from a file."""
    # Test reading topics
    topics = read_topics_from_file(str(topics_file))
    assert len(topics) == 2
    assert "topic1" in topics
    assert "topic2" in topics


def test_read_topics_from_file_empty(temp_dir):
    """Test reading topics from an empty file."""
    # Create an empty file
    topics_file = temp_dir / "empty.txt"
    topics_file.write_text("")

    # Test reading topics
    topics = read_topics_from_file(str(topics_file))
    assert len(topics) == 0


def test_read_topics_from_file_not_found(temp_dir):
    """Test reading topics from a non-existent file."""
    nonexistent_file = temp_dir / "nonexistent.txt"
    with pytest.raises(FileNotFoundError):
        read_topics_from_file(str(nonexistent_file))
