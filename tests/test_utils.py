import pytest
from datetime import datetime, timezone
from mcap_manager.utils import (
    find_mcap_files,
    parse_iso_time,
    check_topic_filters,
    format_timestamp,
)


def test_find_mcap_files(sample_mcap_files, temp_dir):
    """Test finding MCAP files in a directory structure."""
    # Test finding all MCAP files
    mcap_files = find_mcap_files(str(temp_dir))
    assert len(mcap_files) == 3
    assert any("test1.mcap" in f for f in mcap_files)
    assert any("test2.mcap" in f for f in mcap_files)
    assert any("test3.mcap" in f for f in mcap_files)


def test_parse_iso_time(base_time):
    """Test parsing ISO time strings to nanoseconds."""
    # Test with UTC timezone
    assert parse_iso_time("2024-01-01T12:00:00Z") == base_time

    # Test with local timezone
    dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert parse_iso_time(dt.isoformat()) == base_time

    # Test with naive datetime
    dt = datetime(2024, 1, 1, 12, 0, 0)
    assert parse_iso_time(dt.isoformat()) == base_time


def test_check_topic_filters():
    """Test topic filtering functionality."""
    # Test with no filters
    assert check_topic_filters("test_topic") is True

    # Test with include filter
    assert check_topic_filters("test_topic", include_topics={"test_topic"}) is True
    assert check_topic_filters("other_topic", include_topics={"test_topic"}) is False

    # Test with exclude filter
    assert check_topic_filters("test_topic", exclude_topics={"test_topic"}) is False
    assert check_topic_filters("other_topic", exclude_topics={"test_topic"}) is True

    # Test with both filters
    assert (
        check_topic_filters(
            "test_topic", include_topics={"test_topic"}, exclude_topics={"test_topic"}
        )
        is False
    )


def test_format_timestamp(base_time):
    """Test formatting timestamps to ISO strings."""
    # Test with base time
    assert format_timestamp(base_time) == "2024-01-01T12:00:00+00:00"

    # Test with zero timestamp
    assert format_timestamp(0) == "1970-01-01T00:00:00+00:00"
