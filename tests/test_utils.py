import pytest
from datetime import datetime, timezone
from mcap_manager.utils import (
    find_mcap_files,
    parse_iso_time,
    check_topic_filters,
    format_timestamp,
)


def test_find_mcap_files(tmp_path):
    """Test finding MCAP files in a directory structure."""
    # Create test directory structure
    (tmp_path / "dir1").mkdir()
    (tmp_path / "dir2").mkdir()
    (tmp_path / "dir1" / "test1.mcap").write_text("")
    (tmp_path / "dir2" / "test2.mcap").write_text("")
    (tmp_path / "dir2" / "not_mcap.txt").write_text("")

    # Test finding all MCAP files
    mcap_files = find_mcap_files(str(tmp_path))
    assert len(mcap_files) == 2
    assert any("test1.mcap" in f for f in mcap_files)
    assert any("test2.mcap" in f for f in mcap_files)


def test_parse_iso_time():
    """Test parsing ISO time strings to nanoseconds."""
    # Test with UTC timezone
    assert parse_iso_time("2024-01-01T00:00:00Z") == 1704067200000000000

    # Test with local timezone
    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert parse_iso_time(dt.isoformat()) == 1704067200000000000

    # Test with naive datetime
    dt = datetime(2024, 1, 1)
    assert parse_iso_time(dt.isoformat()) == 1704067200000000000


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


def test_format_timestamp():
    """Test formatting timestamps to ISO strings."""
    # Test with a known timestamp
    ns = 1704067200000000000  # 2024-01-01T00:00:00Z
    assert format_timestamp(ns) == "2024-01-01T00:00:00+00:00"

    # Test with zero timestamp
    assert format_timestamp(0) == "1970-01-01T00:00:00+00:00"
