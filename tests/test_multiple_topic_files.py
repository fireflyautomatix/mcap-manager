import pytest
from pathlib import Path
import tempfile
import json
from mcap.writer import Writer
from datetime import datetime, timezone
from click.testing import CliRunner
from mcap_manager.cli import cli


@pytest.fixture
def runner():
    """Create a Click CLI test runner."""
    return CliRunner()


def create_test_mcap(file_path: str, topic: str, timestamp: int, data: dict):
    """Helper function to create a test MCAP file with a single message."""
    with open(file_path, "wb") as stream:
        writer = Writer(stream)
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

        writer.finish()


def test_merge_with_multiple_topic_files(temp_dir, runner, base_time):
    """Test merge command with multiple topic files."""
    # Create test directories
    root_dir = temp_dir / "root"
    root_dir.mkdir()
    output_dir = temp_dir / "output"
    output_dir.mkdir()

    # Create multiple topic files
    topics_file1 = temp_dir / "topics1.txt"
    topics_file1.write_text("topic1\ntopic2\n")

    topics_file2 = temp_dir / "topics2.txt"
    topics_file2.write_text("topic3\ntopic4\n")

    # Create test MCAP files with various topics
    create_test_mcap(root_dir / "test1.mcap", "topic1", base_time, {"value": "test1"})
    create_test_mcap(
        root_dir / "test2.mcap", "topic2", base_time + 1_000_000_000, {"value": "test2"}
    )
    create_test_mcap(
        root_dir / "test3.mcap", "topic3", base_time + 2_000_000_000, {"value": "test3"}
    )
    create_test_mcap(
        root_dir / "test4.mcap", "topic4", base_time + 3_000_000_000, {"value": "test4"}
    )
    create_test_mcap(
        root_dir / "test5.mcap", "topic5", base_time + 4_000_000_000, {"value": "test5"}
    )

    # Run merge command with multiple topic files
    result = runner.invoke(
        cli,
        [
            "merge",
            str(root_dir),
            "--start",
            "2024-01-01T12:00:00Z",
            "--end",
            "2024-01-01T12:00:05Z",
            "--include-topics-file",
            str(topics_file1),
            "--include-topics-file",
            str(topics_file2),
            "--output",
            str(output_dir / "merged.mcap"),
        ],
    )

    assert result.exit_code == 0
    assert "Found 4 matching MCAP files" in result.output


def test_merge_with_overlapping_topic_files(temp_dir, runner, base_time):
    """Test merge command with topic files that have overlapping topics."""
    # Create test directories
    root_dir = temp_dir / "root"
    root_dir.mkdir()
    output_dir = temp_dir / "output"
    output_dir.mkdir()

    # Create topic files with overlapping topics
    topics_file1 = temp_dir / "topics1.txt"
    topics_file1.write_text("topic1\ntopic2\n")

    topics_file2 = temp_dir / "topics2.txt"
    topics_file2.write_text("topic2\ntopic3\n")  # topic2 is in both files

    # Create test MCAP files
    create_test_mcap(root_dir / "test1.mcap", "topic1", base_time, {"value": "test1"})
    create_test_mcap(
        root_dir / "test2.mcap", "topic2", base_time + 1_000_000_000, {"value": "test2"}
    )
    create_test_mcap(
        root_dir / "test3.mcap", "topic3", base_time + 2_000_000_000, {"value": "test3"}
    )

    # Run merge command with overlapping topic files
    result = runner.invoke(
        cli,
        [
            "merge",
            str(root_dir),
            "--start",
            "2024-01-01T12:00:00Z",
            "--end",
            "2024-01-01T12:00:03Z",
            "--include-topics-file",
            str(topics_file1),
            "--include-topics-file",
            str(topics_file2),
            "--output",
            str(output_dir / "merged.mcap"),
        ],
    )

    assert result.exit_code == 0
    assert "Found 3 matching MCAP files" in result.output


def test_merge_with_empty_topic_files(temp_dir, runner, base_time):
    """Test merge command with empty topic files."""
    # Create test directories
    root_dir = temp_dir / "root"
    root_dir.mkdir()
    output_dir = temp_dir / "output"
    output_dir.mkdir()

    # Create empty topic file
    topics_file = temp_dir / "topics.txt"
    topics_file.write_text("")

    # Create test MCAP files
    create_test_mcap(root_dir / "test1.mcap", "topic1", base_time, {"value": "test1"})

    # Run merge command with empty topic file
    result = runner.invoke(
        cli,
        [
            "merge",
            str(root_dir),
            "--start",
            "2024-01-01T12:00:00Z",
            "--end",
            "2024-01-01T12:00:01Z",
            "--include-topics-file",
            str(topics_file),
            "--output",
            str(output_dir / "merged.mcap"),
        ],
    )

    assert result.exit_code != 0
    assert "No topics specified in topic files" in result.output


def test_merge_with_nonexistent_topic_file(temp_dir, runner, base_time):
    """Test merge command with a nonexistent topic file."""
    # Create test directories
    root_dir = temp_dir / "root"
    root_dir.mkdir()
    output_dir = temp_dir / "output"
    output_dir.mkdir()

    # Create test MCAP files
    create_test_mcap(root_dir / "test1.mcap", "topic1", base_time, {"value": "test1"})

    # Run merge command with nonexistent topic file
    result = runner.invoke(
        cli,
        [
            "merge",
            str(root_dir),
            "--start",
            "2024-01-01T12:00:00Z",
            "--end",
            "2024-01-01T12:00:01Z",
            "--include-topics-file",
            str(temp_dir / "nonexistent.txt"),
            "--output",
            str(output_dir / "merged.mcap"),
        ],
    )

    assert result.exit_code != 0
    assert "Error: Topic file does not exist" in result.output
