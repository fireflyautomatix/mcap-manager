import pytest
from pathlib import Path
import tempfile
import json
import subprocess
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


def test_cli_query(sample_mcap_files, temp_dir):
    """Test the CLI query functionality."""
    # Test basic query
    result = subprocess.run(
        [
            "mcap_manager",
            "merge",
            str(temp_dir),
            "--start",
            "2024-01-01T12:00:00Z",
            "--end",
            "2024-01-01T12:00:02Z",
            "--output",
            str(temp_dir / "merged.mcap"),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0

    # Test query with topic filters
    result = subprocess.run(
        [
            "mcap_manager",
            "merge",
            str(temp_dir),
            "--start",
            "2024-01-01T12:00:00Z",
            "--end",
            "2024-01-01T12:00:02Z",
            "--include-topics",
            "topic1",
            "--exclude-topics",
            "topic2",
            "--output",
            str(temp_dir / "merged.mcap"),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


def test_cli_merge(sample_mcap_files, temp_dir):
    """Test the CLI merge functionality."""
    output_path = temp_dir / "merged.mcap"

    # Test basic merge
    result = subprocess.run(
        [
            "mcap_manager",
            "merge",
            str(temp_dir),
            "--start",
            "2024-01-01T12:00:00Z",
            "--end",
            "2024-01-01T12:00:02Z",
            "--output",
            str(output_path),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert output_path.exists()

    # Test merge with topic filters
    result = subprocess.run(
        [
            "mcap_manager",
            "merge",
            str(temp_dir),
            "--start",
            "2024-01-01T12:00:00Z",
            "--end",
            "2024-01-01T12:00:02Z",
            "--include-topics",
            "topic1",
            "--exclude-topics",
            "topic2",
            "--output",
            str(output_path),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert output_path.exists()


def test_cli_invalid_command():
    """Test handling of invalid CLI commands."""
    result = subprocess.run(
        ["mcap_manager", "invalid_command"], capture_output=True, text=True
    )
    assert result.returncode != 0


def test_cli_merge_with_topics_file(sample_mcap_files, topics_file, temp_dir, runner):
    """Test merge command with topics from a file."""
    output_dir = temp_dir / "output"
    output_dir.mkdir()

    # Run merge command
    result = runner.invoke(
        cli,
        [
            "merge",
            str(temp_dir),
            "--start",
            "2024-01-01T12:00:00Z",
            "--end",
            "2024-01-01T12:00:02Z",
            "--include-topics-file",
            str(topics_file),
            "--output",
            str(output_dir / "merged.mcap"),
        ],
    )

    assert result.exit_code == 0
    assert "Found 2 matching MCAP files" in result.output


def test_cli_info_with_topics_file(sample_mcap_files, topics_file, temp_dir, runner):
    """Test info command with topics from a file."""
    result = runner.invoke(
        cli,
        [
            "info",
            str(temp_dir),
            "--include-topics-file",
            str(topics_file),
        ],
    )

    assert result.exit_code == 0
    # Check for topic information in the output
    assert "Found 3 MCAP files" in result.output
    assert "Total files: 3" in result.output
    assert "Time range:" in result.output


def test_cli_with_nonexistent_topics_file(sample_mcap_files, temp_dir, runner):
    """Test handling of nonexistent topics file."""
    nonexistent_file = temp_dir / "nonexistent.txt"
    result = runner.invoke(
        cli,
        [
            "merge",
            str(temp_dir),
            "--include-topics-file",
            str(nonexistent_file),
        ],
    )

    assert result.exit_code != 0
    assert "Error" in result.output


def test_cli_merge_with_time_range(sample_mcap_files, temp_dir, runner):
    """Test merge command with time range."""
    output_path = temp_dir / "merged.mcap"

    # Test with valid time range
    result = runner.invoke(
        cli,
        [
            "merge",
            str(temp_dir),
            "--start",
            "2024-01-01T12:00:00Z",
            "--end",
            "2024-01-01T12:00:02Z",
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()

    # Test with time range that excludes all files
    result = runner.invoke(
        cli,
        [
            "merge",
            str(temp_dir),
            "--start",
            "2024-01-02T00:00:00Z",
            "--end",
            "2024-01-02T00:00:01Z",
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "No matching MCAP files found" in result.output


def test_cli_merge_time_range_validation(sample_mcap_files, temp_dir, runner):
    """Test time range validation in merge command."""
    output_path = temp_dir / "merged.mcap"

    # Test with invalid time format
    result = runner.invoke(
        cli,
        [
            "merge",
            str(temp_dir),
            "--start",
            "invalid-time",
            "--end",
            "2024-01-01T12:00:02Z",
            "--output",
            str(output_path),
        ],
        catch_exceptions=True,  # Catch exceptions to handle them properly
    )

    assert result.exit_code != 0
    # The error message should be in the exception
    assert isinstance(result.exception, Exception)
    assert "Unknown string format" in str(result.exception)
