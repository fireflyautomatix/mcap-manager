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


def test_cli_query():
    """Test the CLI query functionality."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test MCAP files
        file1_path = Path(temp_dir) / "test1.mcap"
        file2_path = Path(temp_dir) / "test2.mcap"

        # Create test files with different timestamps
        base_time = int(
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1e9
        )
        create_test_mcap(str(file1_path), "topic1", base_time, {"value": "test1"})
        create_test_mcap(
            str(file2_path), "topic2", base_time + 1_000_000_000, {"value": "test2"}
        )

        # Test basic query
        result = subprocess.run(
            [
                "mcap_manager",
                "merge",
                temp_dir,
                "--start",
                "2024-01-01T12:00:00Z",
                "--end",
                "2024-01-01T12:00:02Z",
                "--output",
                str(Path(temp_dir) / "merged.mcap"),
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
                temp_dir,
                "--start",
                "2024-01-01T12:00:00Z",
                "--end",
                "2024-01-01T12:00:02Z",
                "--include-topics",
                "topic1",
                "--exclude-topics",
                "topic2",
                "--output",
                str(Path(temp_dir) / "merged.mcap"),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0


def test_cli_merge():
    """Test the CLI merge functionality."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test MCAP files
        file1_path = Path(temp_dir) / "test1.mcap"
        file2_path = Path(temp_dir) / "test2.mcap"
        output_path = Path(temp_dir) / "merged.mcap"

        # Create test files
        base_time = int(
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1e9
        )
        create_test_mcap(str(file1_path), "topic1", base_time, {"value": "test1"})
        create_test_mcap(
            str(file2_path), "topic2", base_time + 1_000_000_000, {"value": "test2"}
        )

        # Test basic merge
        result = subprocess.run(
            [
                "mcap_manager",
                "merge",
                temp_dir,
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
                temp_dir,
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


def test_cli_merge_with_topics_file(tmp_path, runner):
    """Test merge command with topics from a file."""
    # Create test files
    root_dir = tmp_path / "root"
    root_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Create topics file
    topics_file = tmp_path / "topics.txt"
    topics_file.write_text("topic1\ntopic2\n")

    # Create test MCAP files
    create_test_mcap(root_dir / "test1.mcap", "topic1", 1000, {"value": "test1"})
    create_test_mcap(root_dir / "test2.mcap", "topic2", 2000, {"value": "test2"})
    create_test_mcap(root_dir / "test3.mcap", "topic3", 3000, {"value": "test3"})

    # Run merge command
    result = runner.invoke(
        cli,
        [
            "merge",
            str(root_dir),
            "--start",
            "2024-01-01T00:00:00Z",
            "--end",
            "2024-01-02T00:00:00Z",
            "--include-topics-file",
            str(topics_file),
            "--output",
            str(output_dir / "merged.mcap"),
        ],
    )

    assert result.exit_code == 0
    # The test files are created with timestamps that don't match the time range
    # Update the timestamps to match the test time range
    base_time = int(
        datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1e9
    )
    create_test_mcap(root_dir / "test1.mcap", "topic1", base_time, {"value": "test1"})
    create_test_mcap(
        root_dir / "test2.mcap", "topic2", base_time + 1_000_000_000, {"value": "test2"}
    )
    create_test_mcap(
        root_dir / "test3.mcap", "topic3", base_time + 2_000_000_000, {"value": "test3"}
    )

    # Run merge command again with updated timestamps
    result = runner.invoke(
        cli,
        [
            "merge",
            str(root_dir),
            "--start",
            "2024-01-01T00:00:00Z",
            "--end",
            "2024-01-02T00:00:00Z",
            "--include-topics-file",
            str(topics_file),
            "--output",
            str(output_dir / "merged.mcap"),
        ],
    )

    assert result.exit_code == 0
    assert "Found 2 matching MCAP files" in result.output


def test_cli_info_with_topics_file(tmp_path, runner):
    """Test info command with topics from a file."""
    # Create test files
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Create topics file
    topics_file = tmp_path / "topics.txt"
    topics_file.write_text("topic1\ntopic2\n")

    # Create test MCAP files
    create_test_mcap(root_dir / "test1.mcap", "topic1", 1000, {"value": "test1"})
    create_test_mcap(root_dir / "test2.mcap", "topic2", 2000, {"value": "test2"})
    create_test_mcap(root_dir / "test3.mcap", "topic3", 3000, {"value": "test3"})

    # Run info command
    result = runner.invoke(
        cli, ["info", str(root_dir), "--include-topics-file", str(topics_file)]
    )

    assert result.exit_code == 0
    assert "Found 3 MCAP files" in result.output


def test_cli_with_nonexistent_topics_file(tmp_path, runner):
    """Test CLI commands with a non-existent topics file."""
    # Create test directory
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Run merge command with non-existent topics file
    result = runner.invoke(
        cli,
        [
            "merge",
            str(root_dir),
            "--start",
            "2024-01-01T00:00:00Z",
            "--end",
            "2024-01-02T00:00:00Z",
            "--include-topics-file",
            "nonexistent.txt",
            "--output",
            str(tmp_path / "merged.mcap"),
        ],
    )

    assert result.exit_code != 0
    assert "Error: Topic file does not exist" in result.output


def test_cli_merge_with_time_range(tmp_path, runner):
    """Test merge command with time range option."""
    # Create test directory
    root_dir = tmp_path / "root"
    root_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Create test MCAP files with current timestamps
    base_time = int(datetime.now(timezone.utc).timestamp() * 1e9)
    create_test_mcap(root_dir / "test1.mcap", "topic1", base_time, {"value": "test1"})
    create_test_mcap(
        root_dir / "test2.mcap", "topic2", base_time - 1_000_000_000, {"value": "test2"}
    )  # 1 second ago
    create_test_mcap(
        root_dir / "test3.mcap", "topic3", base_time - 3_000_000_000, {"value": "test3"}
    )  # 3 seconds ago

    # Test with time range of 2 seconds
    result = runner.invoke(
        cli,
        [
            "merge",
            str(root_dir),
            "--time-range",
            "2",
            "--output",
            str(output_dir / "merged.mcap"),
        ],
    )

    assert result.exit_code == 0
    assert (
        "Found 2 matching MCAP files" in result.output
    )  # Should match test1.mcap and test2.mcap

    # Test with time range of 4 seconds
    result = runner.invoke(
        cli,
        [
            "merge",
            str(root_dir),
            "--time-range",
            "4",
            "--output",
            str(output_dir / "merged.mcap"),
        ],
    )

    assert result.exit_code == 0
    assert (
        "Found 3 matching MCAP files" in result.output
    )  # Should match all three files


def test_cli_merge_time_range_validation(tmp_path, runner):
    """Test validation of time range options."""
    # Create test directory
    root_dir = tmp_path / "root"
    root_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Test using both time range and start time
    result = runner.invoke(
        cli,
        [
            "merge",
            str(root_dir),
            "--time-range",
            "2",
            "--start",
            "2024-01-01T00:00:00Z",
            "--output",
            str(output_dir / "merged.mcap"),
        ],
    )

    assert result.exit_code != 0
    assert "--time-range cannot be used with --start or --end" in result.output

    # Test using both time range and end time
    result = runner.invoke(
        cli,
        [
            "merge",
            str(root_dir),
            "--time-range",
            "2",
            "--end",
            "2024-01-01T00:00:00Z",
            "--output",
            str(output_dir / "merged.mcap"),
        ],
    )

    assert result.exit_code != 0
    assert "--time-range cannot be used with --start or --end" in result.output

    # Test using neither time range nor start/end
    result = runner.invoke(
        cli, ["merge", str(root_dir), "--output", str(output_dir / "merged.mcap")]
    )

    assert result.exit_code != 0
    assert (
        "Either --time-range or both --start and --end must be provided"
        in result.output
    )
