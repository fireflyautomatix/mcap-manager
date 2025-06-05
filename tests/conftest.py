import pytest
import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from mcap.writer import Writer
from click.testing import CliRunner


@pytest.fixture
def runner():
    """Create a Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def base_time():
    """Return a base timestamp for test data."""
    return int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1e9)


def create_test_mcap(file_path: Path, topic: str, timestamp: int, data: dict):
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


@pytest.fixture
def sample_mcap_files(temp_dir, base_time):
    """Create a set of sample MCAP files for testing."""
    files = {
        "file1": temp_dir / "test1.mcap",
        "file2": temp_dir / "test2.mcap",
        "file3": temp_dir / "test3.mcap",
    }

    # Create test files with different timestamps
    create_test_mcap(files["file1"], "topic1", base_time, {"value": "test1"})
    create_test_mcap(
        files["file2"], "topic2", base_time + 1_000_000_000, {"value": "test2"}
    )
    create_test_mcap(
        files["file3"], "topic3", base_time + 2_000_000_000, {"value": "test3"}
    )

    return files


@pytest.fixture
def transient_mcap_files(temp_dir, base_time):
    """Create a set of transient MCAP files for testing."""
    transient_dir = temp_dir / "transient_outputs"
    transient_dir.mkdir()

    files = {
        "transient1": transient_dir / "transient1.mcap",
        "transient2": transient_dir / "transient2.mcap",
        "transient3": transient_dir / "transient3.mcap",
    }

    # Create transient files with different timestamps
    # One before the base time
    create_test_mcap(
        files["transient1"],
        "transient_topic1",
        base_time - 2_000_000_000,
        {"value": "transient1"},
    )
    # One at base time
    create_test_mcap(
        files["transient2"], "transient_topic2", base_time, {"value": "transient2"}
    )
    # One after base time
    create_test_mcap(
        files["transient3"],
        "transient_topic3",
        base_time + 1_000_000_000,
        {"value": "transient3"},
    )

    return files


@pytest.fixture
def topics_file(temp_dir):
    """Create a topics file for testing."""
    topics_file = temp_dir / "topics.txt"
    topics_file.write_text("topic1\ntopic2\n")
    return topics_file
