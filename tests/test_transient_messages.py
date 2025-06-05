import pytest
from pathlib import Path
from datetime import datetime, timezone
from mcap_manager.cli import cli
from mcap.reader import make_reader
from tests.conftest import create_test_mcap


def test_merge_with_transient_messages_default(
    runner, temp_dir, sample_mcap_files, transient_mcap_files, base_time
):
    """Test merge with default transient message behavior (1 message)."""
    output_file = temp_dir / "output.mcap"
    start_time = datetime.fromtimestamp(base_time / 1e9, tz=timezone.utc).isoformat()
    end_time = datetime.fromtimestamp(
        (base_time + 3_000_000_000) / 1e9, tz=timezone.utc
    ).isoformat()

    result = runner.invoke(
        cli,
        [
            "merge",
            "--start",
            start_time,
            "--end",
            end_time,
            "--output",
            str(output_file),
            "--latched-transient-output-msgs",
            "1",
            str(temp_dir),
        ],
    )

    assert result.exit_code == 0
    assert output_file.exists()


def test_merge_with_transient_messages_custom_count(
    runner, temp_dir, sample_mcap_files, transient_mcap_files, base_time
):
    """Test merge with custom transient message count."""
    output_file = temp_dir / "output.mcap"
    start_time = datetime.fromtimestamp(base_time / 1e9, tz=timezone.utc).isoformat()
    end_time = datetime.fromtimestamp(
        (base_time + 3_000_000_000) / 1e9, tz=timezone.utc
    ).isoformat()

    result = runner.invoke(
        cli,
        [
            "merge",
            "--start",
            start_time,
            "--end",
            end_time,
            "--output",
            str(output_file),
            "--latched-transient-output-msgs",
            "2",
            str(temp_dir),
        ],
    )

    assert result.exit_code == 0
    assert output_file.exists()


def test_merge_with_transient_messages_no_transients(
    runner, temp_dir, sample_mcap_files, base_time
):
    """Test merge when no transient messages exist."""
    output_file = temp_dir / "output.mcap"
    start_time = datetime.fromtimestamp(base_time / 1e9, tz=timezone.utc).isoformat()
    end_time = datetime.fromtimestamp(
        (base_time + 3_000_000_000) / 1e9, tz=timezone.utc
    ).isoformat()

    result = runner.invoke(
        cli,
        [
            "merge",
            "--start",
            start_time,
            "--end",
            end_time,
            "--output",
            str(output_file),
            "--latched-transient-output-msgs",
            "1",
            str(temp_dir),
        ],
    )

    assert result.exit_code == 0
    assert output_file.exists()


def test_merge_with_transient_messages_missing_folder(
    runner, temp_dir, sample_mcap_files, base_time
):
    """Test merge when transient_outputs folder doesn't exist."""
    output_file = temp_dir / "output.mcap"
    start_time = datetime.fromtimestamp(base_time / 1e9, tz=timezone.utc).isoformat()
    end_time = datetime.fromtimestamp(
        (base_time + 3_000_000_000) / 1e9, tz=timezone.utc
    ).isoformat()

    # Ensure transient_outputs doesn't exist
    transient_dir = temp_dir / "transient_outputs"
    if transient_dir.exists():
        transient_dir.rmdir()

    result = runner.invoke(
        cli,
        [
            "merge",
            "--start",
            start_time,
            "--end",
            end_time,
            "--output",
            str(output_file),
            "--latched-transient-output-msgs",
            "1",
            str(temp_dir),
        ],
    )

    assert result.exit_code == 0
    assert output_file.exists()
    assert "No transient messages found" in result.output


def test_merge_with_transient_messages_and_topic_filter(
    runner, temp_dir, sample_mcap_files, transient_mcap_files, base_time
):
    """Test merge with transient messages and topic filtering."""
    output_file = temp_dir / "output.mcap"
    start_time = datetime.fromtimestamp(base_time / 1e9, tz=timezone.utc).isoformat()
    end_time = datetime.fromtimestamp(
        (base_time + 3_000_000_000) / 1e9, tz=timezone.utc
    ).isoformat()

    result = runner.invoke(
        cli,
        [
            "merge",
            "--start",
            start_time,
            "--end",
            end_time,
            "--output",
            str(output_file),
            "--latched-transient-output-msgs",
            "1",
            "--include-topics",
            "transient_topic1",
            str(temp_dir),
        ],
    )

    assert result.exit_code == 0
    assert output_file.exists()


def test_merge_with_transient_messages_invalid_count(
    runner, temp_dir, sample_mcap_files, transient_mcap_files, base_time
):
    """Test merge with invalid transient message count."""
    output_file = temp_dir / "output.mcap"
    start_time = datetime.fromtimestamp(base_time / 1e9, tz=timezone.utc).isoformat()
    end_time = datetime.fromtimestamp(
        (base_time + 3_000_000_000) / 1e9, tz=timezone.utc
    ).isoformat()

    result = runner.invoke(
        cli,
        [
            "merge",
            "--start",
            start_time,
            "--end",
            end_time,
            "--output",
            str(output_file),
            "--latched-transient-output-msgs",
            "0",
            str(temp_dir),
        ],
    )

    assert result.exit_code != 0  # Should fail with invalid count


def test_merge_with_transient_messages_timestamp_handling(runner, temp_dir, base_time):
    """Test that transient messages before start time are assigned the start time."""
    output_file = temp_dir / "output.mcap"
    start_time = datetime.fromtimestamp(base_time / 1e9, tz=timezone.utc).isoformat()
    end_time = datetime.fromtimestamp(
        (base_time + 3_000_000_000) / 1e9, tz=timezone.utc
    ).isoformat()

    # Create a regular message at base_time + 1s
    regular_file = temp_dir / "regular.mcap"
    create_test_mcap(
        regular_file, "regular_topic", base_time + 1_000_000_000, {"value": "regular"}
    )

    # Create a transient message before base_time
    transient_dir = temp_dir / "transient_outputs"
    transient_dir.mkdir(exist_ok=True)
    transient_file = transient_dir / "transient.mcap"
    create_test_mcap(
        transient_file,
        "transient_topic",
        base_time - 2_000_000_000,
        {"value": "transient"},
    )

    result = runner.invoke(
        cli,
        [
            "merge",
            "--start",
            start_time,
            "--end",
            end_time,
            "--output",
            str(output_file),
            "--latched-transient-output-msgs",
            "1",
            str(temp_dir),
        ],
    )

    assert result.exit_code == 0
    assert output_file.exists()

    # Verify the timestamps in the output file
    with open(output_file, "rb") as f:
        reader = make_reader(f)
        messages = list(reader.iter_messages())

        # Should have 2 messages: the regular message and the transient message
        assert len(messages) == 2

        # Find the transient message
        transient_msg = next(
            msg for _, channel, msg in messages if channel.topic == "transient_topic"
        )
        # The transient message should have the start time as its timestamp
        assert transient_msg.log_time == base_time
