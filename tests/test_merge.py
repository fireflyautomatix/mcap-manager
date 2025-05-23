import pytest
import tempfile
import os
import json
from mcap.writer import Writer
from mcap_manager.mcap_utils import setup_logging, process_mcap_file


def test_merge_mcap_files():
    """Test merging MCAP files with topic filters."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test MCAP files
        file1_path = os.path.join(temp_dir, "test1.mcap")
        file2_path = os.path.join(temp_dir, "test2.mcap")

        # Create first test file
        with open(file1_path, "wb") as stream:
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
                schema_id=schema_id,
                topic="topic1",
                message_encoding="json",
                metadata={},
            )

            writer.add_message(
                channel_id=channel_id,
                log_time=1000,
                data=json.dumps({"value": "test1"}).encode("utf-8"),
                publish_time=1000,
            )

            writer.finish()

        # Create second test file
        with open(file2_path, "wb") as stream:
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
                schema_id=schema_id,
                topic="topic2",
                message_encoding="json",
                metadata={},
            )

            writer.add_message(
                channel_id=channel_id,
                log_time=2000,
                data=json.dumps({"value": "test2"}).encode("utf-8"),
                publish_time=2000,
            )

            writer.finish()

        logger = setup_logging(debug=True)

        # Test merging without filters
        messages1 = list(process_mcap_file(file1_path, logger=logger))
        messages2 = list(process_mcap_file(file2_path, logger=logger))
        assert len(messages1) == 1
        assert len(messages2) == 1
        assert messages1[0][0] == "topic1"
        assert messages2[0][0] == "topic2"

        # Test merging with include filter
        messages1 = list(
            process_mcap_file(file1_path, include_topics={"topic1"}, logger=logger)
        )
        messages2 = list(
            process_mcap_file(file2_path, include_topics={"topic1"}, logger=logger)
        )
        assert len(messages1) == 1
        assert len(messages2) == 0

        # Test merging with exclude filter
        messages1 = list(
            process_mcap_file(file1_path, exclude_topics={"topic1"}, logger=logger)
        )
        messages2 = list(
            process_mcap_file(file2_path, exclude_topics={"topic1"}, logger=logger)
        )
        assert len(messages1) == 0
        assert len(messages2) == 1
