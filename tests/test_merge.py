import pytest
from mcap_manager.mcap_utils import setup_logging, process_mcap_file


def test_merge_mcap_files(sample_mcap_files, temp_dir):
    """Test merging MCAP files with topic filters."""
    logger = setup_logging(debug=True)

    # Test merging without filters
    messages1 = list(process_mcap_file(str(sample_mcap_files["file1"]), logger=logger))
    messages2 = list(process_mcap_file(str(sample_mcap_files["file2"]), logger=logger))
    assert len(messages1) == 1
    assert len(messages2) == 1
    assert messages1[0][0] == "topic1"
    assert messages2[0][0] == "topic2"

    # Test merging with include filter
    messages1 = list(
        process_mcap_file(str(sample_mcap_files["file1"]), include_topics={"topic1"}, logger=logger)
    )
    messages2 = list(
        process_mcap_file(str(sample_mcap_files["file2"]), include_topics={"topic1"}, logger=logger)
    )
    assert len(messages1) == 1
    assert len(messages2) == 0

    # Test merging with exclude filter
    messages1 = list(
        process_mcap_file(str(sample_mcap_files["file1"]), exclude_topics={"topic1"}, logger=logger)
    )
    messages2 = list(
        process_mcap_file(str(sample_mcap_files["file2"]), exclude_topics={"topic1"}, logger=logger)
    )
    assert len(messages1) == 0
    assert len(messages2) == 1
