# MCAP Manager

A CLI tool for querying and managing MCAP files.

## Installation

### From Source

```bash
# Create and activate a virtual environment with uv
uv venv
source .venv/bin/activate

# Install the project in editable mode
uv pip install -e .
```

### From Git
```bash
# Install directly from git
uv pip install git+https://github.com/fireflyautomatix/mcap-manager.git
```

### Getting Help

To see available commands and options:

```bash
mcap_manager --help
```

To see help for a specific command:

```bash
mcap_manager info --help
mcap_manager merge --help
```

## Usage

### Get MCAP File Information

Display summary information about MCAP files in a directory:

```bash
mcap_manager info \
  ./bags \
  --include-topics /topic_a /topic_b \
  --exclude-topics /topic_c \
  --include-topics-file topics.txt \
  --exclude-topics-file excluded_topics.txt \
  --debug
```

### Merge MCAP Files

Merge matching MCAP files into a single output file:

```bash
mcap_manager merge \
  ./bags \
  --start 2025-04-08T15:07:32Z \
  --end 2025-04-08T16:07:32Z \
  --include-topics /topic_a /topic_b \
  --exclude-topics /topic_c \
  --include-topics-file topics.txt \
  --exclude-topics-file excluded_topics.txt \
  --output ./merged_output.mcap \
  --debug
```

Alternatively, you can use the `--time-range` option to specify a time range relative to the current time:

```bash
mcap_manager merge \
  ./bags \
  --time-range 3600 \  # Last hour
  --include-topics /topic_a /topic_b \
  --exclude-topics /topic_c \
  --include-topics-file topics.txt \
  --exclude-topics-file excluded_topics.txt \
  --output ./merged_output.mcap \
  --debug
```

#### Merge Options

- `root_dir`: Directory containing MCAP files to merge
- `--start`: Start time in ISO 8601 format
- `--end`: End time in ISO 8601 format
- `--time-range`: Time range in seconds from now (mutually exclusive with --start and --end)
- `--include-topics`: List of topics to include (can be specified multiple times)
- `--exclude-topics`: List of topics to exclude (can be specified multiple times)
- `--include-topics-file`: Path to a text file containing topics to include (one per line)
- `--exclude-topics-file`: Path to a text file containing topics to exclude (one per line)
- `--output`: Path to the output MCAP file
- `--debug`: Enable debug logging for skipped files

#### Topics File Format

Topics can be specified in a text file with one topic per line. Empty lines and whitespace are ignored. For example:

```text
/topic_a
/topic_b
/topic_c
```

## Development

```bash
# Install development dependencies
uv pip install -e ".[dev]"

# Run tests
pytest
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
