import click
from typing import List, Optional
from .query import query_mcap_files, QueryResult
from .utils import format_timestamp, parse_iso_time
from .mcap_utils import (
    setup_logging,
    parse_topics,
    process_mcap_file,
    find_mcap_files,
    ensure_output_dir,
    read_topics_from_file,
)
from .merge import merge_mcap_files
from .config import get_root_dir
import time
from pathlib import Path
from tqdm import tqdm
import sys
from . import __version__
from datetime import datetime, timezone, timedelta


def display_results_summary(
    results: List[QueryResult], show_topics: bool = True
) -> None:
    """
    Display a summary of the query results.

    Args:
        results: List of QueryResult objects to display
        show_topics: Whether to show the matching topics for each file
    """
    if not results:
        click.echo("No matching MCAP files found.")
        return

    click.echo(f"Found {len(results)} matching MCAP files:")
    for result in results:
        if show_topics:
            click.echo("Matching topics:")
            for topic in sorted(result.matching_topics):
                click.echo(f"  - {topic}")


@click.group()
@click.version_option(version=__version__, prog_name="MCAP Manager")
def cli():
    """MCAP Manager - Query and manage MCAP files."""
    pass


@cli.command()
@click.argument("root_dir", type=click.Path(exists=True), required=False)
@click.option("--start", help="Start time (ISO 8601)")
@click.option("--end", help="End time (ISO 8601)")
@click.option("--time-range", type=int, help="Time range in seconds from now")
@click.option(
    "--include-topics",
    multiple=True,
    help="Only these topics (repeat or comma-separated)",
)
@click.option(
    "--exclude-topics",
    multiple=True,
    help="Exclude these topics (repeat or comma-separated)",
)
@click.option(
    "--include-topics-file",
    type=click.Path(),
    multiple=True,
    help="Path to a text file containing topics to include (one per line). Can be specified multiple times.",
)
@click.option(
    "--exclude-topics-file",
    type=click.Path(),
    multiple=True,
    help="Path to a text file containing topics to exclude (one per line). Can be specified multiple times.",
)
@click.option("--output", required=True, help="Output MCAP file path")
@click.option("--debug", is_flag=True, help="Enable debug logging for skipped files")
@click.option(
    "--latched-transient-output-msgs",
    type=int,
    default=1,
    help="Number of transient messages to include before the start timestamp (default: 1)",
)
def merge(
    root_dir: Optional[str],
    start: Optional[str],
    end: Optional[str],
    time_range: Optional[int],
    include_topics: Optional[List[str]] = None,
    exclude_topics: Optional[List[str]] = None,
    include_topics_file: Optional[List[str]] = None,
    exclude_topics_file: Optional[List[str]] = None,
    output: str = None,
    debug: bool = False,
    latched_transient_output_msgs: int = 1,
):
    """Merge matching MCAP files into a single output file."""
    logger = setup_logging(debug)

    # Validate time range options
    if time_range is not None:
        if start is not None or end is not None:
            raise click.UsageError("--time-range cannot be used with --start or --end")
        # Calculate time range from now
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(seconds=time_range)
        start = start_time.isoformat()
        end = end_time.isoformat()
    else:
        if start is None or end is None:
            raise click.UsageError(
                "Either --time-range or both --start and --end must be provided"
            )

    # Validate latched-transient-output-msgs
    if latched_transient_output_msgs < 1:
        raise click.UsageError("--latched-transient-output-msgs must be at least 1")

    # Use configured root_dir if not provided
    if root_dir is None:
        root_dir = get_root_dir()
        logger.info(f"Using configured root directory: {root_dir}")

    # Parse topics from command line and files
    include_topics_list = parse_topics(include_topics)
    exclude_topics_list = parse_topics(exclude_topics)

    # Handle multiple include-topics-file
    if include_topics_file:
        all_file_topics = []
        for file_path in include_topics_file:
            if not Path(file_path).exists():
                click.echo("Error: Topic file does not exist", err=True)
                sys.exit(1)
            try:
                topics = read_topics_from_file(file_path)
                if not topics:
                    continue
                all_file_topics.extend(topics)
            except (FileNotFoundError, IOError) as e:
                click.echo("Error: Topic file does not exist", err=True)
                sys.exit(1)
        if not all_file_topics:
            click.echo("No topics specified in topic files", err=True)
            sys.exit(1)
        include_topics_list.extend(all_file_topics)

    # Handle multiple exclude-topics-file
    if exclude_topics_file:
        all_file_topics = []
        for file_path in exclude_topics_file:
            if not Path(file_path).exists():
                click.echo("Error: Topic file does not exist", err=True)
                sys.exit(1)
            try:
                topics = read_topics_from_file(file_path)
                if not topics:
                    continue
                all_file_topics.extend(topics)
            except (FileNotFoundError, IOError) as e:
                click.echo("Error: Topic file does not exist", err=True)
                sys.exit(1)
        exclude_topics_list.extend(all_file_topics)

    # Parse start time as nanoseconds for transient message timestamp adjustment
    start_ns = None
    if start is not None:
        start_ns = parse_iso_time(start)

    # Get main results
    results = query_mcap_files(
        root_dir=root_dir,
        start_time=start,
        end_time=end,
        include_topics=include_topics_list,
        exclude_topics=exclude_topics_list,
        logger=logger,
    )

    # Get transient results if requested
    transient_dir = Path(root_dir) / "transient_output"
    if latched_transient_output_msgs > 0:
        if not transient_dir.exists():
            click.echo(
                "No transient messages found (transient_output directory does not exist)"
            )
        else:
            transient_results = query_mcap_files(
                root_dir=str(transient_dir),
                start_time=start,
                end_time=end,
                include_topics=include_topics_list,
                exclude_topics=exclude_topics_list,
                logger=logger,
            )
            results.extend(transient_results)

    display_results_summary(results, show_topics=False)
    if not results:
        return

    ensure_output_dir(output)

    click.echo("Starting merge operation...")
    start_time = time.time()

    merge_mcap_files(
        results=results,
        output_path=output,
        include_topics=include_topics_list,
        exclude_topics=exclude_topics_list,
        logger=logger,
        latched_transient_output_msgs=latched_transient_output_msgs,
        start_ns=start_ns,
    )

    elapsed_time = time.time() - start_time
    click.echo(f"Merge completed in {elapsed_time:.2f} seconds")
    click.echo(f"Successfully merged files into: {output}")


@cli.command()
@click.argument("root_dir", type=click.Path(exists=True), required=False)
@click.option(
    "--include-topics",
    multiple=True,
    help="Only these topics (repeat or comma-separated)",
)
@click.option(
    "--exclude-topics",
    multiple=True,
    help="Exclude these topics (repeat or comma-separated)",
)
@click.option(
    "--include-topics-file",
    type=click.Path(exists=True),
    help="Path to a text file containing topics to include (one per line)",
)
@click.option(
    "--exclude-topics-file",
    type=click.Path(exists=True),
    help="Path to a text file containing topics to exclude (one per line)",
)
@click.option("--debug", is_flag=True, help="Enable debug logging for skipped files")
def info(
    root_dir: Optional[str],
    include_topics: Optional[List[str]] = None,
    exclude_topics: Optional[List[str]] = None,
    include_topics_file: Optional[str] = None,
    exclude_topics_file: Optional[str] = None,
    debug: bool = False,
):
    """Display summary information about MCAP files in the directory."""
    logger = setup_logging(debug)

    # Use configured root_dir if not provided
    if root_dir is None:
        root_dir = get_root_dir()
        logger.info(f"Using configured root directory: {root_dir}")

    # Parse topics from command line and files
    include_topics_list = parse_topics(include_topics)
    exclude_topics_list = parse_topics(exclude_topics)

    if include_topics_file:
        try:
            include_topics_list.extend(read_topics_from_file(include_topics_file))
        except (FileNotFoundError, IOError) as e:
            click.echo("Error: Topic file does not exist", err=True)
            return

    if exclude_topics_file:
        try:
            exclude_topics_list.extend(read_topics_from_file(exclude_topics_file))
        except (FileNotFoundError, IOError) as e:
            click.echo("Error: Topic file does not exist", err=True)
            return

    mcap_files = find_mcap_files(root_dir)

    if not mcap_files:
        click.echo("No MCAP files found in the specified directory.")
        return

    click.echo(f"Found {len(mcap_files)} MCAP files")

    total_size = 0
    first_timestamp = None
    last_timestamp = None

    with tqdm(total=len(mcap_files), desc="Processing files", unit="file") as pbar:
        for mcap_file in mcap_files:
            file_size = Path(mcap_file).stat().st_size
            total_size += file_size

            for topic, timestamp in process_mcap_file(
                mcap_file,
                include_topics=set(include_topics_list),
                exclude_topics=set(exclude_topics_list),
                logger=logger,
            ):
                if first_timestamp is None or timestamp < first_timestamp:
                    first_timestamp = timestamp
                if last_timestamp is None or timestamp > last_timestamp:
                    last_timestamp = timestamp

            pbar.update(1)

    # Display summary
    click.echo("Summary:")
    click.echo(f"Total files: {len(mcap_files)}")
    click.echo(f"Total size: {total_size / (1024*1024):.2f} MB")
    if first_timestamp and last_timestamp:
        click.echo(
            f"Time range: {format_timestamp(first_timestamp)} to {format_timestamp(last_timestamp)}"
        )
        duration = (last_timestamp - first_timestamp) / 1e9  # Convert to seconds
        click.echo(f"Duration: {duration:.2f} seconds")


@cli.command()
@click.argument("root_dir", type=click.Path(exists=True))
def set_root_dir(root_dir: str):
    """Set the default root directory for MCAP files."""
    from .config import set_root_dir as config_set_root_dir

    config_set_root_dir(root_dir)
    click.echo(f"Default root directory set to: {root_dir}")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        cli.main(["--help"])
    else:
        cli()
