"""
cli.py — command-line interface for the api-to-datalake ingestion pipeline.

Commands:

  run             Extract and load one or more sources (or all of them).
  list-sources    Print configured sources and their current watermarks.
  reset-watermark Delete the stored watermark for a specific source.

Examples::

    # Run all sources
    python cli.py run

    # Run only exchange_rates in dry-run mode (extract but do not upload)
    python cli.py run --source exchange_rates --dry-run

    # Verbose output (sets log level to DEBUG)
    python cli.py run --source bank_transactions --verbose

    # List sources and watermarks
    python cli.py list-sources

    # Reset watermark (forces full reload on next run)
    python cli.py reset-watermark --source bank_transactions
"""

import os
import sys
from pathlib import Path

import click

from src.utils.logger import _configure_logger, get_logger
from src.utils.watermark import WatermarkStore

log = get_logger(__name__)


def _load_sources_cfg() -> dict:
    """Load and return the sources configuration dict."""
    import yaml
    cfg_path = Path(__file__).parent / "config" / "sources.yaml"
    with open(cfg_path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data.get("sources", {})


# ── CLI group ─────────────────────────────────────────────────────────────────

@click.group()
def cli():
    """api-to-datalake — ingest REST / JDBC sources into Azure Data Lake."""


# ── run command ───────────────────────────────────────────────────────────────

@cli.command()
@click.option(
    "--source",
    "sources",
    multiple=True,
    help="Source name(s) to run. Repeat to specify multiple. Omit to run all.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Extract data but skip the ADLS upload. Useful for testing connectivity.",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="Set log level to DEBUG.",
)
def run(sources: tuple, dry_run: bool, verbose: bool):
    """Extract from configured sources and load Parquet files to ADLS.

    If --source is omitted every source in config/sources.yaml is processed.
    Failed sources are logged but do not abort the run.
    """
    if verbose:
        _configure_logger(level="DEBUG")
        log.debug("Verbose mode enabled")

    if dry_run:
        log.info("DRY RUN — uploads will be skipped")
        os.environ.setdefault("ADLS_DRY_RUN", "true")

    from src.pipeline import IngestionPipeline

    pipeline = IngestionPipeline()
    selected = list(sources) if sources else None

    try:
        summary = pipeline.run(source_names=selected)
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    # Print summary table
    click.echo("")
    click.echo(f"{'SOURCE':<25} {'STATUS':<8} {'ROWS':>8} {'DURATION':>10}  PATH")
    click.echo("-" * 80)
    has_errors = False
    for name, result in summary.items():
        status = result["status"]
        rows = result.get("rows", 0)
        ms = result.get("duration_ms", 0)
        path = result.get("path") or result.get("error", "")
        status_display = click.style(status, fg="green" if status == "ok" else "red")
        click.echo(f"{name:<25} {status:<8} {rows:>8} {ms:>8}ms  {path}")
        if status != "ok":
            has_errors = True

    click.echo("")
    if has_errors:
        click.echo(click.style("Some sources failed. Check logs for details.", fg="red"))
        sys.exit(1)
    else:
        click.echo(click.style("All sources completed successfully.", fg="green"))


# ── list-sources command ──────────────────────────────────────────────────────

@cli.command("list-sources")
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="Set log level to DEBUG.",
)
def list_sources(verbose: bool):
    """List all configured sources and their current watermark values."""
    if verbose:
        _configure_logger(level="DEBUG")

    sources_cfg = _load_sources_cfg()
    wm_store = WatermarkStore()
    all_wm = wm_store.all_watermarks()

    click.echo("")
    click.echo(f"  {'SOURCE':<25} {'TYPE':<6} {'SCHEDULE':<10} {'LAST WATERMARK'}")
    click.echo("  " + "-" * 70)

    for name, cfg in sources_cfg.items():
        src_type = cfg.get("type", "?")
        schedule = cfg.get("schedule", "?")
        watermark = all_wm.get(name, click.style("(none)", dim=True))
        click.echo(f"  {name:<25} {src_type:<6} {schedule:<10} {watermark}")

    click.echo("")
    click.echo(f"  Total: {len(sources_cfg)} source(s) configured")
    click.echo("")


# ── reset-watermark command ───────────────────────────────────────────────────

@cli.command("reset-watermark")
@click.option(
    "--source",
    required=True,
    help="Name of the source whose watermark should be reset.",
)
@click.option(
    "--yes",
    is_flag=True,
    default=False,
    help="Skip confirmation prompt.",
)
def reset_watermark(source: str, yes: bool):
    """Delete the stored watermark for SOURCE, forcing a full reload on next run.

    This is irreversible — the next pipeline run will re-ingest all historical
    data from the source's ``default_watermark`` value (or a full load if
    incremental is disabled).
    """
    sources_cfg = _load_sources_cfg()
    if source not in sources_cfg:
        click.echo(
            click.style(f"Error: source '{source}' not found in sources.yaml", fg="red"),
            err=True,
        )
        sys.exit(1)

    wm_store = WatermarkStore()
    current = wm_store.get_last_watermark(source)

    if current is None:
        click.echo(f"Source '{source}' has no stored watermark — nothing to reset.")
        return

    click.echo(f"Current watermark for '{source}': {current}")

    if not yes:
        confirmed = click.confirm(
            f"Reset watermark for '{source}'? This will force a full reload on next run.",
            default=False,
        )
        if not confirmed:
            click.echo("Aborted.")
            return

    wm_store.reset_watermark(source)
    click.echo(click.style(f"Watermark for '{source}' has been reset.", fg="yellow"))


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cli()
