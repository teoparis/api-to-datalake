"""
IngestionPipeline — orchestrates extraction and loading for all configured sources.

Usage::

    pipeline = IngestionPipeline()
    summary = pipeline.run()               # run all sources
    summary = pipeline.run(["exchange_rates"])  # run a subset

Each source is run independently; a failure in one source does not abort
the others.  The returned summary dict lets callers inspect per-source
outcomes without parsing logs.
"""

import time
from pathlib import Path
from typing import Optional

import yaml

from src.extractors.jdbc_extractor import JDBCExtractor
from src.extractors.rest_extractor import RESTExtractor
from src.loaders.parquet_loader import ParquetLoader
from src.utils.logger import get_logger, set_correlation_id
from src.utils.watermark import WatermarkStore

log = get_logger(__name__)

_SOURCES_YAML = Path(__file__).parent.parent / "config" / "sources.yaml"


def _load_sources_config(path: Path = _SOURCES_YAML) -> dict:
    """Parse the YAML sources configuration file.

    Args:
        path: Path to ``sources.yaml``.

    Returns:
        Dict mapping source names to their configuration dicts.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the ``sources`` key is missing from the file.
    """
    if not path.exists():
        raise FileNotFoundError(f"Sources config not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if "sources" not in data:
        raise ValueError(f"'sources' key missing in {path}")
    return data["sources"]


def _make_extractor(source_type: str):
    """Return the correct extractor instance for *source_type*.

    Args:
        source_type: ``"rest"`` or ``"jdbc"``.

    Returns:
        Concrete :class:`~src.extractors.base_extractor.BaseExtractor` instance.

    Raises:
        ValueError: If *source_type* is not recognised.
    """
    if source_type == "rest":
        return RESTExtractor()
    if source_type == "jdbc":
        return JDBCExtractor()
    raise ValueError(f"Unknown source type: '{source_type}'. Supported: rest, jdbc")


class IngestionPipeline:
    """Orchestrate end-to-end ingestion for one or more data sources.

    Args:
        sources_path: Override path to ``sources.yaml`` (useful in tests).
        watermark_store: Override the :class:`~src.utils.watermark.WatermarkStore`
                         (useful in tests).
        container_name: ADLS container name.  Falls back to env var
                        ``ADLS_CONTAINER`` or ``"datalake"``.
        account_name: ADLS storage account name.  Falls back to env var
                      ``ADLS_ACCOUNT_NAME``.
        base_path: ADLS root path.  Falls back to ``ADLS_BASE_PATH`` or ``"raw"``.
    """

    def __init__(
        self,
        sources_path: Optional[Path] = None,
        watermark_store: Optional[WatermarkStore] = None,
        container_name: Optional[str] = None,
        account_name: Optional[str] = None,
        base_path: Optional[str] = None,
    ) -> None:
        import os

        self._sources = _load_sources_config(sources_path or _SOURCES_YAML)
        self._watermark_store = watermark_store or WatermarkStore()
        self._container = container_name or os.environ.get("ADLS_CONTAINER", "datalake")
        self._account_name = account_name or os.environ.get("ADLS_ACCOUNT_NAME", "")
        self._base_path = base_path or os.environ.get("ADLS_BASE_PATH", "raw")

        log.info(
            "IngestionPipeline initialised",
            source_count=len(self._sources),
            container=self._container,
        )

    # ── Single-source runner ──────────────────────────────────────────────────

    def _run_source(self, source_name: str, config: dict) -> dict:
        """Extract + load a single source and return its result summary.

        Args:
            source_name: Unique source key from ``sources.yaml``.
            config: Source configuration dict.

        Returns:
            Dict with keys ``status`` (``"ok"`` / ``"error"``), ``rows``,
            ``path``, ``duration_ms``, and optionally ``error``.
        """
        t_start = time.monotonic()
        source_type = config.get("type", "rest")
        target_path = config.get("target_path", f"raw/{source_name}")

        log.info("Starting source", source=source_name, type=source_type)

        # Retrieve last watermark
        watermark = self._watermark_store.get_last_watermark(source_name)

        # Extract
        extractor = _make_extractor(source_type)
        df = extractor.extract(config, watermark)

        row_count = len(df)
        log.info("Extraction done", source=source_name, rows=row_count)

        # Load
        loader = ParquetLoader(
            container_name=self._container,
            account_name=self._account_name,
            base_path=self._base_path,
        )
        load_result = loader.load(df, source_name=source_name, target_path=target_path)

        # Update watermark if the source is incremental and we got data
        incremental_cfg = config.get("incremental", {})
        wm_field = incremental_cfg.get("watermark_field") or incremental_cfg.get("watermark_column")
        if incremental_cfg.get("enabled") and wm_field and not df.empty and wm_field in df.columns:
            new_watermark = str(df[wm_field].max())
            self._watermark_store.update_watermark(source_name, new_watermark)
            log.info(
                "Watermark updated",
                source=source_name,
                field=wm_field,
                new_value=new_watermark,
            )

        duration_ms = int((time.monotonic() - t_start) * 1000)
        return {
            "status": "ok",
            "rows": row_count,
            "path": load_result.get("path"),
            "size_bytes": load_result.get("size_bytes", 0),
            "duration_ms": duration_ms,
        }

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self, source_names: Optional[list] = None) -> dict:
        """Run the ingestion pipeline for one or more sources.

        Sources that raise exceptions are logged as failures but do NOT abort
        the run — all remaining sources continue to execute.

        Args:
            source_names: List of source keys to run.  ``None`` means all
                          sources configured in ``sources.yaml``.

        Returns:
            Dict mapping each source name to its result summary::

                {
                    "exchange_rates": {
                        "status": "ok",
                        "rows": 170,
                        "path": "raw/exchange_rates/year=2025/month=04/...",
                        "size_bytes": 4096,
                        "duration_ms": 312,
                    },
                    "bank_transactions": {
                        "status": "error",
                        "error": "EnvironmentError: JDBC_HOST is not set",
                        "rows": 0,
                        "duration_ms": 5,
                    },
                }
        """
        run_id = set_correlation_id()
        log.info("Pipeline run started", run_id=run_id)

        # Determine which sources to run
        if source_names:
            unknown = [s for s in source_names if s not in self._sources]
            if unknown:
                raise ValueError(f"Unknown source(s): {unknown}")
            targets = {k: self._sources[k] for k in source_names}
        else:
            targets = self._sources

        summary: dict = {}

        for source_name, config in targets.items():
            try:
                result = self._run_source(source_name, config)
                summary[source_name] = result
                log.info(
                    "Source completed",
                    source=source_name,
                    rows=result["rows"],
                    duration_ms=result["duration_ms"],
                )
            except Exception as exc:
                duration_ms = 0
                log.exception(
                    "Source failed — continuing",
                    source=source_name,
                    error=str(exc),
                )
                summary[source_name] = {
                    "status": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "rows": 0,
                    "duration_ms": duration_ms,
                }

        ok_count = sum(1 for v in summary.values() if v["status"] == "ok")
        err_count = len(summary) - ok_count
        log.info(
            "Pipeline run complete",
            run_id=run_id,
            total=len(summary),
            ok=ok_count,
            errors=err_count,
        )
        return summary
