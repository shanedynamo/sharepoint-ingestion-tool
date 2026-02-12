"""One-time full ingestion of all SharePoint documents into S3.

Designed to run on EC2 (no timeout limits) for initial ingestion of
several thousand documents.  Uses concurrent uploads (5 threads) while
keeping Graph API crawling serialized to respect rate limits.

Usage::

    python -m src.bulk_ingest [--dry-run] [--library NAME]
"""

import argparse
import csv
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from access_control import AccessControlMapper
from config import config
from graph_client import GraphClient
from s3_client import S3Client
from delta_tracker import DeltaTracker
from document_registry import DocumentRegistry
from utils.path_mapper import PathMapper

MAX_WORKERS = 5
PROGRESS_INTERVAL = 100

# Thread-local storage for per-thread S3 clients.
_local = threading.local()


# ===================================================================
# JSON structured logging (CloudWatch-friendly)
# ===================================================================

class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, config.log_level))


logger = logging.getLogger(__name__)


# ===================================================================
# Thread-safe stats tracker
# ===================================================================

class _Stats:
    """Accumulates ingestion statistics from multiple threads."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.ingested = 0
        self.skipped = 0
        self.bytes_transferred = 0
        self.errors = 0
        self.failures: list[dict] = []

    def record_ingested(self, size_bytes: int) -> None:
        with self._lock:
            self.ingested += 1
            self.bytes_transferred += size_bytes

    def record_skipped(self) -> None:
        with self._lock:
            self.skipped += 1

    def record_error(self, filename: str, error: str) -> None:
        with self._lock:
            self.errors += 1
            self.failures.append({
                "filename": filename,
                "error": error,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

    @property
    def total_processed(self) -> int:
        with self._lock:
            return self.ingested + self.skipped + self.errors

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "ingested": self.ingested,
                "skipped": self.skipped,
                "bytes_transferred": self.bytes_transferred,
                "errors": self.errors,
            }


# ===================================================================
# Upload worker (runs inside ThreadPoolExecutor)
# ===================================================================

def _upload_worker(
    doc: dict,
    s3_key: str,
    tags: dict[str, str],
    graph: GraphClient,
    registry: DocumentRegistry,
    stats: _Stats,
    dry_run: bool,
) -> None:
    """Download from SharePoint, upload to S3, register in DynamoDB.

    Each thread lazily creates its own :class:`S3Client` via
    thread-local storage so boto3 sessions are not shared.
    """
    if dry_run:
        stats.record_ingested(doc["size"])
        return

    try:
        # Thread-local S3 client (created once per pool thread)
        if not hasattr(_local, "s3"):
            _local.s3 = S3Client()
        s3 = _local.s3

        data = graph.download_file(doc["download_url"])

        s3.upload_document(
            content=data,
            s3_key=s3_key,
            content_type=doc["content_type"],
            tags=tags,
        )

        ext = os.path.splitext(doc["name"])[1].lower()
        registry.register_document({
            "s3_source_key": s3_key,
            "sp_item_id": doc["id"],
            "sp_path": doc["sharepoint_path"],
            "sp_library": doc["library_name"],
            "sp_last_modified": doc.get("last_modified", ""),
            "file_type": ext,
            "size_bytes": doc["size"],
        })

        stats.record_ingested(len(data))
    except Exception as exc:
        stats.record_error(doc["sharepoint_path"], str(exc))
        logger.exception("Failed to ingest: %s", doc["sharepoint_path"])


# ===================================================================
# Main ingestion routine
# ===================================================================

def run_bulk_ingestion(
    dry_run: bool = False,
    library_filter: str | None = None,
) -> int:
    """Crawl the entire SharePoint site and upload everything to S3.

    Returns 0 on success, 1 if any documents failed.
    """
    start_time = time.monotonic()
    start_ts = datetime.now(timezone.utc).isoformat()

    logger.info(json.dumps({
        "event": "bulk_ingestion_start",
        "site": config.sharepoint_site_name,
        "dry_run": dry_run,
        "library_filter": library_filter,
        "timestamp": start_ts,
    }))

    graph = GraphClient()
    registry = DocumentRegistry()
    delta_tracker = DeltaTracker()
    mapper = PathMapper(
        config.s3_bucket, config.s3_source_prefix, config.s3_extracted_prefix,
    )
    acl = AccessControlMapper()
    stats = _Stats()

    # ------------------------------------------------------------------
    # Discover site & libraries
    # ------------------------------------------------------------------
    site_id = graph.get_site_id()
    libraries = graph.list_document_libraries(site_id)

    if not libraries:
        logger.error("No document libraries found on site")
        return 1

    if library_filter:
        libraries = [lib for lib in libraries if lib["name"] == library_filter]
        if not libraries:
            logger.error("Library '%s' not found. Available: %s",
                         library_filter,
                         ", ".join(l["name"] for l in graph.list_document_libraries(site_id)))
            return 1

    logger.info(json.dumps({
        "event": "libraries_discovered",
        "count": len(libraries),
        "names": [lib["name"] for lib in libraries],
    }))

    # ------------------------------------------------------------------
    # Crawl & upload
    # ------------------------------------------------------------------
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for lib in libraries:
            drive_id = lib["id"]
            lib_name = lib["name"]
            lib_submitted = 0
            futures: list = []

            logger.info(json.dumps({
                "event": "library_crawl_start",
                "library": lib_name,
                "drive_id": drive_id,
            }))

            # Graph crawl is serial (rate-limit sensitive)
            for doc in graph.crawl_library(drive_id, library_name=lib_name):
                s3_key = mapper.to_s3_source_key(
                    doc["site_name"], doc["library_name"], doc["sharepoint_path"],
                )

                # Check if unchanged (serial — lightweight DynamoDB read)
                if not dry_run:
                    existing = registry.get_document(s3_key)
                    sp_last_modified = doc.get("last_modified", "")
                    if existing and existing.get("sp_last_modified") == sp_last_modified:
                        stats.record_skipped()
                        continue

                if not doc["download_url"]:
                    logger.warning("No download URL for %s, skipping",
                                   doc["sharepoint_path"])
                    stats.record_skipped()
                    continue

                tags = PathMapper.build_s3_tags(doc)
                access_tags = acl.map_document(lib_name, doc["sharepoint_path"])
                tags["access-tags"] = ",".join(access_tags)
                lib_submitted += 1

                mode = "DRY RUN" if dry_run else "Submitting"
                logger.info("%s: %d - %s (%d bytes)",
                            mode, lib_submitted, doc["name"], doc["size"])

                # Parallel upload: download → S3 → DynamoDB
                future = pool.submit(
                    _upload_worker,
                    doc=doc,
                    s3_key=s3_key,
                    tags=tags,
                    graph=graph,
                    registry=registry,
                    stats=stats,
                    dry_run=dry_run,
                )
                futures.append(future)

                # Progress summary every N documents
                if lib_submitted % PROGRESS_INTERVAL == 0:
                    snap = stats.snapshot()
                    logger.info(json.dumps({
                        "event": "progress",
                        "submitted": lib_submitted,
                        "ingested": snap["ingested"],
                        "skipped": snap["skipped"],
                        "errors": snap["errors"],
                        "bytes_transferred": _format_bytes(snap["bytes_transferred"]),
                    }))

            # Wait for all uploads in this library to finish
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    pass  # errors already recorded in _upload_worker

            logger.info(json.dumps({
                "event": "library_crawl_complete",
                "library": lib_name,
                "submitted": lib_submitted,
            }))

            # Establish a delta token for future daily syncs
            if not dry_run:
                _save_initial_delta_token(
                    graph, delta_tracker, drive_id, lib_submitted,
                )

    # ------------------------------------------------------------------
    # Error CSV
    # ------------------------------------------------------------------
    if stats.failures:
        _write_error_csv(stats.failures)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    duration = time.monotonic() - start_time
    end_ts = datetime.now(timezone.utc).isoformat()
    snap = stats.snapshot()

    summary = {
        "event": "bulk_ingestion_complete",
        "start": start_ts,
        "end": end_ts,
        "duration_seconds": round(duration, 1),
        "libraries_crawled": len(libraries),
        "documents_ingested": snap["ingested"],
        "documents_skipped": snap["skipped"],
        "bytes_transferred": snap["bytes_transferred"],
        "bytes_transferred_human": _format_bytes(snap["bytes_transferred"]),
        "errors": snap["errors"],
        "dry_run": dry_run,
    }
    logger.info(json.dumps(summary))

    if stats.failures:
        logger.error("Failed documents:")
        for f in stats.failures:
            logger.error("  %s — %s", f["filename"], f["error"])

    return 1 if stats.errors > 0 else 0


# ===================================================================
# Helpers
# ===================================================================

def _save_initial_delta_token(
    graph: GraphClient,
    delta_tracker: DeltaTracker,
    drive_id: str,
    items_processed: int,
) -> None:
    """Consume the initial delta response to get a token for future daily syncs."""
    try:
        logger.info("Establishing delta token for drive %s", drive_id)
        _, delta_token = graph.get_delta(drive_id, None)
        delta_tracker.save_delta_token(
            drive_id=drive_id,
            token=delta_token,
            last_sync=datetime.now(timezone.utc).isoformat(),
            items_processed=items_processed,
        )
    except Exception:
        logger.exception("Failed to save delta token for drive %s", drive_id)


def _write_error_csv(failures: list[dict]) -> None:
    """Write all failures to errors.csv for post-run review."""
    path = "errors.csv"
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["filename", "error", "timestamp"])
        writer.writeheader()
        writer.writerows(failures)
    logger.info("Error report written to %s (%d failures)", path, len(failures))


def _format_bytes(n: int | float) -> str:
    """Human-readable byte size (e.g. ``1.5 GB``)."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} PB"


# ===================================================================
# CLI entry point
# ===================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-time full ingestion of SharePoint documents into S3.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Crawl and log what would be ingested, but don't upload.",
    )
    parser.add_argument(
        "--library",
        type=str,
        default=None,
        metavar="NAME",
        help="Ingest only the named library (for testing).",
    )
    args = parser.parse_args()

    _configure_logging()
    exit_code = run_bulk_ingestion(
        dry_run=args.dry_run,
        library_filter=args.library,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
