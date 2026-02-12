"""Lambda handler for daily incremental SharePoint sync via Graph delta API.

Triggered by EventBridge on a daily schedule.  For each document library:

1. Retrieve the stored delta token from DynamoDB.
2. Call the Graph delta API to discover new / modified / deleted items.
3. Download changed files → upload to S3 → register in DynamoDB.
4. Delete removed files from S3 + DynamoDB.
5. Save the new delta token for the next run.
"""

import json
import logging
import os
from datetime import datetime, timezone

from access_control import AccessControlMapper
from config import config
from graph_client import GraphClient
from s3_client import S3Client
from delta_tracker import DeltaTracker
from document_registry import DocumentRegistry
from utils.path_mapper import PathMapper

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, config.log_level))


def handler(event: dict, context: object) -> dict:
    """EventBridge-triggered Lambda: fetch delta changes and sync to S3."""
    graph = GraphClient()
    s3 = S3Client()
    delta_tracker = DeltaTracker()
    registry = DocumentRegistry()
    mapper = PathMapper(
        config.s3_bucket, config.s3_source_prefix, config.s3_extracted_prefix,
    )
    acl = AccessControlMapper()

    site_id = graph.get_site_id()
    libraries = graph.list_document_libraries(site_id)

    if not libraries:
        logger.error("No document libraries found")
        return {"statusCode": 200, "body": json.dumps({"error": "no libraries"})}

    stats = {"created": 0, "updated": 0, "deleted": 0, "skipped": 0, "errors": 0}

    for lib in libraries:
        drive_id = lib["id"]
        lib_name = lib["name"]
        delta_token = delta_tracker.get_delta_token(drive_id)
        items_processed = 0

        try:
            changes, new_delta_token = graph.get_delta(drive_id, delta_token)
        except Exception:
            logger.exception("Failed to get delta for library %s", lib_name)
            stats["errors"] += 1
            continue

        for item in changes:
            name = item.get("name", "")
            sp_id = item.get("id", "")

            # ----------------------------------------------------------
            # Deletions
            # ----------------------------------------------------------
            if "deleted" in item:
                try:
                    parent_path = item.get("parentReference", {}).get("path", "")
                    if name and parent_path:
                        sp_path = _extract_sp_path(parent_path, name, drive_id)
                        s3_key = mapper.to_s3_source_key(
                            config.sharepoint_site_name, lib_name, sp_path,
                        )
                        s3.delete_document(s3_key)
                        registry.delete_document(s3_key)
                    else:
                        logger.warning(
                            "Cannot resolve s3_key for deleted item %s, "
                            "skipping S3/registry cleanup",
                            sp_id,
                        )
                    stats["deleted"] += 1
                    items_processed += 1
                except Exception:
                    logger.exception("Failed to process deletion: %s", sp_id)
                    stats["errors"] += 1
                continue

            # Skip folders and non-file items
            if "folder" in item:
                continue
            if "file" not in item:
                continue

            # Skip items in excluded folders
            parent_path = item.get("parentReference", {}).get("path", "")
            if any(exc in parent_path for exc in config.excluded_folders):
                stats["skipped"] += 1
                continue

            sp_path = _extract_sp_path(parent_path, name, drive_id)
            s3_key = mapper.to_s3_source_key(
                config.sharepoint_site_name, lib_name, sp_path,
            )

            # ----------------------------------------------------------
            # Check if unchanged
            # ----------------------------------------------------------
            sp_last_modified = item.get("lastModifiedDateTime", "")
            existing = registry.get_document(s3_key)
            if existing and existing.get("sp_last_modified") == sp_last_modified:
                stats["skipped"] += 1
                continue

            # ----------------------------------------------------------
            # Download and upload
            # ----------------------------------------------------------
            content_type = item.get("file", {}).get(
                "mimeType", "application/octet-stream",
            )
            download_url = item.get("@microsoft.graph.downloadUrl", "")

            if not download_url:
                logger.warning("No download URL for delta item %s, skipping", name)
                stats["skipped"] += 1
                continue

            try:
                data = graph.download_file(download_url)

                access_tags = acl.map_document(lib_name, sp_path)

                tags = PathMapper.build_s3_tags({
                    "site_name": config.sharepoint_site_name,
                    "library_name": lib_name,
                    "sharepoint_path": sp_path,
                    "name": name,
                    "file_type": os.path.splitext(name)[1].lower(),
                    "last_modified": sp_last_modified,
                    "id": sp_id,
                })
                tags["access-tags"] = ",".join(access_tags)

                s3.upload_document(
                    content=data,
                    s3_key=s3_key,
                    content_type=content_type,
                    tags=tags,
                )

                ext = os.path.splitext(name)[1].lower()
                registry.register_document({
                    "s3_source_key": s3_key,
                    "sp_item_id": sp_id,
                    "sp_path": sp_path,
                    "sp_library": lib_name,
                    "sp_last_modified": sp_last_modified,
                    "file_type": ext,
                    "size_bytes": item.get("size", 0),
                })

                if existing:
                    stats["updated"] += 1
                else:
                    stats["created"] += 1
                items_processed += 1

            except Exception:
                logger.exception("Failed to sync: %s", name)
                stats["errors"] += 1

        # Save the new delta token regardless of individual item errors
        try:
            delta_tracker.save_delta_token(
                drive_id=drive_id,
                token=new_delta_token,
                last_sync=datetime.now(timezone.utc).isoformat(),
                items_processed=items_processed,
            )
        except Exception:
            logger.exception("Failed to save delta token for drive %s", drive_id)

    logger.info("Daily sync complete: %s", stats)
    return {"statusCode": 200, "body": json.dumps(stats)}


def _extract_sp_path(parent_path: str, name: str, drive_id: str) -> str:
    """Strip the Graph drive prefix from a parentReference path."""
    sp_path = f"{parent_path}/{name}"
    prefix = f"/drives/{drive_id}/root:"
    if sp_path.startswith(prefix):
        sp_path = sp_path[len(prefix):]
    return sp_path
