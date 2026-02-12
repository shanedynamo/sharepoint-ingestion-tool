#!/usr/bin/env python3
"""Smoke test: validates the complete pipeline locally before AWS deployment.

Runs 8 sequential tests that exercise the full ingestion and extraction
pipeline using real Azure AD credentials (read-only) for Graph API tests
and LocalStack for all AWS service interactions.

Usage:
    Called by scripts/smoke-test-local.sh (preferred), or directly:
        AWS_ENDPOINT_URL=http://localhost:4566 \
        AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
        .venv/bin/python scripts/smoke_test.py
"""

import importlib
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Project setup — must run before any src/ imports
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
os.chdir(PROJECT_ROOT)

# Suppress noisy library-level logging; we control our own output.
logging.basicConfig(level=logging.WARNING, format="%(name)s: %(message)s")

# Reload config so it reads env vars set by the shell wrapper + .env
import config as config_mod
importlib.reload(config_mod)
from config import config

# ---------------------------------------------------------------------------
# Terminal colours
# ---------------------------------------------------------------------------
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

# ---------------------------------------------------------------------------
# Shared state passed between tests
# ---------------------------------------------------------------------------


class _State:
    graph = None
    site_id = None
    libraries = []
    crawl_docs = []
    ingested_s3_key = None
    ingested_doc = None


state = _State()

# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------
_results: list[tuple[int, str, bool, str]] = []


def run_test(num: int, name: str, fn):
    """Execute *fn*, capture pass/fail, and pretty-print the outcome."""
    print(f"\n{'=' * 60}")
    print(f"  TEST {num}: {name}")
    print(f"{'=' * 60}")
    try:
        fn()
        _results.append((num, name, True, ""))
        print(f"\n  {GREEN}[PASS]{RESET}")
    except Exception as exc:
        _results.append((num, name, False, str(exc)))
        print(f"\n  {RED}[FAIL]{RESET} {exc}")
        if os.environ.get("SMOKE_VERBOSE"):
            traceback.print_exc()


# ===================================================================
# TEST 1: Graph API Connectivity
# ===================================================================
def test_graph_connectivity():
    from graph_client import GraphClient

    print("  Authenticating with Azure AD...")
    state.graph = GraphClient()

    print("  Calling get_site_id('Dynamo')...")
    state.site_id = state.graph.get_site_id("Dynamo")

    assert state.site_id, "get_site_id returned empty/None"
    print(f"  Site ID: {state.site_id}")


# ===================================================================
# TEST 2: Library Discovery
# ===================================================================
def test_library_discovery():
    assert state.site_id, "Skipped — no site_id from TEST 1"

    print(f"  Listing libraries for site {state.site_id[:30]}...")
    state.libraries = state.graph.list_document_libraries(state.site_id)

    assert len(state.libraries) > 0, "No document libraries found"

    print(f"  Found {len(state.libraries)} libraries:")
    for lib in state.libraries:
        print(f"    - {lib['name']}  (drive: {lib['id'][:16]}...)")


# ===================================================================
# TEST 3: Path Mapping
# ===================================================================
def test_path_mapping():
    from utils.path_mapper import PathMapper

    mapper = PathMapper("dynamo-ai-documents", "source", "extracted")

    cases = [
        ("Dynamo", "HR-Policies", "/Employee Handbook.docx"),
        ("Dynamo", "Finance", "/2025/Budget Report.xlsx"),
        ("Dynamo", "Legal", "/Contracts/NDA (Final).pdf"),
    ]

    print("  Verifying S3 key generation:")
    for site, lib, sp_path in cases:
        key = mapper.to_s3_source_key(site, lib, sp_path)
        twin = mapper.to_s3_extracted_key(key)

        assert key.startswith("source/"), f"Bad source prefix: {key}"
        assert twin.startswith("extracted/"), f"Bad extracted prefix: {twin}"
        assert twin.endswith(".json"), f"Twin not .json: {twin}"
        assert " " not in key, f"Spaces in S3 key: {key}"

        print(f"    {sp_path}")
        print(f"      -> {key}")
        print(f"      -> {twin}")

    # Tag generation
    tags = PathMapper.build_s3_tags({
        "site_name": "Dynamo",
        "library_name": "HR",
        "sharepoint_path": "/docs/test.pdf",
        "author": "Test User",
        "last_modified": "2025-01-01T00:00:00Z",
        "content_type": "application/pdf",
        "file_type": ".pdf",
    })

    required_tags = ["sp-site", "sp-library", "sp-path", "file-type"]
    for tag in required_tags:
        assert tag in tags, f"Missing required tag: {tag}"

    print(f"  Tags generated: {list(tags.keys())}")


# ===================================================================
# TEST 4: Dry Run Crawl
# ===================================================================
def test_dry_run_crawl():
    assert state.libraries, "Skipped — no libraries from TEST 2"

    first_lib = state.libraries[0]
    lib_name = first_lib["name"]
    drive_id = first_lib["id"]

    print(f"  Crawling library '{lib_name}' (dry run, max 50 docs)...")

    doc_count = 0
    state.crawl_docs = []
    for doc in state.graph.crawl_library(drive_id, library_name=lib_name):
        doc_count += 1
        if len(state.crawl_docs) < 10:
            state.crawl_docs.append(doc)
        if doc_count >= 50:
            print(f"    (capped at 50 — library may have more)")
            break

    assert doc_count > 0, f"No documents found in library '{lib_name}'"

    # Show sample documents
    ext_counts: dict[str, int] = {}
    for d in state.crawl_docs:
        ext_counts[d["file_type"]] = ext_counts.get(d["file_type"], 0) + 1

    print(f"  Found {doc_count} documents in library '{lib_name}'")
    print(f"  File types sampled: {dict(ext_counts)}")
    for d in state.crawl_docs[:3]:
        print(f"    - {d['name']} ({d['size']} bytes)")


# ===================================================================
# TEST 5: Single Document Ingestion
# ===================================================================
def test_single_document_ingestion():
    assert state.crawl_docs, "Skipped — no documents from TEST 4"

    from s3_client import S3Client
    from document_registry import DocumentRegistry
    from utils.path_mapper import PathMapper

    # Pick first doc with a download URL
    doc = None
    for candidate in state.crawl_docs:
        if candidate.get("download_url"):
            doc = candidate
            break
    assert doc, "No documents with download_url found"

    state.ingested_doc = doc

    s3 = S3Client()
    registry = DocumentRegistry()
    mapper = PathMapper(config.s3_bucket, config.s3_source_prefix, config.s3_extracted_prefix)

    # 1. Download from SharePoint
    print(f"  Downloading: {doc['name']} ({doc['size']} bytes)...")
    content = state.graph.download_file(doc["download_url"])
    print(f"  Downloaded {len(content)} bytes")

    # 2. Build S3 key & tags
    s3_key = mapper.to_s3_source_key(
        doc["site_name"], doc["library_name"], doc["sharepoint_path"],
    )
    tags = PathMapper.build_s3_tags(doc)
    state.ingested_s3_key = s3_key

    # 3. Upload to LocalStack S3
    result = s3.upload_document(
        content, s3_key, content_type=doc["content_type"], tags=tags,
    )
    print(f"  Uploaded to S3: {s3_key}")
    print(f"  ETag: {result['etag']}")

    # 4. Register in DynamoDB
    registry.register_document({
        "s3_source_key": s3_key,
        "sp_item_id": doc["id"],
        "sp_path": doc["sharepoint_path"],
        "sp_library": doc["library_name"],
        "sp_last_modified": doc.get("last_modified", ""),
        "file_type": doc["file_type"],
        "size_bytes": doc["size"],
    })

    # 5. Verify S3 object exists
    assert s3.document_exists(s3_key), f"S3 object not found: {s3_key}"
    print(f"  S3 object verified")

    # 6. Verify tags
    import boto3
    s3_raw = boto3.client("s3", region_name=config.aws_region)
    tag_resp = s3_raw.get_object_tagging(Bucket=config.s3_bucket, Key=s3_key)
    actual_tags = {t["Key"]: t["Value"] for t in tag_resp["TagSet"]}
    assert "sp-site" in actual_tags, f"Missing sp-site tag. Got: {actual_tags}"
    print(f"  Tags verified: {list(actual_tags.keys())}")

    # 7. Verify DynamoDB entry
    db_doc = registry.get_document(s3_key)
    assert db_doc is not None, "Registry entry not found in DynamoDB"
    assert db_doc["sp_item_id"] == doc["id"]
    print(f"  Registry entry verified: status={db_doc['textract_status']}")


# ===================================================================
# TEST 6: Direct Extraction (PPTX)
# ===================================================================
def test_direct_extraction():
    from s3_client import S3Client
    from document_registry import DocumentRegistry

    # Generate a test PPTX fixture
    sys.path.insert(0, str(PROJECT_ROOT / "tests"))
    from fixtures.generate_fixtures import generate_pptx

    pptx_path = generate_pptx()
    content = pptx_path.read_bytes()
    source_key = "source/Dynamo/SmokeTest/test-presentation.pptx"

    s3 = S3Client()
    registry = DocumentRegistry()

    # 1. Upload PPTX to LocalStack S3
    s3.upload_document(
        content, source_key,
        content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
    )
    print(f"  Uploaded PPTX: {source_key} ({len(content)} bytes)")

    # 2. Register in DynamoDB
    registry.register_document({
        "s3_source_key": source_key,
        "sp_item_id": "smoke-test-pptx",
        "sp_path": "/SmokeTest/test-presentation.pptx",
        "sp_library": "SmokeTest",
        "sp_last_modified": "2025-01-01T00:00:00Z",
        "file_type": ".pptx",
        "size_bytes": len(content),
    })

    # 3. Invoke textract_trigger handler with S3 event
    #    PPTX uses the direct-extract path — no Textract API calls needed
    event = {
        "Records": [{
            "s3": {
                "bucket": {"name": config.s3_bucket},
                "object": {"key": source_key},
            },
        }],
    }

    from textract_trigger import handler
    result = handler(event, None)
    body = json.loads(result["body"])

    assert body["direct_extracts"] == 1, f"Expected 1 direct extract, got: {body}"
    assert body["errors"] == 0, f"Handler reported errors: {body}"
    print(f"  Handler result: {body}")

    # 4. Verify JSON twin was created
    doc_record = registry.get_document(source_key)
    assert doc_record["textract_status"] == "completed", \
        f"Status is '{doc_record['textract_status']}', expected 'completed'"

    twin_key = doc_record["s3_twin_key"]
    assert twin_key, "No s3_twin_key in registry"
    assert s3.document_exists(twin_key), f"Twin not found in S3: {twin_key}"

    # 5. Verify JSON twin structure
    import boto3
    s3_raw = boto3.client("s3", region_name=config.aws_region)
    resp = s3_raw.get_object(Bucket=config.s3_bucket, Key=twin_key)
    twin = json.loads(resp["Body"].read())

    assert twin.get("schema_version") == "2.0", \
        f"Wrong schema version: {twin.get('schema_version')}"
    assert twin.get("extracted_text"), "No extracted_text in twin"
    assert isinstance(twin.get("pages"), list), "pages is not a list"

    print(f"  Twin key: {twin_key}")
    print(f"  Schema: {twin['schema_version']}")
    print(f"  Extracted text: {len(twin['extracted_text'])} chars")
    print(f"  Pages: {len(twin['pages'])}")


# ===================================================================
# TEST 7: Delta Tracker
# ===================================================================
def test_delta_tracker():
    from delta_tracker import DeltaTracker

    tracker = DeltaTracker()
    drive_id = "smoke-test-drive-001"

    # Save a delta token
    tracker.save_delta_token(
        drive_id=drive_id,
        token="smoke-delta-token-abc123",
        last_sync="2025-06-01T12:00:00Z",
        items_processed=42,
    )

    # Retrieve and verify
    token = tracker.get_delta_token(drive_id)
    assert token == "smoke-delta-token-abc123", f"Token mismatch: {token}"
    print(f"  Saved token for drive '{drive_id}'")
    print(f"  Retrieved: {token}")

    # Verify atomic increment by saving again
    tracker.save_delta_token(
        drive_id=drive_id,
        token="smoke-delta-token-v2",
        last_sync="2025-06-02T12:00:00Z",
        items_processed=10,
    )

    token2 = tracker.get_delta_token(drive_id)
    assert token2 == "smoke-delta-token-v2", f"Updated token mismatch: {token2}"

    # Verify sync_count via raw DynamoDB read
    import boto3
    dynamo = boto3.resource("dynamodb", region_name=config.aws_region)
    table = dynamo.Table(config.dynamodb_delta_table)
    item = table.get_item(Key={"drive_id": drive_id})["Item"]

    from decimal import Decimal
    assert item["sync_count"] == Decimal("2"), \
        f"sync_count should be 2, got {item['sync_count']}"

    print(f"  Updated token verified: {token2}")
    print(f"  Atomic sync_count = {item['sync_count']} (2 saves)")


# ===================================================================
# TEST 8: Document Registry Stats
# ===================================================================
def test_registry_stats():
    from document_registry import DocumentRegistry

    registry = DocumentRegistry()
    stats = registry.get_stats()

    print(f"  Total documents: {stats['total']}")
    print(f"  By type:    {stats['by_type']}")
    print(f"  By status:  {stats['by_status']}")
    print(f"  By library: {stats['by_library']}")

    # We registered at least 2 docs: one in Test 5, one PPTX in Test 6
    assert stats["total"] >= 2, \
        f"Expected at least 2 registered docs, got {stats['total']}"

    # Verify the PPTX from Test 6 shows as completed
    completed = stats["by_status"].get("completed", 0)
    assert completed >= 1, "Expected at least 1 completed document"
    print(f"  Completed extractions: {completed}")


# ===================================================================
# Main
# ===================================================================
def main():
    print(f"\n{BOLD}{'=' * 60}")
    print(f"  SMOKE TEST — Pre-Deployment Pipeline Validation")
    print(f"{'=' * 60}{RESET}")
    print(f"  LocalStack:  {os.environ.get('AWS_ENDPOINT_URL', '(not set)')}")
    print(f"  S3 Bucket:   {config.s3_bucket}")
    print(f"  Region:      {config.aws_region}")
    print(f"  Site:        {config.sharepoint_site_name}")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"  Timestamp:   {ts}")

    run_test(1, "Graph API Connectivity", test_graph_connectivity)
    run_test(2, "Library Discovery", test_library_discovery)
    run_test(3, "Path Mapping", test_path_mapping)
    run_test(4, "Dry Run Crawl", test_dry_run_crawl)
    run_test(5, "Single Document Ingestion", test_single_document_ingestion)
    run_test(6, "Direct Extraction (PPTX)", test_direct_extraction)
    run_test(7, "Delta Tracker", test_delta_tracker)
    run_test(8, "Document Registry Stats", test_registry_stats)

    # ---- Summary ----
    passed = sum(1 for _, _, ok, _ in _results if ok)
    failed = sum(1 for _, _, ok, _ in _results if not ok)
    total = len(_results)

    print(f"\n\n{'=' * 60}")
    if failed == 0:
        print(f"  {GREEN}{BOLD}SMOKE TEST RESULTS: {passed}/{total} PASSED{RESET}")
    else:
        print(f"  {RED}{BOLD}SMOKE TEST RESULTS: {passed}/{total} PASSED, {failed} FAILED{RESET}")
    print(f"{'=' * 60}")

    if failed == 0:
        print(f"  {GREEN}Ready for AWS deployment.{RESET}")
    else:
        print(f"\n  {RED}Failed tests:{RESET}")
        for num, name, ok, detail in _results:
            if not ok:
                print(f"    TEST {num}: {name}")
                print(f"      {DIM}{detail}{RESET}")
        print(f"\n  {YELLOW}Fix failures before deploying.{RESET}")
        print(f"  {DIM}Set SMOKE_VERBOSE=1 for full tracebacks.{RESET}")

    print()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
