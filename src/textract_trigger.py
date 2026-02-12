"""Lambda handler: S3 PutObject event triggers extraction.

Routes each uploaded document to the appropriate extraction strategy:

* **textract-direct** (.pdf, .docx, .doc) — start async Textract job.
* **convert-then-textract** / **direct-extract** (.pptx, .xlsx) —
  extract text in-process with python-pptx / openpyxl, build a JSON
  twin immediately, and upload it to S3.
* **plain-text** (.txt) — read content as UTF-8, build twin immediately.
* Unsupported types are logged and skipped.
"""

import json
import logging
import os

import boto3

from config import config
from textract_client import TextractClient
from s3_client import S3Client
from document_registry import DocumentRegistry
from digital_twin import DigitalTwinBuilder
from utils.file_converter import FileConverter
from utils.path_mapper import PathMapper

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, config.log_level))


def handler(event: dict, context: object) -> dict:
    """Triggered by S3 event notification when a document lands in source/."""
    textract = TextractClient()
    s3 = S3Client()
    registry = DocumentRegistry()
    converter = FileConverter()
    builder = DigitalTwinBuilder()
    mapper = PathMapper(
        config.s3_bucket, config.s3_source_prefix, config.s3_extracted_prefix,
    )

    results = {"textract_jobs": 0, "direct_extracts": 0, "skipped": 0, "errors": 0}

    for record in event.get("Records", []):
        s3_bucket = record["s3"]["bucket"]["name"]
        s3_key = record["s3"]["object"]["key"]

        # Only process files under the source prefix
        if not s3_key.startswith(config.s3_source_prefix):
            results["skipped"] += 1
            continue

        ext = os.path.splitext(s3_key)[1].lower() if "." in s3_key else ""
        strategy = converter.get_extraction_strategy(ext)

        if strategy == "unsupported":
            logger.info("Unsupported file type %s, skipping: %s", ext, s3_key)
            results["skipped"] += 1
            continue

        doc = registry.get_document(s3_key)
        if not doc:
            logger.warning("No registry entry for %s, skipping", s3_key)
            results["skipped"] += 1
            continue

        try:
            if strategy == "textract-direct":
                _handle_textract(textract, registry, s3_bucket, s3_key)
                results["textract_jobs"] += 1

            elif strategy in ("convert-then-textract", "direct-extract"):
                _handle_direct_extract(
                    s3, registry, converter, builder, mapper,
                    s3_bucket, s3_key, ext, doc,
                )
                results["direct_extracts"] += 1

            elif strategy == "plain-text":
                _handle_plain_text(
                    s3, registry, builder, mapper,
                    s3_bucket, s3_key, doc,
                )
                results["direct_extracts"] += 1

        except Exception:
            logger.exception("Failed to process %s", s3_key)
            try:
                registry.update_textract_status(s3_key, "failed")
            except Exception:
                logger.exception("Failed to update status for %s", s3_key)
            results["errors"] += 1

    return {"statusCode": 200, "body": json.dumps(results)}


# ===================================================================
# Strategy handlers
# ===================================================================

def _handle_textract(
    textract: TextractClient,
    registry: DocumentRegistry,
    s3_bucket: str,
    s3_key: str,
) -> None:
    """Start an async Textract document-analysis job."""
    job_id = textract.start_document_analysis(s3_bucket, s3_key)
    registry.update_textract_status(s3_key, "processing", job_id=job_id)
    logger.info("Started Textract job %s for %s", job_id, s3_key)


def _handle_direct_extract(
    s3: S3Client,
    registry: DocumentRegistry,
    converter: FileConverter,
    builder: DigitalTwinBuilder,
    mapper: PathMapper,
    s3_bucket: str,
    s3_key: str,
    ext: str,
    doc: dict,
) -> None:
    """Extract text in-process using python-pptx / openpyxl."""
    # Download source from S3
    s3_raw = boto3.client("s3", region_name=config.aws_region)
    resp = s3_raw.get_object(Bucket=s3_bucket, Key=s3_key)
    content = resp["Body"].read()

    filename = os.path.basename(s3_key)

    # Extract text via the Lambda fallback path
    text_bytes = converter.convert_to_pdf_lambda(content, filename, ext)
    text = text_bytes.decode("utf-8")

    # Build the JSON twin
    twin = builder.build_twin_from_direct_extract(text, [], doc)
    twin_key = mapper.to_s3_extracted_key(s3_key)

    s3.upload_json_twin(twin, twin_key)

    registry.update_textract_status(
        s3_key, "completed", twin_key=twin_key,
    )
    logger.info("Direct extract complete for %s -> %s", s3_key, twin_key)


def _handle_plain_text(
    s3: S3Client,
    registry: DocumentRegistry,
    builder: DigitalTwinBuilder,
    mapper: PathMapper,
    s3_bucket: str,
    s3_key: str,
    doc: dict,
) -> None:
    """Read plain text content and build a twin."""
    s3_raw = boto3.client("s3", region_name=config.aws_region)
    resp = s3_raw.get_object(Bucket=s3_bucket, Key=s3_key)
    content = resp["Body"].read()
    text = content.decode("utf-8")

    twin = builder.build_twin_from_direct_extract(text, [], doc)
    twin_key = mapper.to_s3_extracted_key(s3_key)

    s3.upload_json_twin(twin, twin_key)

    registry.update_textract_status(
        s3_key, "completed", twin_key=twin_key,
    )
    logger.info("Plain text extract complete for %s -> %s", s3_key, twin_key)
