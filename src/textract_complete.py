"""Lambda handler: SNS notification when Textract job completes.

Parses the SNS message to get the Textract JobId, retrieves the full
result, builds a JSON digital twin, uploads it to S3, and updates the
document registry.

Failed Textract jobs are recorded with ``textract_status = "failed"``.
"""

import json
import logging

from config import config
from textract_client import TextractClient
from s3_client import S3Client
from document_registry import DocumentRegistry
from digital_twin import DigitalTwinBuilder
from utils.path_mapper import PathMapper

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, config.log_level))


def handler(event: dict, context: object) -> dict:
    """Triggered by SNS when Textract finishes a job."""
    textract = TextractClient()
    s3 = S3Client()
    registry = DocumentRegistry()
    builder = DigitalTwinBuilder()
    mapper = PathMapper(
        config.s3_bucket, config.s3_source_prefix, config.s3_extracted_prefix,
    )

    results = {"twins_built": 0, "failed": 0, "errors": 0}

    for record in event.get("Records", []):
        try:
            message = json.loads(record["Sns"]["Message"])
            job_id = message.get("JobId", "")
            status = message.get("Status", "")
            s3_key = (
                message.get("DocumentLocation", {}).get("S3ObjectName", "")
            )

            # ----------------------------------------------------------
            # Textract reported failure
            # ----------------------------------------------------------
            if status != "SUCCEEDED":
                logger.error(
                    "Textract job %s finished with status: %s", job_id, status,
                )
                if s3_key:
                    try:
                        registry.update_textract_status(
                            s3_key, "failed", job_id=job_id,
                        )
                    except Exception:
                        logger.exception(
                            "Failed to update registry for failed job %s",
                            job_id,
                        )
                results["failed"] += 1
                continue

            # ----------------------------------------------------------
            # Look up the document in the registry
            # ----------------------------------------------------------
            doc = registry.get_document(s3_key)
            if not doc:
                logger.warning(
                    "No registry entry found for job %s (key=%s)",
                    job_id, s3_key,
                )
                results["errors"] += 1
                continue

            # ----------------------------------------------------------
            # Retrieve full Textract results
            # ----------------------------------------------------------
            textract_result = textract.get_document_analysis(job_id)

            # ----------------------------------------------------------
            # Build and upload JSON twin
            # ----------------------------------------------------------
            twin = builder.build_twin_from_textract(textract_result, doc)
            twin_key = mapper.to_s3_extracted_key(s3_key)

            # Carry forward source tags onto the twin
            source_tags: dict[str, str] = {}
            for tag_key in ("sp_library", "sp_path", "sp_item_id"):
                val = doc.get(tag_key, "")
                if val:
                    source_tags[tag_key.replace("_", "-")] = val

            s3.upload_json_twin(twin, twin_key, tags=source_tags)

            # ----------------------------------------------------------
            # Mark as completed in the registry
            # ----------------------------------------------------------
            registry.update_textract_status(
                s3_key, "completed", job_id=job_id, twin_key=twin_key,
            )
            results["twins_built"] += 1

            logger.info(
                "Built twin for %s (job %s) -> %s",
                s3_key, job_id, twin_key,
            )

        except Exception:
            logger.exception("Error processing SNS record")
            # Attempt to mark the document as failed
            try:
                if s3_key:
                    registry.update_textract_status(s3_key, "failed")
            except Exception:
                logger.exception("Failed to update registry on error")
            results["errors"] += 1

    return {"statusCode": 200, "body": json.dumps(results)}
