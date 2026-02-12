"""AWS Textract job management.

Provides :class:`TextractClient` which starts asynchronous Textract jobs
(document analysis *and* text detection) and retrieves paginated results.

Usage::

    client = TextractClient()
    job_id = client.start_document_analysis("my-bucket", "source/doc.pdf")
    # ... wait for SNS notification ...
    result = client.get_document_analysis(job_id)
"""

import logging
import time

import boto3

from config import config

logger = logging.getLogger(__name__)


class TextractClient:
    """Start and retrieve Textract document analysis / text-detection jobs."""

    def __init__(
        self,
        region: str | None = None,
        sns_topic_arn: str | None = None,
        sns_role_arn: str | None = None,
    ) -> None:
        self._region = region or config.aws_region
        self._sns_topic_arn = sns_topic_arn or config.textract_sns_topic_arn
        self._sns_role_arn = sns_role_arn or config.textract_sns_role_arn
        self._client = boto3.client("textract", region_name=self._region)

    # ------------------------------------------------------------------ #
    # Start jobs
    # ------------------------------------------------------------------ #

    def start_document_analysis(self, s3_bucket: str, s3_key: str) -> str:
        """Start async document analysis (TABLES + FORMS) on a PDF in S3.

        Returns the Textract *JobId*.
        """
        params: dict = {
            "DocumentLocation": {
                "S3Object": {"Bucket": s3_bucket, "Name": s3_key},
            },
            "FeatureTypes": ["TABLES", "FORMS"],
            "OutputConfig": {
                "S3Bucket": s3_bucket,
                "S3Prefix": "textract-raw/",
            },
        }
        if self._sns_topic_arn and self._sns_role_arn:
            params["NotificationChannel"] = {
                "SNSTopicArn": self._sns_topic_arn,
                "RoleArn": self._sns_role_arn,
            }

        resp = self._client.start_document_analysis(**params)
        job_id = resp["JobId"]
        logger.info("Started document analysis %s for s3://%s/%s", job_id, s3_bucket, s3_key)
        return job_id

    def start_text_detection(self, s3_bucket: str, s3_key: str) -> str:
        """Start async text detection (no tables/forms) on a document in S3.

        Returns the Textract *JobId*.
        """
        params: dict = {
            "DocumentLocation": {
                "S3Object": {"Bucket": s3_bucket, "Name": s3_key},
            },
            "OutputConfig": {
                "S3Bucket": s3_bucket,
                "S3Prefix": "textract-raw/",
            },
        }
        if self._sns_topic_arn and self._sns_role_arn:
            params["NotificationChannel"] = {
                "SNSTopicArn": self._sns_topic_arn,
                "RoleArn": self._sns_role_arn,
            }

        resp = self._client.start_document_text_detection(**params)
        job_id = resp["JobId"]
        logger.info("Started text detection %s for s3://%s/%s", job_id, s3_bucket, s3_key)
        return job_id

    # ------------------------------------------------------------------ #
    # Retrieve results (paginated)
    # ------------------------------------------------------------------ #

    def get_document_analysis(self, job_id: str) -> dict:
        """Retrieve all pages of a document-analysis job.

        Follows ``NextToken`` until every block has been fetched and returns
        a single consolidated dict with the union of all ``Blocks``.
        """
        return self._get_paginated_results(
            self._client.get_document_analysis, job_id,
        )

    def get_text_detection(self, job_id: str) -> dict:
        """Retrieve all pages of a text-detection job.

        Same pagination logic as :meth:`get_document_analysis`.
        """
        return self._get_paginated_results(
            self._client.get_document_text_detection, job_id,
        )

    # ------------------------------------------------------------------ #
    # Polling helper (local testing only)
    # ------------------------------------------------------------------ #

    def wait_for_completion(
        self,
        job_id: str,
        poll_interval: int = 5,
        max_wait: int = 600,
    ) -> str:
        """Poll until *job_id* completes. Returns final status string.

        Intended **only** for local testing — production uses SNS.
        """
        elapsed = 0
        while elapsed < max_wait:
            resp = self._client.get_document_analysis(JobId=job_id)
            status = resp["JobStatus"]
            if status in ("SUCCEEDED", "FAILED"):
                return status
            logger.info("Job %s status: %s, waiting…", job_id, status)
            time.sleep(poll_interval)
            elapsed += poll_interval
        raise TimeoutError(
            f"Textract job {job_id} did not complete within {max_wait}s"
        )

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #

    @staticmethod
    def _get_paginated_results(api_method, job_id: str) -> dict:
        """Call *api_method* repeatedly, merging ``Blocks`` across pages."""
        all_blocks: list[dict] = []
        result: dict = {}
        next_token: str | None = None

        while True:
            kwargs: dict = {"JobId": job_id}
            if next_token:
                kwargs["NextToken"] = next_token

            resp = api_method(**kwargs)

            # First page: capture the top-level metadata
            if not result:
                result = {k: v for k, v in resp.items() if k != "Blocks"}

            all_blocks.extend(resp.get("Blocks", []))
            next_token = resp.get("NextToken")
            if not next_token:
                break

        result["Blocks"] = all_blocks
        # Remove stale NextToken from the merged result
        result.pop("NextToken", None)
        return result
