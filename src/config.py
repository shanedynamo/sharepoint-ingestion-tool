"""Configuration loaded from environment variables."""

import os
from dataclasses import dataclass, field

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not needed in Lambda — env vars set by Terraform


@dataclass(frozen=True)
class Config:
    # Azure / Microsoft Graph
    azure_client_id: str = os.getenv("AZURE_CLIENT_ID", "")
    azure_tenant_id: str = os.getenv("AZURE_TENANT_ID", "")
    azure_client_secret: str = os.getenv("AZURE_CLIENT_SECRET", "")
    sharepoint_site_name: str = os.getenv("SHAREPOINT_SITE_NAME", "Dynamo")
    excluded_folders: list[str] = field(
        default_factory=lambda: os.getenv("EXCLUDED_FOLDERS", "Drafts,drafts").split(",")
    )

    # AWS S3
    s3_bucket: str = os.getenv("S3_BUCKET", "dynamo-ai-documents")
    s3_source_prefix: str = os.getenv("S3_SOURCE_PREFIX", "source")
    s3_extracted_prefix: str = os.getenv("S3_EXTRACTED_PREFIX", "extracted")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")

    # DynamoDB
    dynamodb_delta_table: str = os.getenv("DYNAMODB_DELTA_TABLE", "sp-ingest-delta-tokens")
    dynamodb_registry_table: str = os.getenv(
        "DYNAMODB_REGISTRY_TABLE", "sp-ingest-document-registry"
    )

    # Textract
    textract_sns_topic_arn: str = os.getenv("TEXTRACT_SNS_TOPIC_ARN", "")
    textract_sns_role_arn: str = os.getenv("TEXTRACT_SNS_ROLE_ARN", "")

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


config = Config()
