variable "project_name" {
  description = "Top-level project name for tagging"
  type        = string
  default     = "dynamo-ai-platform"
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "s3_bucket_name" {
  description = "S3 bucket for document storage"
  type        = string
  default     = "dynamo-ai-documents"
}

variable "delta_table_name" {
  description = "DynamoDB table for delta tokens"
  type        = string
  default     = "sp-ingest-delta-tokens"
}

variable "registry_table_name" {
  description = "DynamoDB table for document registry"
  type        = string
  default     = "sp-ingest-document-registry"
}

variable "alert_email" {
  description = "Email address for SNS alert notifications"
  type        = string
  default     = ""
}

variable "sharepoint_site_name" {
  description = "SharePoint site name for Graph API crawling"
  type        = string
  default     = ""
}

variable "excluded_folders" {
  description = "Comma-separated list of SharePoint folder paths to exclude from sync"
  type        = string
  default     = ""
}

# -------------------------------------------------------------------
# Bulk EC2 instance (temporary — set to false after use)
# -------------------------------------------------------------------

variable "enable_bulk_instance" {
  description = "Set to true to create the temporary EC2 bulk loader, false to destroy it"
  type        = bool
  default     = false
}

variable "bulk_key_pair_name" {
  description = "EC2 key pair name for SSH access to the bulk loader (must already exist in AWS)"
  type        = string
  default     = ""
}

variable "bulk_admin_cidr" {
  description = "CIDR block allowed to SSH into the bulk loader (e.g. 203.0.113.10/32)"
  type        = string
  default     = ""
}
