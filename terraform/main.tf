terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "dynamo-terraform-state-760560299079"
    key    = "sharepoint-ingest/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Service     = "sharepoint-ingest"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
