# ---------------------------------------------------------------------------
# Delta Tokens — stores Graph API delta links for incremental sync.
# PK: drive_id (S)
# ---------------------------------------------------------------------------
resource "aws_dynamodb_table" "delta_tokens" {
  name         = var.delta_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "drive_id"

  attribute {
    name = "drive_id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }
}

# ---------------------------------------------------------------------------
# Document Registry — tracks every document through the ingest lifecycle.
# PK: s3_source_key (S)
# GSI: textract-status-index  (PK: textract_status, SK: ingested_at)
# GSI: sp-library-index       (PK: sp_library, SK: sp_last_modified)
# ---------------------------------------------------------------------------
resource "aws_dynamodb_table" "document_registry" {
  name         = var.registry_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "s3_source_key"

  attribute {
    name = "s3_source_key"
    type = "S"
  }

  attribute {
    name = "textract_status"
    type = "S"
  }

  attribute {
    name = "ingested_at"
    type = "S"
  }

  attribute {
    name = "sp_library"
    type = "S"
  }

  attribute {
    name = "sp_last_modified"
    type = "S"
  }

  global_secondary_index {
    name            = "textract_status-index"
    hash_key        = "textract_status"
    range_key       = "ingested_at"
    projection_type = "ALL"
  }

  global_secondary_index {
    name            = "sp_library-index"
    hash_key        = "sp_library"
    range_key       = "sp_last_modified"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }
}
