resource "aws_s3_bucket" "documents" {
  bucket = var.s3_bucket_name

  tags = {
    Project = "dynamo-ai-platform"
    Service = "sharepoint-ingest"
  }
}

resource "aws_s3_bucket_versioning" "documents" {
  bucket = aws_s3_bucket.documents.id
  versioning_configuration {
    status = "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "documents" {
  bucket = aws_s3_bucket.documents.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "documents" {
  bucket                  = aws_s3_bucket.documents.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Allow Textract service to read source documents and write output.
# Async Textract jobs access S3 using the service principal, not the caller.
resource "aws_s3_bucket_policy" "textract_access" {
  bucket = aws_s3_bucket.documents.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "TextractReadSource"
        Effect    = "Allow"
        Principal = { Service = "textract.amazonaws.com" }
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.documents.arn}/source/*"
      },
      {
        Sid       = "TextractWriteOutput"
        Effect    = "Allow"
        Principal = { Service = "textract.amazonaws.com" }
        Action    = "s3:PutObject"
        Resource  = "${aws_s3_bucket.documents.arn}/textract-raw/*"
      },
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.documents]
}

# ---------------------------------------------------------------------------
# S3 event notifications — trigger Lambda on PutObject for supported types.
# Each suffix filter requires its own notification configuration block.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_notification" "textract_trigger" {
  bucket = aws_s3_bucket.documents.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.textract_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "source/"
    filter_suffix       = ".pdf"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.textract_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "source/"
    filter_suffix       = ".docx"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.textract_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "source/"
    filter_suffix       = ".pptx"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.textract_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "source/"
    filter_suffix       = ".xlsx"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.textract_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "source/"
    filter_suffix       = ".txt"
  }

  depends_on = [aws_lambda_permission.s3_invoke_textract_trigger]
}
