# ===================================================================
# Lambda Layer — shared Python dependencies for all functions
# ===================================================================

resource "aws_lambda_layer_version" "shared_deps" {
  layer_name          = "sp-ingest-shared-deps"
  filename            = "${path.module}/../dist/lambda-layer.zip"
  source_code_hash    = filebase64sha256("${path.module}/../dist/lambda-layer.zip")
  compatible_runtimes = ["python3.11"]
  description         = "Shared dependencies: msal, requests, python-pptx, openpyxl"
}


# ===================================================================
# Lambda 1: Daily Sync — incremental delta crawl via Graph API
# ===================================================================

resource "aws_lambda_function" "daily_sync" {
  function_name = "sp-ingest-daily-sync"
  role          = aws_iam_role.daily_sync_lambda.arn
  handler       = "src.daily_sync.handler"
  runtime       = "python3.11"
  timeout       = 900
  memory_size   = 512

  filename         = "${path.module}/../dist/lambda-code.zip"
  source_code_hash = filebase64sha256("${path.module}/../dist/lambda-code.zip")

  layers = [aws_lambda_layer_version.shared_deps.arn]

  environment {
    variables = {
      SHAREPOINT_SITE_NAME   = var.sharepoint_site_name
      EXCLUDED_FOLDERS       = var.excluded_folders
      S3_BUCKET              = var.s3_bucket_name
      S3_SOURCE_PREFIX       = "source"
      S3_EXTRACTED_PREFIX    = "extracted"
      DYNAMODB_DELTA_TABLE   = var.delta_table_name
      DYNAMODB_REGISTRY_TABLE = var.registry_table_name
      SECRET_PREFIX          = "sp-ingest/"
      AWS_REGION_NAME        = var.aws_region
      LOG_LEVEL              = "INFO"
    }
  }
}


# ===================================================================
# Lambda 2: Textract Trigger — S3 event → start extraction
# ===================================================================

resource "aws_lambda_function" "textract_trigger" {
  function_name = "sp-ingest-textract-trigger"
  role          = aws_iam_role.textract_trigger_lambda.arn
  handler       = "src.textract_trigger.handler"
  runtime       = "python3.11"
  timeout       = 300
  memory_size   = 1024

  filename         = "${path.module}/../dist/lambda-code.zip"
  source_code_hash = filebase64sha256("${path.module}/../dist/lambda-code.zip")

  layers = [aws_lambda_layer_version.shared_deps.arn]

  environment {
    variables = {
      S3_BUCKET               = var.s3_bucket_name
      DYNAMODB_REGISTRY_TABLE = var.registry_table_name
      TEXTRACT_SNS_TOPIC_ARN  = aws_sns_topic.textract_notifications.arn
      TEXTRACT_SNS_ROLE_ARN   = aws_iam_role.textract_service.arn
      LOG_LEVEL               = "INFO"
    }
  }
}

resource "aws_lambda_permission" "s3_invoke_textract_trigger" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.textract_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.documents.arn
}


# ===================================================================
# Lambda 3: Textract Complete — SNS notification → build JSON twin
# ===================================================================

resource "aws_lambda_function" "textract_complete" {
  function_name = "sp-ingest-textract-complete"
  role          = aws_iam_role.textract_complete_lambda.arn
  handler       = "src.textract_complete.handler"
  runtime       = "python3.11"
  timeout       = 300
  memory_size   = 1024

  filename         = "${path.module}/../dist/lambda-code.zip"
  source_code_hash = filebase64sha256("${path.module}/../dist/lambda-code.zip")

  layers = [aws_lambda_layer_version.shared_deps.arn]

  environment {
    variables = {
      S3_BUCKET               = var.s3_bucket_name
      DYNAMODB_REGISTRY_TABLE = var.registry_table_name
      LOG_LEVEL               = "INFO"
    }
  }
}

resource "aws_lambda_permission" "sns_invoke_textract_complete" {
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.textract_complete.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.textract_notifications.arn
}
