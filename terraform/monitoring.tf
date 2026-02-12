# ===================================================================
# CloudWatch monitoring: log groups, metric filters, dashboard, alarms
# ===================================================================

locals {
  metric_namespace = "SP-Ingest"
}


# ===================================================================
# SNS Alert Topic
# ===================================================================

resource "aws_sns_topic" "alerts" {
  name = "sp-ingest-alerts"
}

resource "aws_sns_topic_subscription" "alert_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}


# ===================================================================
# CloudWatch Log Groups — 30-day retention
# ===================================================================

resource "aws_cloudwatch_log_group" "daily_sync" {
  name              = "/sp-ingest/daily-sync"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "textract_trigger" {
  name              = "/sp-ingest/textract-trigger"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "textract_complete" {
  name              = "/sp-ingest/textract-complete"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "bulk_ingest" {
  name              = "/sp-ingest/bulk-ingest"
  retention_in_days = 30
}


# ===================================================================
# Metric Filters — parse structured JSON logs
# ===================================================================

# --- Documents synced (daily sync: "Daily sync complete" with stats) ---
resource "aws_cloudwatch_log_metric_filter" "documents_synced" {
  name           = "sp-ingest-documents-synced"
  log_group_name = aws_cloudwatch_log_group.daily_sync.name
  pattern        = "{ $.message = \"Daily sync complete*\" }"

  metric_transformation {
    name          = "DocumentsSynced"
    namespace     = local.metric_namespace
    value         = "1"
    default_value = "0"
  }
}

# --- Documents created (daily sync: register document) ---
resource "aws_cloudwatch_log_metric_filter" "documents_created" {
  name           = "sp-ingest-documents-created"
  log_group_name = aws_cloudwatch_log_group.daily_sync.name
  pattern        = "\"Registered document\""

  metric_transformation {
    name          = "DocumentsCreated"
    namespace     = local.metric_namespace
    value         = "1"
    default_value = "0"
  }
}

# --- Documents deleted (daily sync) ---
resource "aws_cloudwatch_log_metric_filter" "documents_deleted" {
  name           = "sp-ingest-documents-deleted"
  log_group_name = aws_cloudwatch_log_group.daily_sync.name
  pattern        = "\"Deleted registry entry\""

  metric_transformation {
    name          = "DocumentsDeleted"
    namespace     = local.metric_namespace
    value         = "1"
    default_value = "0"
  }
}

# --- Textract completed (twin built successfully) ---
resource "aws_cloudwatch_log_metric_filter" "textract_completed" {
  name           = "sp-ingest-textract-completed"
  log_group_name = aws_cloudwatch_log_group.textract_complete.name
  pattern        = "\"Built twin for\""

  metric_transformation {
    name          = "TextractCompleted"
    namespace     = local.metric_namespace
    value         = "1"
    default_value = "0"
  }
}

# --- Textract failed (job finished with non-SUCCEEDED status) ---
resource "aws_cloudwatch_log_metric_filter" "textract_failed" {
  name           = "sp-ingest-textract-failed"
  log_group_name = aws_cloudwatch_log_group.textract_complete.name
  pattern        = "\"Textract job\" \"finished with status\""

  metric_transformation {
    name          = "TextractFailed"
    namespace     = local.metric_namespace
    value         = "1"
    default_value = "0"
  }
}

# --- Direct extract completed (trigger Lambda: pptx/xlsx/txt) ---
resource "aws_cloudwatch_log_metric_filter" "direct_extract_completed" {
  name           = "sp-ingest-direct-extract-completed"
  log_group_name = aws_cloudwatch_log_group.textract_trigger.name
  pattern        = "\"extract complete for\""

  metric_transformation {
    name          = "DirectExtractCompleted"
    namespace     = local.metric_namespace
    value         = "1"
    default_value = "0"
  }
}

# --- Sync duration (bulk ingest complete event) ---
resource "aws_cloudwatch_log_metric_filter" "sync_duration" {
  name           = "sp-ingest-sync-duration"
  log_group_name = aws_cloudwatch_log_group.bulk_ingest.name
  pattern        = "{ $.event = \"bulk_ingestion_complete\" }"

  metric_transformation {
    name          = "SyncDurationSeconds"
    namespace     = local.metric_namespace
    value         = "$.duration_seconds"
    default_value = "0"
  }
}

# --- Bulk documents ingested (progress events) ---
resource "aws_cloudwatch_log_metric_filter" "bulk_documents_ingested" {
  name           = "sp-ingest-bulk-documents-ingested"
  log_group_name = aws_cloudwatch_log_group.bulk_ingest.name
  pattern        = "{ $.event = \"library_crawl_complete\" }"

  metric_transformation {
    name          = "BulkDocumentsIngested"
    namespace     = local.metric_namespace
    value         = "$.submitted"
    default_value = "0"
  }
}


# ===================================================================
# CloudWatch Alarms
# ===================================================================

# --- Daily sync Lambda errors > 0 ---
resource "aws_cloudwatch_metric_alarm" "daily_sync_errors" {
  alarm_name          = "sp-ingest-daily-sync-errors"
  alarm_description   = "Daily sync Lambda encountered errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.daily_sync.function_name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# --- Textract complete errors > 3 in 1 hour ---
resource "aws_cloudwatch_metric_alarm" "textract_complete_errors" {
  alarm_name          = "sp-ingest-textract-complete-errors"
  alarm_description   = "Textract complete Lambda has >3 errors in 1 hour"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600
  statistic           = "Sum"
  threshold           = 3
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.textract_complete.function_name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# --- Daily sync did not run (no invocations in 26 hours) ---
resource "aws_cloudwatch_metric_alarm" "daily_sync_missing" {
  alarm_name          = "sp-ingest-daily-sync-missing"
  alarm_description   = "Daily sync Lambda has not been invoked in 26 hours"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Invocations"
  namespace           = "AWS/Lambda"
  period              = 93600 # 26 hours
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "breaching"

  dimensions = {
    FunctionName = aws_lambda_function.daily_sync.function_name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# --- DynamoDB throttled requests (delta tokens) ---
resource "aws_cloudwatch_metric_alarm" "dynamo_throttle_delta" {
  alarm_name          = "sp-ingest-dynamo-throttle-delta-tokens"
  alarm_description   = "DynamoDB throttled requests on delta tokens table"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ThrottledRequests"
  namespace           = "AWS/DynamoDB"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    TableName = aws_dynamodb_table.delta_tokens.name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
}

# --- DynamoDB throttled requests (document registry) ---
resource "aws_cloudwatch_metric_alarm" "dynamo_throttle_registry" {
  alarm_name          = "sp-ingest-dynamo-throttle-registry"
  alarm_description   = "DynamoDB throttled requests on document registry table"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ThrottledRequests"
  namespace           = "AWS/DynamoDB"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    TableName = aws_dynamodb_table.document_registry.name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
}


# ===================================================================
# CloudWatch Dashboard
# ===================================================================

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "SP-Ingest-Pipeline"

  dashboard_body = jsonencode({
    widgets = [
      # ---------------------------------------------------------------
      # Row 1: High-level counters (number widgets)
      # ---------------------------------------------------------------
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 6
        height = 4
        properties = {
          metrics = [
            [local.metric_namespace, "DocumentsCreated", { stat = "Sum", period = 86400, label = "Created" }],
            [local.metric_namespace, "DocumentsDeleted", { stat = "Sum", period = 86400, label = "Deleted" }],
          ]
          view   = "singleValue"
          region = var.aws_region
          title  = "Documents Synced Today"
          period = 86400
        }
      },
      {
        type   = "metric"
        x      = 6
        y      = 0
        width  = 6
        height = 4
        properties = {
          metrics = [
            [local.metric_namespace, "TextractCompleted", { stat = "Sum", period = 86400, label = "Completed" }],
            [local.metric_namespace, "DirectExtractCompleted", { stat = "Sum", period = 86400, label = "Direct Extract" }],
          ]
          view   = "singleValue"
          region = var.aws_region
          title  = "Textract Jobs Today"
          period = 86400
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 6
        height = 4
        properties = {
          metrics = [
            [local.metric_namespace, "TextractFailed", { stat = "Sum", period = 86400, label = "Failed" }],
          ]
          view   = "singleValue"
          region = var.aws_region
          title  = "Textract Failures Today"
          period = 86400
        }
      },
      {
        type   = "metric"
        x      = 18
        y      = 0
        width  = 6
        height = 4
        properties = {
          metrics = [
            ["AWS/S3", "NumberOfObjects", "StorageType", "AllStorageTypes", "BucketName", var.s3_bucket_name, { stat = "Average", period = 86400 }],
          ]
          view   = "singleValue"
          region = var.aws_region
          title  = "S3 Object Count"
          period = 86400
        }
      },

      # ---------------------------------------------------------------
      # Row 2: Lambda invocations (line charts)
      # ---------------------------------------------------------------
      {
        type   = "metric"
        x      = 0
        y      = 4
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", "sp-ingest-daily-sync", { stat = "Sum", label = "Daily Sync" }],
            ["AWS/Lambda", "Invocations", "FunctionName", "sp-ingest-textract-trigger", { stat = "Sum", label = "Textract Trigger" }],
            ["AWS/Lambda", "Invocations", "FunctionName", "sp-ingest-textract-complete", { stat = "Sum", label = "Textract Complete" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
          title  = "Lambda Invocations"
          period = 300
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 4
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["AWS/Lambda", "Errors", "FunctionName", "sp-ingest-daily-sync", { stat = "Sum", label = "Daily Sync" }],
            ["AWS/Lambda", "Errors", "FunctionName", "sp-ingest-textract-trigger", { stat = "Sum", label = "Textract Trigger" }],
            ["AWS/Lambda", "Errors", "FunctionName", "sp-ingest-textract-complete", { stat = "Sum", label = "Textract Complete" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
          title  = "Lambda Errors"
          period = 300
          yAxis  = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 4
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", "sp-ingest-daily-sync", { stat = "Average", label = "Daily Sync" }],
            ["AWS/Lambda", "Duration", "FunctionName", "sp-ingest-textract-trigger", { stat = "Average", label = "Textract Trigger" }],
            ["AWS/Lambda", "Duration", "FunctionName", "sp-ingest-textract-complete", { stat = "Average", label = "Textract Complete" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
          title  = "Lambda Duration (ms)"
          period = 300
        }
      },

      # ---------------------------------------------------------------
      # Row 3: DynamoDB metrics
      # ---------------------------------------------------------------
      {
        type   = "metric"
        x      = 0
        y      = 10
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/DynamoDB", "ConsumedReadCapacityUnits", "TableName", var.registry_table_name, { stat = "Sum", label = "Registry Reads" }],
            ["AWS/DynamoDB", "ConsumedWriteCapacityUnits", "TableName", var.registry_table_name, { stat = "Sum", label = "Registry Writes" }],
            ["AWS/DynamoDB", "ConsumedReadCapacityUnits", "TableName", var.delta_table_name, { stat = "Sum", label = "Delta Reads" }],
            ["AWS/DynamoDB", "ConsumedWriteCapacityUnits", "TableName", var.delta_table_name, { stat = "Sum", label = "Delta Writes" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
          title  = "DynamoDB Consumed Capacity"
          period = 300
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 10
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/DynamoDB", "ThrottledRequests", "TableName", var.registry_table_name, { stat = "Sum", label = "Registry Throttled" }],
            ["AWS/DynamoDB", "ThrottledRequests", "TableName", var.delta_table_name, { stat = "Sum", label = "Delta Throttled" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
          title  = "DynamoDB Throttled Requests"
          period = 300
          yAxis  = { left = { min = 0 } }
        }
      },

      # ---------------------------------------------------------------
      # Row 4: Custom pipeline metrics
      # ---------------------------------------------------------------
      {
        type   = "metric"
        x      = 0
        y      = 16
        width  = 12
        height = 6
        properties = {
          metrics = [
            [local.metric_namespace, "DocumentsCreated", { stat = "Sum", label = "Docs Created" }],
            [local.metric_namespace, "DocumentsDeleted", { stat = "Sum", label = "Docs Deleted" }],
            [local.metric_namespace, "DocumentsSynced", { stat = "Sum", label = "Sync Runs" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
          title  = "Document Sync Activity"
          period = 3600
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 16
        width  = 12
        height = 6
        properties = {
          metrics = [
            [local.metric_namespace, "TextractCompleted", { stat = "Sum", label = "Textract OK" }],
            [local.metric_namespace, "TextractFailed", { stat = "Sum", label = "Textract Failed" }],
            [local.metric_namespace, "DirectExtractCompleted", { stat = "Sum", label = "Direct Extract" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
          title  = "Extraction Activity"
          period = 3600
        }
      },
    ]
  })
}
