# ===================================================================
# EventBridge — daily sync schedule (7 AM UTC = 2 AM EST)
# ===================================================================

resource "aws_cloudwatch_event_rule" "daily_sync" {
  name                = "sp-ingest-daily-sync-schedule"
  description         = "Trigger daily SharePoint delta sync at 7 AM UTC"
  schedule_expression = "cron(0 7 * * ? *)"
}

resource "aws_cloudwatch_event_target" "daily_sync" {
  rule      = aws_cloudwatch_event_rule.daily_sync.name
  target_id = "daily-sync-lambda"
  arn       = aws_lambda_function.daily_sync.arn
  input     = "{}"

  retry_policy {
    maximum_retry_attempts       = 2
    maximum_event_age_in_seconds = 3600
  }
}

resource "aws_lambda_permission" "eventbridge_invoke_daily_sync" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.daily_sync.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_sync.arn
}
