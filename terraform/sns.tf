resource "aws_sns_topic" "textract_notifications" {
  name = "sp-ingest-textract-notifications"
}

resource "aws_sns_topic_subscription" "textract_complete" {
  topic_arn = aws_sns_topic.textract_notifications.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.textract_complete.arn
}
