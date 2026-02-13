# ===================================================================
# Role 1: EC2 — bulk ingestion (one-time full crawl)
# ===================================================================

resource "aws_iam_role" "bulk_ec2" {
  name = "sp-ingest-bulk-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_instance_profile" "bulk_ec2" {
  name = "sp-ingest-bulk-instance-profile"
  role = aws_iam_role.bulk_ec2.name
}

resource "aws_iam_role_policy" "bulk_ec2" {
  name = "sp-ingest-bulk-ec2-policy"
  role = aws_iam_role.bulk_ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectTagging",
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.documents.arn,
          "${aws_s3_bucket.documents.arn}/*",
        ]
      },
      {
        Sid    = "DynamoDBAccess"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan",
        ]
        Resource = [
          aws_dynamodb_table.delta_tokens.arn,
          aws_dynamodb_table.document_registry.arn,
          "${aws_dynamodb_table.document_registry.arn}/index/*",
        ]
      },
      {
        Sid    = "SecretsManagerRead"
        Effect = "Allow"
        Action = "secretsmanager:GetSecretValue"
        Resource = [
          aws_secretsmanager_secret.azure_client_id.arn,
          aws_secretsmanager_secret.azure_tenant_id.arn,
          aws_secretsmanager_secret.azure_client_secret.arn,
        ]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:*"
      },
    ]
  })
}


# ===================================================================
# Role 2: Lambda — daily sync (incremental delta crawl)
# ===================================================================

resource "aws_iam_role" "daily_sync_lambda" {
  name = "sp-ingest-daily-sync-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "daily_sync_lambda_basic" {
  role       = aws_iam_role.daily_sync_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "daily_sync_lambda" {
  name = "sp-ingest-daily-sync-lambda-policy"
  role = aws_iam_role.daily_sync_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:HeadBucket",
          "s3:HeadObject",
          "s3:PutObjectTagging",
        ]
        Resource = [
          aws_s3_bucket.documents.arn,
          "${aws_s3_bucket.documents.arn}/*",
        ]
      },
      {
        Sid    = "DynamoDBAccess"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan",
        ]
        Resource = [
          aws_dynamodb_table.delta_tokens.arn,
          aws_dynamodb_table.document_registry.arn,
          "${aws_dynamodb_table.document_registry.arn}/index/*",
        ]
      },
      {
        Sid    = "SecretsManagerRead"
        Effect = "Allow"
        Action = "secretsmanager:GetSecretValue"
        Resource = [
          aws_secretsmanager_secret.azure_client_id.arn,
          aws_secretsmanager_secret.azure_tenant_id.arn,
          aws_secretsmanager_secret.azure_client_secret.arn,
        ]
      },
    ]
  })
}


# ===================================================================
# Role 3: Lambda — textract trigger (S3 event → start extraction)
# ===================================================================

resource "aws_iam_role" "textract_trigger_lambda" {
  name = "sp-ingest-textract-trigger-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "textract_trigger_lambda_basic" {
  role       = aws_iam_role.textract_trigger_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "textract_trigger_lambda" {
  name = "sp-ingest-textract-trigger-lambda-policy"
  role = aws_iam_role.textract_trigger_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3FullAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:HeadObject",
          "s3:PutObject",
          "s3:PutObjectTagging",
          "s3:HeadBucket",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.documents.arn,
          "${aws_s3_bucket.documents.arn}/*",
        ]
      },
      {
        Sid    = "DynamoDBRegistry"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
        ]
        Resource = aws_dynamodb_table.document_registry.arn
      },
      {
        Sid    = "TextractStartJobs"
        Effect = "Allow"
        Action = [
          "textract:StartDocumentAnalysis",
          "textract:StartDocumentTextDetection",
        ]
        Resource = "*"
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = "sns:Publish"
        Resource = aws_sns_topic.textract_notifications.arn
      },
      {
        Sid    = "PassTextractServiceRole"
        Effect = "Allow"
        Action = "iam:PassRole"
        Resource = aws_iam_role.textract_service.arn
      },
    ]
  })
}


# ===================================================================
# Role 4: Lambda — textract complete (SNS → build JSON twin)
# ===================================================================

resource "aws_iam_role" "textract_complete_lambda" {
  name = "sp-ingest-textract-complete-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "textract_complete_lambda_basic" {
  role       = aws_iam_role.textract_complete_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "textract_complete_lambda" {
  name = "sp-ingest-textract-complete-lambda-policy"
  role = aws_iam_role.textract_complete_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "TextractGetResults"
        Effect = "Allow"
        Action = [
          "textract:GetDocumentAnalysis",
          "textract:GetDocumentTextDetection",
        ]
        Resource = "*"
      },
      {
        Sid    = "S3FullAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectTagging",
          "s3:HeadObject",
          "s3:PutObject",
          "s3:PutObjectTagging",
          "s3:HeadBucket",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.documents.arn,
          "${aws_s3_bucket.documents.arn}/*",
        ]
      },
      {
        Sid    = "DynamoDBRegistry"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
        ]
        Resource = aws_dynamodb_table.document_registry.arn
      },
    ]
  })
}


# ===================================================================
# Role 5: Textract service — publishes job completion to SNS
# ===================================================================

resource "aws_iam_role" "textract_service" {
  name = "sp-ingest-textract-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "textract.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "textract_service" {
  name = "sp-ingest-textract-service-policy"
  role = aws_iam_role.textract_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "SNSPublish"
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.textract_notifications.arn
      },
      {
        Sid    = "S3ReadSource"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
        ]
        Resource = "${aws_s3_bucket.documents.arn}/source/*"
      },
      {
        Sid    = "S3WriteOutput"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
        ]
        Resource = "${aws_s3_bucket.documents.arn}/textract-raw/*"
      },
    ]
  })
}
