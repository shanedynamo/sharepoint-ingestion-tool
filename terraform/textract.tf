# Textract itself is a managed service with no Terraform resources to create.
# This file documents the Textract integration points:
#
# 1. Textract reads source documents from s3.tf -> aws_s3_bucket.documents
# 2. Textract publishes completion notifications to sns.tf -> aws_sns_topic.textract_notifications
# 3. Textract assumes iam.tf -> aws_iam_role.textract_service to publish to SNS
# 4. Lambda (textract_trigger) starts Textract jobs via the API
# 5. Lambda (textract_complete) processes results when SNS notifies completion
#
# Textract API permissions are granted in iam.tf per-function role policies
