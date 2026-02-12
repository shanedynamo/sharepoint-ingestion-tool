# ---------------------------------------------------------------------------
# Secrets Manager — Azure AD credentials for Microsoft Graph API access.
# Placeholder values are replaced by the deploy script.
# ---------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "azure_client_id" {
  name        = "sp-ingest/azure-client-id"
  description = "Azure AD application (client) ID for SharePoint Graph API"
}

resource "aws_secretsmanager_secret_version" "azure_client_id" {
  secret_id     = aws_secretsmanager_secret.azure_client_id.id
  secret_string = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [secret_string]
  }
}

resource "aws_secretsmanager_secret" "azure_tenant_id" {
  name        = "sp-ingest/azure-tenant-id"
  description = "Azure AD tenant ID"
}

resource "aws_secretsmanager_secret_version" "azure_tenant_id" {
  secret_id     = aws_secretsmanager_secret.azure_tenant_id.id
  secret_string = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [secret_string]
  }
}

resource "aws_secretsmanager_secret" "azure_client_secret" {
  name        = "sp-ingest/azure-client-secret"
  description = "Azure AD client secret for SharePoint Graph API"
}

resource "aws_secretsmanager_secret_version" "azure_client_secret" {
  secret_id     = aws_secretsmanager_secret.azure_client_secret.id
  secret_string = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [secret_string]
  }
}
