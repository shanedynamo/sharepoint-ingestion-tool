# dynamo-sharepoint-ingest

Automated pipeline that ingests documents from a SharePoint site into AWS, extracts content with Textract, and produces structured JSON "digital twins" for downstream AI consumption.

## Architecture

```
SharePoint (Dynamo)
       │
       ▼
 ┌─────────────┐     ┌──────────────┐     ┌──────────────────┐
 │  Graph API   │────▶│  S3 (source/) │────▶│  AWS Textract     │
 │  Delta Sync  │     │              │     │  (async analysis)  │
 └─────────────┘     └──────────────┘     └────────┬───────────┘
       │                                           │
       ▼                                           ▼
 ┌─────────────┐                          ┌──────────────────┐
 │  DynamoDB    │                          │  S3 (extracted/   │
 │  - delta     │                          │   twins/*.json)   │
 │  - registry  │                          └──────────────────┘
 └─────────────┘
```

### Components

| Component | Purpose |
|---|---|
| `graph_client.py` | Authenticates via MSAL and calls Microsoft Graph API for SharePoint operations |
| `s3_client.py` | Uploads/downloads documents and manages S3 object tags |
| `textract_client.py` | Starts async Textract jobs and retrieves results |
| `delta_tracker.py` | Persists Graph API delta tokens in DynamoDB for incremental sync |
| `document_registry.py` | Tracks every document through the ingest/extract/twin lifecycle |
| `digital_twin.py` | Assembles structured JSON from Textract output + SharePoint metadata |
| `utils/file_converter.py` | Routes file types to extraction strategies; extracts text from DOCX/PPTX/XLSX in Lambda |

## Three Pipelines

### 1. Ingestion Pipeline

Moves documents from SharePoint into S3 and registers them in DynamoDB.

- **Bulk ingest** (`bulk_ingest.py`): One-time recursive crawl of the entire SharePoint document library. Run on EC2 or locally for the initial load.
- **Daily sync** (`daily_sync.py`): Lambda triggered by EventBridge on a cron schedule. Uses the Graph delta API to fetch only changed/new/deleted files since the last run. Delta tokens are stored in DynamoDB.

Flow: `SharePoint → Graph API → S3 (source/) → DynamoDB registry`

### 2. Extraction Pipeline

Converts raw documents into structured text using AWS Textract.

- **Textract trigger** (`textract_trigger.py`): Lambda triggered by S3 `ObjectCreated` events on the `source/` prefix. Routes documents by type:
  - **PDF** → async Textract `StartDocumentAnalysis`
  - **DOCX** → direct text extraction via `python-docx`
  - **PPTX** → direct text extraction via `python-pptx`
  - **XLSX** → direct text extraction via `openpyxl`
  - **TXT** → read as UTF-8 plain text
- **Textract complete** (`textract_complete.py`): Lambda triggered by SNS when Textract finishes. Retrieves results and builds a JSON digital twin.

Flow: `S3 event → Textract (PDF) or direct extract (Office) → JSON twin → S3 (extracted/twins/)`

### 3. Coordination Pipeline

DynamoDB tables track state across the system:

- **Delta tokens table**: Stores the latest Graph API delta link so each sync is incremental.
- **Document registry table**: Tracks every document's lifecycle status: `ingested → extracting → twin_ready`. Stores SharePoint metadata, S3 keys, Textract job IDs, and twin locations.

Documents are deduplicated by SharePoint item ID and eTag — unchanged files are skipped on re-sync.

## Prerequisites

- Python 3.11+
- AWS account with Textract, S3, DynamoDB, Lambda, EventBridge, SNS access
- Azure AD app registration with `Sites.Read.All` Graph API permission
- Terraform >= 1.5
- (Optional) LibreOffice for legacy format conversion (.ppt, .xls, .doc) on EC2

## Local Setup

```bash
# Clone and enter project
cd dynamo-sharepoint-ingest

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your Azure and AWS credentials
```

## Running Locally

### Run tests

```bash
python -m pytest tests/ -v
# or
./scripts/test-local.sh
```

### Run bulk ingestion

Requires valid Azure and AWS credentials in `.env`:

```bash
./scripts/run-bulk-ingest.sh
```

### Simulate daily sync

```bash
cd src && python -c "
from daily_sync import handler
result = handler({}, None)
print(result)
"
```

## Deployment

### Infrastructure (Terraform)

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

This creates:
- S3 bucket with versioning and encryption
- Two DynamoDB tables (delta tokens + document registry)
- Three Lambda functions (daily sync, Textract trigger, Textract complete)
- EventBridge rule for daily cron
- SNS topic for Textract notifications
- IAM roles and policies

### Lambda Code

```bash
./scripts/deploy.sh
```

This packages the source code with dependencies into a zip and runs `terraform apply`.

### Docker (bulk ingestion on EC2)

```bash
docker build -f docker/Dockerfile.bulk -t sp-ingest-bulk .
docker run --env-file .env sp-ingest-bulk
```

## S3 Key Structure

```
dynamo-ai-documents/
├── source/                    # Raw SharePoint documents
│   ├── General/Reports/Q4_Report.pdf
│   └── Projects/Alpha/design.pdf
└── extracted/
    └── twins/                 # JSON digital twins
        ├── General/Reports/Q4_Report.json
        └── Projects/Alpha/design.json
```

## Document Lifecycle

```
ingested → extracting → twin_ready
                      ↘ (failed)
```

Each document in the registry tracks:
- SharePoint item ID, path, eTag, content type
- S3 source key and twin key
- Textract job ID
- Status and timestamps

## Configuration

All configuration is via environment variables. See `.env.example` for the full list. Key settings:

| Variable | Description |
|---|---|
| `AZURE_CLIENT_ID/SECRET` | Azure AD app credentials for Graph API |
| `SHAREPOINT_SITE_NAME` | SharePoint site to crawl (default: `Dynamo`) |
| `EXCLUDED_FOLDERS` | Comma-separated folder names to skip |
| `S3_BUCKET` | Target S3 bucket for all documents |
| `DYNAMODB_DELTA_TABLE` | Table name for delta token storage |
| `DYNAMODB_REGISTRY_TABLE` | Table name for document registry |
