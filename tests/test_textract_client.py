"""Tests for TextractClient."""

import sys
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, "src")


# ===================================================================
# Fixtures
# ===================================================================

@pytest.fixture
def mock_boto(monkeypatch):
    """Patch boto3.client so TextractClient doesn't need real AWS creds."""
    mock_client = MagicMock()
    with patch("textract_client.boto3.client", return_value=mock_client) as factory:
        yield mock_client, factory


@pytest.fixture
def client(mock_boto):
    """Return a TextractClient wired to the mocked boto3 client."""
    from textract_client import TextractClient
    return TextractClient(
        region="us-east-1",
        sns_topic_arn="arn:aws:sns:us-east-1:123456:textract-topic",
        sns_role_arn="arn:aws:iam::123456:role/textract-role",
    )


@pytest.fixture
def client_no_sns(mock_boto):
    """TextractClient without SNS configuration."""
    from textract_client import TextractClient
    return TextractClient(
        region="us-east-1",
        sns_topic_arn="",
        sns_role_arn="",
    )


# ===================================================================
# __init__
# ===================================================================

class TestInit:
    def test_creates_boto3_client(self, mock_boto):
        _, factory = mock_boto
        from textract_client import TextractClient
        TextractClient(region="eu-west-1", sns_topic_arn="t", sns_role_arn="r")
        factory.assert_called_with("textract", region_name="eu-west-1")

    def test_stores_sns_config(self, mock_boto):
        from textract_client import TextractClient
        tc = TextractClient(
            region="us-east-1",
            sns_topic_arn="arn:topic",
            sns_role_arn="arn:role",
        )
        assert tc._sns_topic_arn == "arn:topic"
        assert tc._sns_role_arn == "arn:role"


# ===================================================================
# start_document_analysis
# ===================================================================

class TestStartDocumentAnalysis:
    def test_returns_job_id(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.start_document_analysis.return_value = {"JobId": "job-123"}

        job_id = client.start_document_analysis("my-bucket", "source/doc.pdf")

        assert job_id == "job-123"

    def test_passes_correct_params(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.start_document_analysis.return_value = {"JobId": "job-1"}

        client.start_document_analysis("my-bucket", "source/doc.pdf")

        call_kwargs = mock_client.start_document_analysis.call_args[1]
        assert call_kwargs["DocumentLocation"] == {
            "S3Object": {"Bucket": "my-bucket", "Name": "source/doc.pdf"},
        }
        assert call_kwargs["FeatureTypes"] == ["TABLES", "FORMS"]
        assert call_kwargs["OutputConfig"] == {
            "S3Bucket": "my-bucket",
            "S3Prefix": "textract-raw/",
        }

    def test_includes_notification_channel(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.start_document_analysis.return_value = {"JobId": "job-1"}

        client.start_document_analysis("bucket", "key")

        call_kwargs = mock_client.start_document_analysis.call_args[1]
        assert call_kwargs["NotificationChannel"] == {
            "SNSTopicArn": "arn:aws:sns:us-east-1:123456:textract-topic",
            "RoleArn": "arn:aws:iam::123456:role/textract-role",
        }

    def test_omits_notification_when_no_sns(self, client_no_sns, mock_boto):
        mock_client, _ = mock_boto
        mock_client.start_document_analysis.return_value = {"JobId": "job-1"}

        client_no_sns.start_document_analysis("bucket", "key")

        call_kwargs = mock_client.start_document_analysis.call_args[1]
        assert "NotificationChannel" not in call_kwargs


# ===================================================================
# start_text_detection
# ===================================================================

class TestStartTextDetection:
    def test_returns_job_id(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.start_document_text_detection.return_value = {"JobId": "det-456"}

        job_id = client.start_text_detection("my-bucket", "source/doc.docx")

        assert job_id == "det-456"

    def test_passes_correct_params(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.start_document_text_detection.return_value = {"JobId": "det-1"}

        client.start_text_detection("my-bucket", "source/doc.docx")

        call_kwargs = mock_client.start_document_text_detection.call_args[1]
        assert call_kwargs["DocumentLocation"] == {
            "S3Object": {"Bucket": "my-bucket", "Name": "source/doc.docx"},
        }
        assert call_kwargs["OutputConfig"] == {
            "S3Bucket": "my-bucket",
            "S3Prefix": "textract-raw/",
        }
        # No FeatureTypes for text detection
        assert "FeatureTypes" not in call_kwargs

    def test_includes_notification_channel(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.start_document_text_detection.return_value = {"JobId": "det-1"}

        client.start_text_detection("bucket", "key")

        call_kwargs = mock_client.start_document_text_detection.call_args[1]
        assert "NotificationChannel" in call_kwargs

    def test_omits_notification_when_no_sns(self, client_no_sns, mock_boto):
        mock_client, _ = mock_boto
        mock_client.start_document_text_detection.return_value = {"JobId": "det-1"}

        client_no_sns.start_text_detection("bucket", "key")

        call_kwargs = mock_client.start_document_text_detection.call_args[1]
        assert "NotificationChannel" not in call_kwargs


# ===================================================================
# get_document_analysis (paginated)
# ===================================================================

class TestGetDocumentAnalysis:
    def test_single_page_response(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.get_document_analysis.return_value = {
            "JobStatus": "SUCCEEDED",
            "JobId": "job-1",
            "Blocks": [
                {"Id": "b1", "BlockType": "LINE", "Text": "Hello"},
                {"Id": "b2", "BlockType": "LINE", "Text": "World"},
            ],
        }

        result = client.get_document_analysis("job-1")

        assert result["JobStatus"] == "SUCCEEDED"
        assert len(result["Blocks"]) == 2
        assert result["Blocks"][0]["Text"] == "Hello"
        assert "NextToken" not in result

    def test_multi_page_pagination(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.get_document_analysis.side_effect = [
            {
                "JobStatus": "SUCCEEDED",
                "JobId": "job-1",
                "Blocks": [{"Id": "b1", "BlockType": "LINE", "Text": "Page 1"}],
                "NextToken": "token-2",
            },
            {
                "JobStatus": "SUCCEEDED",
                "Blocks": [{"Id": "b2", "BlockType": "LINE", "Text": "Page 2"}],
                "NextToken": "token-3",
            },
            {
                "JobStatus": "SUCCEEDED",
                "Blocks": [{"Id": "b3", "BlockType": "LINE", "Text": "Page 3"}],
            },
        ]

        result = client.get_document_analysis("job-1")

        assert len(result["Blocks"]) == 3
        texts = [b["Text"] for b in result["Blocks"]]
        assert texts == ["Page 1", "Page 2", "Page 3"]
        assert "NextToken" not in result

    def test_passes_next_token(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.get_document_analysis.side_effect = [
            {
                "JobStatus": "SUCCEEDED",
                "Blocks": [{"Id": "b1"}],
                "NextToken": "tok-abc",
            },
            {
                "JobStatus": "SUCCEEDED",
                "Blocks": [{"Id": "b2"}],
            },
        ]

        client.get_document_analysis("job-1")

        calls = mock_client.get_document_analysis.call_args_list
        assert calls[0] == call(JobId="job-1")
        assert calls[1] == call(JobId="job-1", NextToken="tok-abc")

    def test_preserves_top_level_metadata(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.get_document_analysis.return_value = {
            "JobStatus": "SUCCEEDED",
            "JobId": "job-1",
            "DocumentMetadata": {"Pages": 5},
            "Blocks": [],
        }

        result = client.get_document_analysis("job-1")

        assert result["DocumentMetadata"] == {"Pages": 5}
        assert result["JobId"] == "job-1"

    def test_empty_blocks(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.get_document_analysis.return_value = {
            "JobStatus": "SUCCEEDED",
            "Blocks": [],
        }

        result = client.get_document_analysis("job-1")
        assert result["Blocks"] == []


# ===================================================================
# get_text_detection (paginated)
# ===================================================================

class TestGetTextDetection:
    def test_single_page_response(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.get_document_text_detection.return_value = {
            "JobStatus": "SUCCEEDED",
            "Blocks": [
                {"Id": "b1", "BlockType": "LINE", "Text": "Detected text"},
            ],
        }

        result = client.get_text_detection("det-1")

        assert len(result["Blocks"]) == 1
        assert result["Blocks"][0]["Text"] == "Detected text"

    def test_multi_page_pagination(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.get_document_text_detection.side_effect = [
            {
                "JobStatus": "SUCCEEDED",
                "Blocks": [{"Id": "b1", "Text": "A"}],
                "NextToken": "tok-2",
            },
            {
                "JobStatus": "SUCCEEDED",
                "Blocks": [{"Id": "b2", "Text": "B"}],
            },
        ]

        result = client.get_text_detection("det-1")

        assert len(result["Blocks"]) == 2
        assert "NextToken" not in result


# ===================================================================
# wait_for_completion
# ===================================================================

class TestWaitForCompletion:
    def test_returns_succeeded(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.get_document_analysis.return_value = {
            "JobStatus": "SUCCEEDED",
        }

        status = client.wait_for_completion("job-1")
        assert status == "SUCCEEDED"

    def test_returns_failed(self, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.get_document_analysis.return_value = {
            "JobStatus": "FAILED",
        }

        status = client.wait_for_completion("job-1")
        assert status == "FAILED"

    @patch("textract_client.time.sleep")
    def test_polls_until_complete(self, mock_sleep, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.get_document_analysis.side_effect = [
            {"JobStatus": "IN_PROGRESS"},
            {"JobStatus": "IN_PROGRESS"},
            {"JobStatus": "SUCCEEDED"},
        ]

        status = client.wait_for_completion("job-1", poll_interval=1)

        assert status == "SUCCEEDED"
        assert mock_client.get_document_analysis.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("textract_client.time.sleep")
    def test_raises_on_timeout(self, mock_sleep, client, mock_boto):
        mock_client, _ = mock_boto
        mock_client.get_document_analysis.return_value = {
            "JobStatus": "IN_PROGRESS",
        }

        with pytest.raises(TimeoutError, match="did not complete"):
            client.wait_for_completion("job-1", poll_interval=1, max_wait=3)


# ===================================================================
# _get_paginated_results (static)
# ===================================================================

class TestGetPaginatedResults:
    def test_merges_blocks_from_multiple_pages(self):
        from textract_client import TextractClient

        call_count = 0

        def fake_api(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "JobStatus": "SUCCEEDED",
                    "Blocks": [{"Id": "1"}, {"Id": "2"}],
                    "NextToken": "next",
                }
            return {
                "JobStatus": "SUCCEEDED",
                "Blocks": [{"Id": "3"}],
            }

        result = TextractClient._get_paginated_results(fake_api, "job-x")

        assert len(result["Blocks"]) == 3
        assert [b["Id"] for b in result["Blocks"]] == ["1", "2", "3"]

    def test_first_page_metadata_preserved(self):
        from textract_client import TextractClient

        call_count = 0

        def fake_api(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "JobStatus": "SUCCEEDED",
                    "DocumentMetadata": {"Pages": 10},
                    "AnalyzeDocumentModelVersion": "1.0",
                    "Blocks": [{"Id": "1"}],
                    "NextToken": "next",
                }
            return {
                "JobStatus": "SUCCEEDED",
                "DocumentMetadata": {"Pages": 10},
                "Blocks": [{"Id": "2"}],
            }

        result = TextractClient._get_paginated_results(fake_api, "job-x")

        assert result["DocumentMetadata"] == {"Pages": 10}
        assert result["AnalyzeDocumentModelVersion"] == "1.0"
        assert "NextToken" not in result
