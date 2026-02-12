"""Tests for DocumentChunker."""

import hashlib
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from chunker import DocumentChunker


# ===================================================================
# Fixtures: sample twin JSON documents
# ===================================================================

def _make_twin(
    *,
    text: str = "Hello world",
    pages: list[dict] | None = None,
    tables: list[dict] | None = None,
    s3_key: str = "source/Dynamo/HR/handbook.pdf",
    file_type: str = ".pdf",
    sp_library: str = "HR",
    sp_path: str = "/HR/handbook.pdf",
) -> dict:
    """Build a minimal twin JSON for testing."""
    document_id = hashlib.sha256(s3_key.encode()).hexdigest()

    if pages is None and text:
        pages = [{"page_number": 1, "text": text}]
    elif pages is None:
        pages = []

    return {
        "schema_version": "2.0",
        "document_id": document_id,
        "source_s3_key": s3_key,
        "source_sharepoint_url": "",
        "filename": os.path.basename(sp_path),
        "file_type": file_type,
        "content_type": "application/pdf",
        "metadata": {
            "sp_library": sp_library,
            "sp_path": sp_path,
            "sp_item_id": "sp-001",
            "sp_last_modified": "2025-06-01T10:00:00Z",
            "size_bytes": 50000,
        },
        "extracted_text": text,
        "pages": pages,
        "tables": tables or [],
        "extraction_metadata": {
            "method": "textract-document-analysis",
            "job_id": "job-123",
            "confidence": 98.5,
            "timestamp": "2025-06-01T10:05:00Z",
            "block_count": 42,
        },
    }


def _words(n: int) -> str:
    """Generate a string of *n* unique words."""
    return " ".join(f"word{i}" for i in range(n))


def _paragraphs(n_paragraphs: int, words_per_para: int) -> str:
    """Generate text with *n_paragraphs* paragraphs of *words_per_para* words."""
    paras = []
    word_idx = 0
    for _ in range(n_paragraphs):
        para = " ".join(f"word{word_idx + j}" for j in range(words_per_para))
        paras.append(para)
        word_idx += words_per_para
    return "\n\n".join(paras)


# ===================================================================
# Constructor
# ===================================================================

class TestConstructor:
    def test_default_values(self):
        c = DocumentChunker()
        assert c.chunk_size == 512
        assert c.chunk_overlap == 50

    def test_custom_values(self):
        c = DocumentChunker(chunk_size=256, chunk_overlap=25)
        assert c.chunk_size == 256
        assert c.chunk_overlap == 25

    def test_overlap_must_be_less_than_size(self):
        with pytest.raises(ValueError, match="chunk_overlap must be less than chunk_size"):
            DocumentChunker(chunk_size=100, chunk_overlap=100)

    def test_overlap_greater_than_size_raises(self):
        with pytest.raises(ValueError):
            DocumentChunker(chunk_size=100, chunk_overlap=150)


# ===================================================================
# chunk_document — basic structure
# ===================================================================

class TestChunkDocumentStructure:
    def test_returns_list(self):
        chunker = DocumentChunker()
        twin = _make_twin(text="Short text.")
        result = chunker.chunk_document(twin)
        assert isinstance(result, list)

    def test_single_chunk_for_short_text(self):
        chunker = DocumentChunker()
        twin = _make_twin(text="A short document with just a few words.")
        chunks = chunker.chunk_document(twin)
        assert len(chunks) == 1

    def test_chunk_has_required_keys(self):
        chunker = DocumentChunker()
        twin = _make_twin(text="Test document text.")
        chunks = chunker.chunk_document(twin)

        expected_keys = {
            "chunk_id", "document_id", "source_s3_key", "filename",
            "chunk_index", "total_chunks", "text", "metadata",
        }
        assert set(chunks[0].keys()) == expected_keys

    def test_chunk_id_format(self):
        chunker = DocumentChunker()
        twin = _make_twin()
        chunks = chunker.chunk_document(twin)

        doc_id = twin["document_id"]
        assert chunks[0]["chunk_id"] == f"{doc_id}_0"

    def test_document_id_preserved(self):
        chunker = DocumentChunker()
        twin = _make_twin()
        chunks = chunker.chunk_document(twin)
        assert chunks[0]["document_id"] == twin["document_id"]

    def test_source_s3_key_preserved(self):
        chunker = DocumentChunker()
        twin = _make_twin()
        chunks = chunker.chunk_document(twin)
        assert chunks[0]["source_s3_key"] == "source/Dynamo/HR/handbook.pdf"

    def test_filename_preserved(self):
        chunker = DocumentChunker()
        twin = _make_twin()
        chunks = chunker.chunk_document(twin)
        assert chunks[0]["filename"] == "handbook.pdf"

    def test_total_chunks_is_correct(self):
        chunker = DocumentChunker(chunk_size=50, chunk_overlap=5)
        # Create enough text for multiple chunks (50 tokens ≈ 37 words)
        twin = _make_twin(text=_words(200))
        chunks = chunker.chunk_document(twin)
        assert len(chunks) > 1
        for chunk in chunks:
            assert chunk["total_chunks"] == len(chunks)

    def test_chunk_index_sequential(self):
        chunker = DocumentChunker(chunk_size=50, chunk_overlap=5)
        twin = _make_twin(text=_words(200))
        chunks = chunker.chunk_document(twin)
        for i, chunk in enumerate(chunks):
            assert chunk["chunk_index"] == i


# ===================================================================
# chunk_document — metadata
# ===================================================================

class TestChunkMetadata:
    def test_metadata_has_required_keys(self):
        chunker = DocumentChunker()
        twin = _make_twin()
        chunks = chunker.chunk_document(twin)

        expected_keys = {
            "sp_site", "sp_library", "sp_path", "access_tags",
            "author", "last_modified", "file_type", "page_numbers",
        }
        assert set(chunks[0]["metadata"].keys()) == expected_keys

    def test_sp_library_from_twin(self):
        chunker = DocumentChunker()
        twin = _make_twin(sp_library="Legal")
        chunks = chunker.chunk_document(twin)
        assert chunks[0]["metadata"]["sp_library"] == "Legal"

    def test_sp_path_from_twin(self):
        chunker = DocumentChunker()
        twin = _make_twin(sp_path="/Legal/nda.pdf")
        chunks = chunker.chunk_document(twin)
        assert chunks[0]["metadata"]["sp_path"] == "/Legal/nda.pdf"

    def test_file_type_from_twin(self):
        chunker = DocumentChunker()
        twin = _make_twin(file_type=".docx")
        chunks = chunker.chunk_document(twin)
        assert chunks[0]["metadata"]["file_type"] == ".docx"

    def test_last_modified_from_twin(self):
        chunker = DocumentChunker()
        twin = _make_twin()
        chunks = chunker.chunk_document(twin)
        assert chunks[0]["metadata"]["last_modified"] == "2025-06-01T10:00:00Z"


# ===================================================================
# chunk_document — text splitting
# ===================================================================

class TestTextSplitting:
    def test_short_text_not_split(self):
        chunker = DocumentChunker(chunk_size=512, chunk_overlap=50)
        twin = _make_twin(text="Just a few words here.")
        chunks = chunker.chunk_document(twin)
        assert len(chunks) == 1
        assert chunks[0]["text"] == "Just a few words here."

    def test_long_text_produces_multiple_chunks(self):
        chunker = DocumentChunker(chunk_size=50, chunk_overlap=5)
        # 50 tokens ≈ 37 words target, so 200 words should produce multiple chunks
        twin = _make_twin(text=_words(200))
        chunks = chunker.chunk_document(twin)
        assert len(chunks) > 1

    def test_chunks_have_overlap(self):
        chunker = DocumentChunker(chunk_size=50, chunk_overlap=10)
        # Target: ~37 words, overlap: ~7 words
        twin = _make_twin(text=_words(200))
        chunks = chunker.chunk_document(twin)

        assert len(chunks) >= 2
        # Check that the end of chunk N appears at the start of chunk N+1
        words_0 = chunks[0]["text"].split()
        words_1 = chunks[1]["text"].split()
        overlap_words = int(10 * 0.75)

        # The last overlap_words of chunk 0 should appear at the start of chunk 1
        tail_0 = words_0[-overlap_words:]
        head_1 = words_1[:overlap_words]
        assert tail_0 == head_1

    def test_prefers_paragraph_breaks(self):
        chunker = DocumentChunker(chunk_size=50, chunk_overlap=5)
        # Two paragraphs, each ~30 words (under target of 37)
        text = _words(30) + "\n\n" + _words(30)
        twin = _make_twin(text=text)
        chunks = chunker.chunk_document(twin)

        # Should produce 2 chunks (one per paragraph), not split mid-paragraph
        assert len(chunks) == 2

    def test_falls_back_to_sentence_breaks(self):
        chunker = DocumentChunker(chunk_size=30, chunk_overlap=3)
        # One big paragraph with sentences (target: ~22 words)
        sentences = [
            "The quick brown fox jumps over the lazy dog.",
            "A second sentence with enough words to fill space.",
            "And a third sentence that adds more content here.",
            "Finally a fourth sentence to push past the limit.",
        ]
        text = " ".join(sentences)
        twin = _make_twin(text=text)
        chunks = chunker.chunk_document(twin)

        assert len(chunks) >= 2
        # Each chunk should end on or near a sentence boundary
        for chunk in chunks:
            assert len(chunk["text"]) > 0

    def test_empty_text_produces_no_chunks(self):
        chunker = DocumentChunker()
        twin = _make_twin(text="", pages=[])
        chunks = chunker.chunk_document(twin)
        # No text chunks, no table chunks
        assert len(chunks) == 0

    def test_whitespace_only_produces_no_chunks(self):
        chunker = DocumentChunker()
        twin = _make_twin(text="   \n\n   ", pages=[{"page_number": 1, "text": "   \n\n   "}])
        chunks = chunker.chunk_document(twin)
        assert len(chunks) == 0


# ===================================================================
# chunk_document — page tracking
# ===================================================================

class TestPageTracking:
    def test_single_page_tracked(self):
        chunker = DocumentChunker()
        twin = _make_twin(
            text="Short text on one page.",
            pages=[{"page_number": 1, "text": "Short text on one page."}],
        )
        chunks = chunker.chunk_document(twin)
        assert chunks[0]["metadata"]["page_numbers"] == [1]

    def test_multi_page_tracked(self):
        chunker = DocumentChunker()
        twin = _make_twin(
            text="Page one text.\n\nPage two text.",
            pages=[
                {"page_number": 1, "text": "Page one text."},
                {"page_number": 2, "text": "Page two text."},
            ],
        )
        chunks = chunker.chunk_document(twin)
        # Short enough for one chunk spanning both pages
        assert 1 in chunks[0]["metadata"]["page_numbers"]
        assert 2 in chunks[0]["metadata"]["page_numbers"]

    def test_chunk_spanning_pages(self):
        chunker = DocumentChunker(chunk_size=60, chunk_overlap=5)
        # Two pages, each with enough text to need chunking together
        pages = [
            {"page_number": 1, "text": _words(30)},
            {"page_number": 2, "text": _words(30)},
            {"page_number": 3, "text": _words(30)},
        ]
        text = "\n\n".join(p["text"] for p in pages)
        twin = _make_twin(text=text, pages=pages)
        chunks = chunker.chunk_document(twin)

        # At least one chunk should span pages
        all_page_nums = set()
        for chunk in chunks:
            for pn in chunk["metadata"]["page_numbers"]:
                all_page_nums.add(pn)
        assert all_page_nums == {1, 2, 3}


# ===================================================================
# chunk_document — tables
# ===================================================================

class TestTableChunking:
    def test_table_becomes_separate_chunk(self):
        chunker = DocumentChunker()
        twin = _make_twin(
            text="Some document text.",
            tables=[{"table_index": 1, "rows": [["A", "B"], ["C", "D"]]}],
        )
        chunks = chunker.chunk_document(twin)
        # 1 text chunk + 1 table chunk
        assert len(chunks) == 2

    def test_table_chunk_text_is_json(self):
        chunker = DocumentChunker()
        rows = [["Name", "Value"], ["Alice", "100"]]
        twin = _make_twin(
            text="Text.",
            tables=[{"table_index": 1, "rows": rows}],
        )
        chunks = chunker.chunk_document(twin)

        table_chunk = chunks[-1]
        parsed = json.loads(table_chunk["text"])
        assert parsed == rows

    def test_multiple_tables_each_get_chunk(self):
        chunker = DocumentChunker()
        twin = _make_twin(
            text="Text.",
            tables=[
                {"table_index": 1, "rows": [["A"]]},
                {"table_index": 2, "rows": [["B"]]},
            ],
        )
        chunks = chunker.chunk_document(twin)
        # 1 text + 2 tables
        assert len(chunks) == 3

    def test_empty_table_rows_skipped(self):
        chunker = DocumentChunker()
        twin = _make_twin(
            text="Text.",
            tables=[{"table_index": 1, "rows": []}],
        )
        chunks = chunker.chunk_document(twin)
        # Only the text chunk, no table chunk
        assert len(chunks) == 1

    def test_table_chunk_index_follows_text(self):
        chunker = DocumentChunker()
        twin = _make_twin(
            text="Some text.",
            tables=[{"table_index": 1, "rows": [["X"]]}],
        )
        chunks = chunker.chunk_document(twin)
        assert chunks[0]["chunk_index"] == 0  # text
        assert chunks[1]["chunk_index"] == 1  # table

    def test_table_chunk_has_metadata(self):
        chunker = DocumentChunker()
        twin = _make_twin(
            text="Text.",
            sp_library="Finance",
            tables=[{"table_index": 1, "rows": [["X"]]}],
        )
        chunks = chunker.chunk_document(twin)
        table_chunk = chunks[-1]
        assert table_chunk["metadata"]["sp_library"] == "Finance"

    def test_only_tables_no_text(self):
        chunker = DocumentChunker()
        twin = _make_twin(
            text="",
            pages=[],
            tables=[{"table_index": 1, "rows": [["A", "B"]]}],
        )
        chunks = chunker.chunk_document(twin)
        assert len(chunks) == 1
        assert json.loads(chunks[0]["text"]) == [["A", "B"]]


# ===================================================================
# chunk_document — edge cases
# ===================================================================

class TestEdgeCases:
    def test_twin_with_no_pages_uses_extracted_text(self):
        chunker = DocumentChunker()
        twin = _make_twin(text="Fallback text.", pages=[])
        chunks = chunker.chunk_document(twin)
        assert len(chunks) == 1
        assert chunks[0]["text"] == "Fallback text."

    def test_missing_metadata_keys_default_to_empty(self):
        chunker = DocumentChunker()
        twin = _make_twin()
        # Remove optional metadata keys
        twin["metadata"] = {"sp_library": "HR"}
        chunks = chunker.chunk_document(twin)
        meta = chunks[0]["metadata"]
        assert meta["sp_site"] == ""
        assert meta["author"] == ""
        assert meta["access_tags"] == []

    def test_very_long_single_word(self):
        """A single extremely long word should not cause infinite loop."""
        chunker = DocumentChunker(chunk_size=10, chunk_overlap=2)
        long_word = "x" * 10000
        twin = _make_twin(text=long_word)
        chunks = chunker.chunk_document(twin)
        assert len(chunks) >= 1
        # All content should be preserved
        reconstructed = " ".join(c["text"] for c in chunks)
        assert long_word in reconstructed

    def test_total_chunks_with_tables(self):
        """total_chunks should count both text and table chunks."""
        chunker = DocumentChunker()
        twin = _make_twin(
            text="Some text.",
            tables=[
                {"table_index": 1, "rows": [["A"]]},
                {"table_index": 2, "rows": [["B"]]},
            ],
        )
        chunks = chunker.chunk_document(twin)
        assert all(c["total_chunks"] == 3 for c in chunks)


# ===================================================================
# chunk_all_documents
# ===================================================================

class TestChunkAllDocuments:
    def test_yields_chunks_from_s3(self):
        chunker = DocumentChunker()
        twin = _make_twin(text="Document content.")

        mock_s3 = MagicMock()
        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "extracted/Dynamo/HR/handbook.json"}]},
        ]
        body = MagicMock()
        body.read.return_value = json.dumps(twin).encode()
        mock_s3.get_object.return_value = {"Body": body}

        chunks = list(chunker.chunk_all_documents(mock_s3, "my-bucket", "extracted/"))
        assert len(chunks) >= 1
        assert chunks[0]["document_id"] == twin["document_id"]

    def test_skips_non_json_files(self):
        chunker = DocumentChunker()

        mock_s3 = MagicMock()
        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "extracted/readme.txt"},
                {"Key": "extracted/data.csv"},
            ]},
        ]

        chunks = list(chunker.chunk_all_documents(mock_s3, "bucket", "extracted/"))
        assert len(chunks) == 0
        mock_s3.get_object.assert_not_called()

    def test_handles_s3_errors_gracefully(self):
        chunker = DocumentChunker()

        mock_s3 = MagicMock()
        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "extracted/broken.json"}]},
        ]
        mock_s3.get_object.side_effect = Exception("S3 error")

        # Should not raise; just logs the error
        chunks = list(chunker.chunk_all_documents(mock_s3, "bucket", "extracted/"))
        assert len(chunks) == 0

    def test_paginates_multiple_pages(self):
        chunker = DocumentChunker()
        twin = _make_twin(text="Content.")

        mock_s3 = MagicMock()
        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "extracted/doc1.json"}]},
            {"Contents": [{"Key": "extracted/doc2.json"}]},
        ]
        body = MagicMock()
        body.read.return_value = json.dumps(twin).encode()
        mock_s3.get_object.return_value = {"Body": body}

        chunks = list(chunker.chunk_all_documents(mock_s3, "bucket", "extracted/"))
        assert mock_s3.get_object.call_count == 2

    def test_empty_s3_prefix(self):
        chunker = DocumentChunker()

        mock_s3 = MagicMock()
        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{"Contents": []}]

        chunks = list(chunker.chunk_all_documents(mock_s3, "bucket", "extracted/"))
        assert len(chunks) == 0


# ===================================================================
# export_chunks_to_jsonl — local
# ===================================================================

class TestExportJsonlLocal:
    def test_writes_jsonl_file(self, tmp_path):
        chunker = DocumentChunker()
        twin = _make_twin(text="Test content.")
        chunks = chunker.chunk_document(twin)

        output = str(tmp_path / "output.jsonl")
        count = DocumentChunker.export_chunks_to_jsonl(chunks, output)

        assert count == len(chunks)
        assert os.path.exists(output)

        with open(output) as f:
            lines = f.readlines()
        assert len(lines) == len(chunks)

        # Each line should be valid JSON
        for line in lines:
            parsed = json.loads(line)
            assert "chunk_id" in parsed
            assert "text" in parsed

    def test_jsonl_format_one_per_line(self, tmp_path):
        chunker = DocumentChunker(chunk_size=30, chunk_overlap=3)
        twin = _make_twin(text=_words(100))
        chunks = chunker.chunk_document(twin)
        assert len(chunks) > 1

        output = str(tmp_path / "multi.jsonl")
        count = DocumentChunker.export_chunks_to_jsonl(chunks, output)

        with open(output) as f:
            lines = f.readlines()
        assert len(lines) == count

    def test_empty_chunks_writes_empty_file(self, tmp_path):
        output = str(tmp_path / "empty.jsonl")
        count = DocumentChunker.export_chunks_to_jsonl([], output)
        assert count == 0
        assert os.path.getsize(output) == 0

    def test_generator_input(self, tmp_path):
        chunker = DocumentChunker()
        twin = _make_twin(text="Content.")
        chunks = chunker.chunk_document(twin)

        def chunk_gen():
            yield from chunks

        output = str(tmp_path / "gen.jsonl")
        count = DocumentChunker.export_chunks_to_jsonl(chunk_gen(), output)
        assert count == len(chunks)


# ===================================================================
# export_chunks_to_jsonl — S3
# ===================================================================

class TestExportJsonlS3:
    def test_writes_to_s3(self):
        chunks = [{"chunk_id": "abc_0", "text": "hello"}]
        mock_s3 = MagicMock()

        count = DocumentChunker.export_chunks_to_jsonl(
            chunks, "s3://my-bucket/chunks/output.jsonl", s3_client=mock_s3,
        )

        assert count == 1
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "my-bucket"
        assert call_kwargs["Key"] == "chunks/output.jsonl"

        # Verify the body is valid JSONL
        body = call_kwargs["Body"].decode()
        parsed = json.loads(body.strip())
        assert parsed["chunk_id"] == "abc_0"

    def test_s3_requires_client(self):
        with pytest.raises(ValueError, match="s3_client is required"):
            DocumentChunker.export_chunks_to_jsonl(
                [], "s3://bucket/key.jsonl", s3_client=None,
            )


# ===================================================================
# Integration: realistic multi-page document
# ===================================================================

class TestRealisticDocument:
    def _multi_page_twin(self) -> dict:
        """A realistic 5-page document with tables."""
        pages = [
            {"page_number": 1, "text": (
                "Employee Handbook\n\n"
                "Welcome to Dynamo Corp. This handbook outlines the policies "
                "and procedures that govern your employment with us. Please "
                "read it carefully and keep it for reference."
            )},
            {"page_number": 2, "text": (
                "Chapter 1: Code of Conduct\n\n"
                "All employees are expected to maintain the highest standards "
                "of professional conduct. This includes treating colleagues "
                "with respect, maintaining confidentiality of company information, "
                "and adhering to all applicable laws and regulations.\n\n"
                "Violations of the code of conduct may result in disciplinary "
                "action up to and including termination of employment."
            )},
            {"page_number": 3, "text": (
                "Chapter 2: Benefits\n\n"
                "Full-time employees are eligible for the following benefits "
                "after completing their probationary period. Part-time employees "
                "may be eligible for prorated benefits depending on their "
                "hours worked per week."
            )},
            {"page_number": 4, "text": (
                "Chapter 3: Leave Policies\n\n"
                "Employees accrue paid time off based on their years of service. "
                "New employees receive fifteen days of PTO per year. After five "
                "years of service, this increases to twenty days. After ten years, "
                "employees receive twenty-five days of PTO annually.\n\n"
                "Sick leave is provided separately and does not count against PTO."
            )},
            {"page_number": 5, "text": (
                "Chapter 4: Remote Work\n\n"
                "Eligible employees may work remotely up to three days per week "
                "with manager approval. Remote work arrangements must be documented "
                "and reviewed quarterly."
            )},
        ]
        full_text = "\n\n".join(p["text"] for p in pages)
        tables = [
            {
                "table_index": 1,
                "rows": [
                    ["Benefit", "Coverage", "Employee Cost"],
                    ["Health Insurance", "100%", "$150/month"],
                    ["Dental", "80%", "$25/month"],
                    ["Vision", "80%", "$10/month"],
                ],
            },
        ]
        return _make_twin(text=full_text, pages=pages, tables=tables)

    def test_produces_multiple_text_chunks(self):
        chunker = DocumentChunker(chunk_size=100, chunk_overlap=10)
        twin = self._multi_page_twin()
        chunks = chunker.chunk_document(twin)

        text_chunks = [c for c in chunks if not c["text"].startswith("[")]
        assert len(text_chunks) >= 2

    def test_all_text_preserved(self):
        """All original words should appear in at least one chunk."""
        chunker = DocumentChunker(chunk_size=100, chunk_overlap=10)
        twin = self._multi_page_twin()
        chunks = chunker.chunk_document(twin)

        all_chunk_text = " ".join(c["text"] for c in chunks)
        original_words = twin["extracted_text"].split()

        for word in original_words:
            assert word in all_chunk_text, f"Missing word: {word}"

    def test_table_preserved_as_chunk(self):
        chunker = DocumentChunker(chunk_size=100, chunk_overlap=10)
        twin = self._multi_page_twin()
        chunks = chunker.chunk_document(twin)

        table_chunks = [c for c in chunks if c["text"].startswith("[")]
        assert len(table_chunks) == 1

        parsed = json.loads(table_chunks[0]["text"])
        assert parsed[0] == ["Benefit", "Coverage", "Employee Cost"]

    def test_page_numbers_make_sense(self):
        chunker = DocumentChunker(chunk_size=100, chunk_overlap=10)
        twin = self._multi_page_twin()
        chunks = chunker.chunk_document(twin)

        text_chunks = [c for c in chunks if not c["text"].startswith("[")]
        for chunk in text_chunks:
            pns = chunk["metadata"]["page_numbers"]
            # Page numbers should be non-empty and valid
            assert len(pns) > 0
            assert all(1 <= pn <= 5 for pn in pns)
            # Should be sorted
            assert pns == sorted(pns)

    def test_chunk_ids_are_unique(self):
        chunker = DocumentChunker(chunk_size=100, chunk_overlap=10)
        twin = self._multi_page_twin()
        chunks = chunker.chunk_document(twin)

        chunk_ids = [c["chunk_id"] for c in chunks]
        assert len(chunk_ids) == len(set(chunk_ids))

    def test_round_trip_jsonl(self, tmp_path):
        """Chunk, export to JSONL, read back, verify integrity."""
        chunker = DocumentChunker(chunk_size=100, chunk_overlap=10)
        twin = self._multi_page_twin()
        chunks = chunker.chunk_document(twin)

        output = str(tmp_path / "roundtrip.jsonl")
        count = DocumentChunker.export_chunks_to_jsonl(chunks, output)
        assert count == len(chunks)

        # Read back
        with open(output) as f:
            loaded = [json.loads(line) for line in f]

        assert len(loaded) == len(chunks)
        for original, loaded_chunk in zip(chunks, loaded):
            assert original["chunk_id"] == loaded_chunk["chunk_id"]
            assert original["text"] == loaded_chunk["text"]
            assert original["metadata"] == loaded_chunk["metadata"]
