"""Tests for DigitalTwinBuilder."""

import hashlib
import sys

import pytest

sys.path.insert(0, "src")

from digital_twin import (
    DigitalTwinBuilder,
    SCHEMA_VERSION,
    _group_lines_by_page,
    _extract_tables,
    _cell_text,
    _average_confidence,
    _pages_from_text,
)


# ===================================================================
# Fixtures: Textract response blocks
# ===================================================================

def _word(word_id: str, text: str, confidence: float = 99.0, page: int = 1) -> dict:
    return {
        "Id": word_id,
        "BlockType": "WORD",
        "Text": text,
        "Confidence": confidence,
        "Page": page,
    }


def _line(line_id: str, text: str, word_ids: list[str],
          confidence: float = 98.0, page: int = 1) -> dict:
    return {
        "Id": line_id,
        "BlockType": "LINE",
        "Text": text,
        "Confidence": confidence,
        "Page": page,
        "Relationships": [{"Type": "CHILD", "Ids": word_ids}],
    }


def _cell(cell_id: str, row: int, col: int, word_ids: list[str]) -> dict:
    block: dict = {
        "Id": cell_id,
        "BlockType": "CELL",
        "RowIndex": row,
        "ColumnIndex": col,
        "Confidence": 95.0,
    }
    if word_ids:
        block["Relationships"] = [{"Type": "CHILD", "Ids": word_ids}]
    return block


def _table(table_id: str, cell_ids: list[str]) -> dict:
    return {
        "Id": table_id,
        "BlockType": "TABLE",
        "Confidence": 97.0,
        "Relationships": [{"Type": "CHILD", "Ids": cell_ids}],
    }


def _page_block(page_id: str, child_ids: list[str], page: int = 1) -> dict:
    return {
        "Id": page_id,
        "BlockType": "PAGE",
        "Page": page,
        "Relationships": [{"Type": "CHILD", "Ids": child_ids}],
    }


def _sample_metadata(**overrides) -> dict:
    base = {
        "s3_source_key": "source/Dynamo/HR/handbook.pdf",
        "sp_path": "/HR/handbook.pdf",
        "sp_library": "HR",
        "sp_item_id": "sp-001",
        "sp_last_modified": "2025-06-01T10:00:00Z",
        "file_type": ".pdf",
        "content_type": "application/pdf",
        "size_bytes": 50000,
        "source_sharepoint_url": "https://sp.example.com/HR/handbook.pdf",
    }
    base.update(overrides)
    return base


def _simple_textract_response() -> dict:
    """A realistic two-page Textract response with text and a table."""
    return {
        "JobId": "job-abc-123",
        "JobStatus": "SUCCEEDED",
        "DocumentMetadata": {"Pages": 2},
        "Blocks": [
            # Page 1: two lines of text
            _page_block("pg1", ["l1", "l2", "w1", "w2", "w3", "w4"], page=1),
            _word("w1", "Company", page=1),
            _word("w2", "Handbook", page=1),
            _line("l1", "Company Handbook", ["w1", "w2"], page=1),
            _word("w3", "Version", page=1),
            _word("w4", "2.0", page=1),
            _line("l2", "Version 2.0", ["w3", "w4"], page=1),

            # Page 2: one line of text + a 2x2 table
            _page_block("pg2", ["l3", "w5", "w6", "t1"], page=2),
            _word("w5", "Summary", page=2),
            _word("w6", "section", page=2),
            _line("l3", "Summary section", ["w5", "w6"], page=2),

            # Table on page 2
            _word("tw1", "Name"),
            _word("tw2", "Value"),
            _word("tw3", "Alice"),
            _word("tw4", "100"),
            _cell("c1", 1, 1, ["tw1"]),
            _cell("c2", 1, 2, ["tw2"]),
            _cell("c3", 2, 1, ["tw3"]),
            _cell("c4", 2, 2, ["tw4"]),
            _table("t1", ["c1", "c2", "c3", "c4"]),
        ],
    }


# ===================================================================
# _group_lines_by_page
# ===================================================================

class TestGroupLinesByPage:
    def test_groups_by_page_number(self):
        blocks = [
            {"BlockType": "LINE", "Text": "Page 1 line 1", "Page": 1},
            {"BlockType": "LINE", "Text": "Page 1 line 2", "Page": 1},
            {"BlockType": "LINE", "Text": "Page 2 line 1", "Page": 2},
        ]
        result = _group_lines_by_page(blocks)
        assert result == {
            1: ["Page 1 line 1", "Page 1 line 2"],
            2: ["Page 2 line 1"],
        }

    def test_ignores_non_line_blocks(self):
        blocks = [
            {"BlockType": "WORD", "Text": "word", "Page": 1},
            {"BlockType": "LINE", "Text": "line", "Page": 1},
            {"BlockType": "TABLE", "Page": 1},
        ]
        result = _group_lines_by_page(blocks)
        assert result == {1: ["line"]}

    def test_defaults_to_page_one(self):
        blocks = [{"BlockType": "LINE", "Text": "no page field"}]
        result = _group_lines_by_page(blocks)
        assert result == {1: ["no page field"]}

    def test_empty_blocks(self):
        assert _group_lines_by_page([]) == {}


# ===================================================================
# _extract_tables
# ===================================================================

class TestExtractTables:
    def test_extracts_2x2_table(self):
        blocks = [
            _word("w1", "A"),
            _word("w2", "B"),
            _word("w3", "C"),
            _word("w4", "D"),
            _cell("c1", 1, 1, ["w1"]),
            _cell("c2", 1, 2, ["w2"]),
            _cell("c3", 2, 1, ["w3"]),
            _cell("c4", 2, 2, ["w4"]),
            _table("t1", ["c1", "c2", "c3", "c4"]),
        ]
        by_id = {b["Id"]: b for b in blocks}
        tables = _extract_tables(blocks, by_id)

        assert len(tables) == 1
        assert tables[0]["table_index"] == 1
        assert tables[0]["rows"] == [["A", "B"], ["C", "D"]]

    def test_extracts_multiple_tables(self):
        blocks = [
            _word("w1", "X"),
            _cell("c1", 1, 1, ["w1"]),
            _table("t1", ["c1"]),
            _word("w2", "Y"),
            _cell("c2", 1, 1, ["w2"]),
            _table("t2", ["c2"]),
        ]
        by_id = {b["Id"]: b for b in blocks}
        tables = _extract_tables(blocks, by_id)

        assert len(tables) == 2
        assert tables[0]["table_index"] == 1
        assert tables[1]["table_index"] == 2
        assert tables[0]["rows"] == [["X"]]
        assert tables[1]["rows"] == [["Y"]]

    def test_empty_cell(self):
        """Cell with no word children produces empty string."""
        blocks = [
            _cell("c1", 1, 1, []),
            _table("t1", ["c1"]),
        ]
        by_id = {b["Id"]: b for b in blocks}
        tables = _extract_tables(blocks, by_id)

        assert tables[0]["rows"] == [[""]]

    def test_multi_word_cell(self):
        blocks = [
            _word("w1", "Hello"),
            _word("w2", "World"),
            _cell("c1", 1, 1, ["w1", "w2"]),
            _table("t1", ["c1"]),
        ]
        by_id = {b["Id"]: b for b in blocks}
        tables = _extract_tables(blocks, by_id)

        assert tables[0]["rows"] == [["Hello World"]]

    def test_no_tables(self):
        blocks = [{"Id": "l1", "BlockType": "LINE", "Text": "hello"}]
        by_id = {b["Id"]: b for b in blocks}
        assert _extract_tables(blocks, by_id) == []

    def test_table_with_no_cells(self):
        """TABLE block with no CELL children is skipped."""
        blocks = [_table("t1", [])]
        by_id = {b["Id"]: b for b in blocks}
        assert _extract_tables(blocks, by_id) == []

    def test_3x3_grid_indexing(self):
        """Verify RowIndex/ColumnIndex are correctly mapped to grid positions."""
        words = [_word(f"w{i}", f"v{i}") for i in range(1, 10)]
        cells = [
            _cell(f"c{i}", (i - 1) // 3 + 1, (i - 1) % 3 + 1, [f"w{i}"])
            for i in range(1, 10)
        ]
        blocks = words + cells + [_table("t1", [c["Id"] for c in cells])]
        by_id = {b["Id"]: b for b in blocks}
        tables = _extract_tables(blocks, by_id)

        assert len(tables) == 1
        assert tables[0]["rows"] == [
            ["v1", "v2", "v3"],
            ["v4", "v5", "v6"],
            ["v7", "v8", "v9"],
        ]


# ===================================================================
# _cell_text
# ===================================================================

class TestCellText:
    def test_concatenates_words(self):
        blocks_by_id = {
            "w1": {"BlockType": "WORD", "Text": "Hello"},
            "w2": {"BlockType": "WORD", "Text": "World"},
        }
        cell = {"Relationships": [{"Type": "CHILD", "Ids": ["w1", "w2"]}]}
        assert _cell_text(cell, blocks_by_id) == "Hello World"

    def test_ignores_non_word_children(self):
        blocks_by_id = {
            "w1": {"BlockType": "WORD", "Text": "Text"},
            "s1": {"BlockType": "SELECTION_ELEMENT", "Text": "X"},
        }
        cell = {"Relationships": [{"Type": "CHILD", "Ids": ["w1", "s1"]}]}
        assert _cell_text(cell, blocks_by_id) == "Text"

    def test_no_relationships(self):
        assert _cell_text({}, {}) == ""


# ===================================================================
# _average_confidence
# ===================================================================

class TestAverageConfidence:
    def test_computes_average(self):
        blocks = [
            {"Confidence": 90.0},
            {"Confidence": 80.0},
            {"Confidence": 100.0},
        ]
        assert _average_confidence(blocks) == 90.0

    def test_skips_blocks_without_confidence(self):
        blocks = [
            {"Confidence": 80.0},
            {"BlockType": "PAGE"},
            {"Confidence": 100.0},
        ]
        assert _average_confidence(blocks) == 90.0

    def test_returns_none_for_no_scores(self):
        assert _average_confidence([{"BlockType": "PAGE"}]) is None

    def test_returns_none_for_empty(self):
        assert _average_confidence([]) is None

    def test_rounds_to_two_decimals(self):
        blocks = [
            {"Confidence": 33.33},
            {"Confidence": 33.33},
            {"Confidence": 33.34},
        ]
        assert _average_confidence(blocks) == 33.33


# ===================================================================
# _pages_from_text
# ===================================================================

class TestPagesFromText:
    def test_splits_on_slide_headers(self):
        text = "--- Slide 1 ---\nFirst slide\n--- Slide 2 ---\nSecond slide"
        pages = _pages_from_text(text)
        assert len(pages) == 2
        assert pages[0] == {"page_number": 1, "text": "First slide"}
        assert pages[1] == {"page_number": 2, "text": "Second slide"}

    def test_splits_on_sheet_headers(self):
        text = "--- Sheet1 ---\nData\n--- Sheet2 ---\nMore data"
        pages = _pages_from_text(text)
        assert len(pages) == 2

    def test_no_headers_returns_single_page(self):
        text = "Just plain text\nwith multiple lines"
        pages = _pages_from_text(text)
        assert len(pages) == 1
        assert pages[0]["page_number"] == 1
        assert pages[0]["text"] == text.strip()

    def test_empty_text_returns_empty(self):
        assert _pages_from_text("") == []
        assert _pages_from_text("   ") == []

    def test_skips_empty_sections(self):
        text = "--- Slide 1 ---\n\n--- Slide 2 ---\nContent"
        pages = _pages_from_text(text)
        assert len(pages) == 1
        assert pages[0]["text"] == "Content"
        assert pages[0]["page_number"] == 1


# ===================================================================
# build_twin_from_textract
# ===================================================================

class TestBuildTwinFromTextract:
    def test_schema_version(self):
        resp = _simple_textract_response()
        meta = _sample_metadata()
        twin = DigitalTwinBuilder.build_twin_from_textract(resp, meta)

        assert twin["schema_version"] == SCHEMA_VERSION

    def test_document_id_is_sha256_of_s3_key(self):
        meta = _sample_metadata()
        twin = DigitalTwinBuilder.build_twin_from_textract(
            _simple_textract_response(), meta,
        )
        expected = hashlib.sha256(meta["s3_source_key"].encode()).hexdigest()
        assert twin["document_id"] == expected

    def test_source_fields(self):
        meta = _sample_metadata()
        twin = DigitalTwinBuilder.build_twin_from_textract(
            _simple_textract_response(), meta,
        )
        assert twin["source_s3_key"] == "source/Dynamo/HR/handbook.pdf"
        assert twin["source_sharepoint_url"] == "https://sp.example.com/HR/handbook.pdf"
        assert twin["filename"] == "handbook.pdf"
        assert twin["file_type"] == ".pdf"
        assert twin["content_type"] == "application/pdf"

    def test_metadata_block(self):
        meta = _sample_metadata()
        twin = DigitalTwinBuilder.build_twin_from_textract(
            _simple_textract_response(), meta,
        )
        assert twin["metadata"]["sp_library"] == "HR"
        assert twin["metadata"]["sp_path"] == "/HR/handbook.pdf"
        assert twin["metadata"]["sp_item_id"] == "sp-001"
        assert twin["metadata"]["size_bytes"] == 50000

    def test_extracted_text_from_lines(self):
        resp = _simple_textract_response()
        twin = DigitalTwinBuilder.build_twin_from_textract(resp, _sample_metadata())

        # Page 1: "Company Handbook\nVersion 2.0"
        # Page 2: "Summary section"
        assert "Company Handbook" in twin["extracted_text"]
        assert "Version 2.0" in twin["extracted_text"]
        assert "Summary section" in twin["extracted_text"]

    def test_pages_array(self):
        resp = _simple_textract_response()
        twin = DigitalTwinBuilder.build_twin_from_textract(resp, _sample_metadata())

        assert len(twin["pages"]) == 2
        assert twin["pages"][0]["page_number"] == 1
        assert "Company Handbook" in twin["pages"][0]["text"]
        assert twin["pages"][1]["page_number"] == 2
        assert "Summary section" in twin["pages"][1]["text"]

    def test_tables_array(self):
        resp = _simple_textract_response()
        twin = DigitalTwinBuilder.build_twin_from_textract(resp, _sample_metadata())

        assert len(twin["tables"]) == 1
        table = twin["tables"][0]
        assert table["table_index"] == 1
        assert table["rows"] == [["Name", "Value"], ["Alice", "100"]]

    def test_extraction_metadata(self):
        resp = _simple_textract_response()
        twin = DigitalTwinBuilder.build_twin_from_textract(resp, _sample_metadata())

        em = twin["extraction_metadata"]
        assert em["method"] == "textract-document-analysis"
        assert em["job_id"] == "job-abc-123"
        assert em["confidence"] is not None
        assert isinstance(em["confidence"], float)
        assert em["block_count"] == len(resp["Blocks"])
        assert "T" in em["timestamp"]  # ISO format

    def test_empty_textract_response(self):
        resp = {"JobId": "empty", "Blocks": []}
        twin = DigitalTwinBuilder.build_twin_from_textract(resp, _sample_metadata())

        assert twin["extracted_text"] == ""
        assert twin["pages"] == []
        assert twin["tables"] == []
        assert twin["extraction_metadata"]["confidence"] is None

    def test_text_only_no_tables(self):
        resp = {
            "JobId": "text-only",
            "Blocks": [
                _word("w1", "Hello", page=1),
                _line("l1", "Hello", ["w1"], page=1),
            ],
        }
        twin = DigitalTwinBuilder.build_twin_from_textract(resp, _sample_metadata())

        assert twin["extracted_text"] == "Hello"
        assert len(twin["pages"]) == 1
        assert twin["tables"] == []


# ===================================================================
# build_twin_from_direct_extract
# ===================================================================

class TestBuildTwinFromDirectExtract:
    def test_pptx_method(self):
        meta = _sample_metadata(file_type=".pptx")
        twin = DigitalTwinBuilder.build_twin_from_direct_extract(
            "slide content", [], meta,
        )
        assert twin["extraction_metadata"]["method"] == "direct-python-pptx"

    def test_xlsx_method(self):
        meta = _sample_metadata(file_type=".xlsx")
        twin = DigitalTwinBuilder.build_twin_from_direct_extract(
            "sheet data", [], meta,
        )
        assert twin["extraction_metadata"]["method"] == "direct-openpyxl"

    def test_schema_version(self):
        twin = DigitalTwinBuilder.build_twin_from_direct_extract(
            "text", [], _sample_metadata(file_type=".pptx"),
        )
        assert twin["schema_version"] == SCHEMA_VERSION

    def test_document_id(self):
        meta = _sample_metadata(file_type=".pptx")
        twin = DigitalTwinBuilder.build_twin_from_direct_extract("t", [], meta)
        expected = hashlib.sha256(meta["s3_source_key"].encode()).hexdigest()
        assert twin["document_id"] == expected

    def test_extracted_text(self):
        twin = DigitalTwinBuilder.build_twin_from_direct_extract(
            "Full text content", [], _sample_metadata(file_type=".pptx"),
        )
        assert twin["extracted_text"] == "Full text content"

    def test_pages_from_slide_headers(self):
        text = "--- Slide 1 ---\nSlide one\n--- Slide 2 ---\nSlide two"
        twin = DigitalTwinBuilder.build_twin_from_direct_extract(
            text, [], _sample_metadata(file_type=".pptx"),
        )
        assert len(twin["pages"]) == 2
        assert twin["pages"][0]["text"] == "Slide one"
        assert twin["pages"][1]["text"] == "Slide two"

    def test_tables_formatted(self):
        tables = [
            [["H1", "H2"], ["A", "B"]],
            [["X", "Y"]],
        ]
        twin = DigitalTwinBuilder.build_twin_from_direct_extract(
            "text", tables, _sample_metadata(file_type=".xlsx"),
        )
        assert len(twin["tables"]) == 2
        assert twin["tables"][0]["table_index"] == 1
        assert twin["tables"][0]["rows"] == [["H1", "H2"], ["A", "B"]]
        assert twin["tables"][1]["table_index"] == 2

    def test_no_job_id_or_confidence(self):
        twin = DigitalTwinBuilder.build_twin_from_direct_extract(
            "text", [], _sample_metadata(file_type=".pptx"),
        )
        em = twin["extraction_metadata"]
        assert em["job_id"] is None
        assert em["confidence"] is None
        assert em["block_count"] is None

    def test_empty_text(self):
        twin = DigitalTwinBuilder.build_twin_from_direct_extract(
            "", [], _sample_metadata(file_type=".pptx"),
        )
        assert twin["extracted_text"] == ""
        assert twin["pages"] == []

    def test_metadata_block(self):
        meta = _sample_metadata(file_type=".xlsx")
        twin = DigitalTwinBuilder.build_twin_from_direct_extract("d", [], meta)
        assert twin["metadata"]["sp_library"] == "HR"
        assert twin["metadata"]["sp_item_id"] == "sp-001"
        assert twin["metadata"]["size_bytes"] == 50000

    def test_unknown_file_type_method(self):
        meta = _sample_metadata(file_type=".csv")
        twin = DigitalTwinBuilder.build_twin_from_direct_extract("d", [], meta)
        assert twin["extraction_metadata"]["method"] == "direct-csv"


# ===================================================================
# Integration: full twin structure
# ===================================================================

class TestTwinStructureIntegrity:
    """Verify the complete twin schema has all expected keys."""

    EXPECTED_TOP_KEYS = {
        "schema_version", "document_id", "source_s3_key",
        "source_sharepoint_url", "filename", "file_type",
        "content_type", "metadata", "extracted_text",
        "pages", "tables", "extraction_metadata",
    }

    EXPECTED_META_KEYS = {
        "sp_library", "sp_path", "sp_item_id",
        "sp_last_modified", "size_bytes",
    }

    EXPECTED_EXTRACTION_KEYS = {
        "method", "job_id", "confidence", "timestamp", "block_count",
    }

    def test_textract_twin_has_all_keys(self):
        twin = DigitalTwinBuilder.build_twin_from_textract(
            _simple_textract_response(), _sample_metadata(),
        )
        assert set(twin.keys()) == self.EXPECTED_TOP_KEYS
        assert set(twin["metadata"].keys()) == self.EXPECTED_META_KEYS
        assert set(twin["extraction_metadata"].keys()) == self.EXPECTED_EXTRACTION_KEYS

    def test_direct_twin_has_all_keys(self):
        twin = DigitalTwinBuilder.build_twin_from_direct_extract(
            "text", [], _sample_metadata(file_type=".pptx"),
        )
        assert set(twin.keys()) == self.EXPECTED_TOP_KEYS
        assert set(twin["metadata"].keys()) == self.EXPECTED_META_KEYS
        assert set(twin["extraction_metadata"].keys()) == self.EXPECTED_EXTRACTION_KEYS

    def test_filename_extracted_from_sp_path(self):
        meta = _sample_metadata(sp_path="/Legal/contracts/nda.pdf")
        twin = DigitalTwinBuilder.build_twin_from_textract(
            {"Blocks": []}, meta,
        )
        assert twin["filename"] == "nda.pdf"

    def test_filename_fallback_to_s3_key(self):
        meta = _sample_metadata(sp_path="")
        twin = DigitalTwinBuilder.build_twin_from_textract(
            {"Blocks": []}, meta,
        )
        assert twin["filename"] == "handbook.pdf"
