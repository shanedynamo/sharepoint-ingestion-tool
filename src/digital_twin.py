"""Build JSON 'digital twin' documents from Textract output or direct extraction.

A digital twin captures the full extracted content of a SharePoint document
(text, tables, metadata) in a normalised JSON schema stored alongside the
source file in S3.

Two entry points:

* :meth:`DigitalTwinBuilder.build_twin_from_textract` — from a Textract
  response (document analysis or text detection).
* :meth:`DigitalTwinBuilder.build_twin_from_direct_extract` — from
  pure-Python extraction (python-pptx / openpyxl).
"""

import hashlib
import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Current schema version for the twin JSON.
SCHEMA_VERSION = "2.0"


class DigitalTwinBuilder:
    """Assembles normalised JSON twin documents."""

    # ------------------------------------------------------------------ #
    # Public: Textract path
    # ------------------------------------------------------------------ #

    @staticmethod
    def build_twin_from_textract(
        textract_response: dict,
        source_metadata: dict,
    ) -> dict[str, Any]:
        """Build a twin from a consolidated Textract response.

        Parameters
        ----------
        textract_response:
            A single dict with a ``Blocks`` list — the output of
            :meth:`TextractClient.get_document_analysis` or
            :meth:`TextractClient.get_text_detection`.
        source_metadata:
            Registry / S3-tag metadata for the source document.  Expected
            keys: ``s3_source_key``, ``sp_path``, ``sp_library``,
            ``file_type``, ``size_bytes``, ``content_type``, etc.
        """
        blocks = textract_response.get("Blocks", [])
        blocks_by_id = {b["Id"]: b for b in blocks if "Id" in b}

        pages = _group_lines_by_page(blocks)
        tables = _extract_tables(blocks, blocks_by_id)
        full_text = "\n\n".join(
            "\n".join(lines) for lines in pages.values() if lines
        )
        avg_confidence = _average_confidence(blocks)
        job_id = textract_response.get("JobId", "")

        return _assemble_twin(
            source_metadata=source_metadata,
            extracted_text=full_text,
            pages=[
                {"page_number": pg, "text": "\n".join(lines)}
                for pg, lines in sorted(pages.items())
            ],
            tables=tables,
            extraction_metadata={
                "method": "textract-document-analysis",
                "job_id": job_id,
                "confidence": avg_confidence,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "block_count": len(blocks),
            },
        )

    # ------------------------------------------------------------------ #
    # Public: Direct-extract path (python-pptx / openpyxl)
    # ------------------------------------------------------------------ #

    @staticmethod
    def build_twin_from_direct_extract(
        text: str,
        tables: list[list[list[str]]],
        source_metadata: dict,
    ) -> dict[str, Any]:
        """Build a twin from direct Python extraction.

        Parameters
        ----------
        text:
            Full extracted text (e.g. from ``FileConverter.convert_to_pdf_lambda``
            decoded as UTF-8).
        tables:
            List of tables, each a list of rows, each row a list of cell
            strings.  May be empty.
        source_metadata:
            Same dict as for :meth:`build_twin_from_textract`.
        """
        file_type = source_metadata.get("file_type", "").lower()
        if file_type in (".pptx", ".ppt"):
            method = "direct-python-pptx"
        elif file_type in (".xlsx", ".xls"):
            method = "direct-openpyxl"
        else:
            method = f"direct-{file_type.lstrip('.')}"

        # Build pages from section headers if present, otherwise single page
        pages = _pages_from_text(text)

        formatted_tables = [
            {"table_index": idx, "rows": tbl}
            for idx, tbl in enumerate(tables, 1)
        ]

        return _assemble_twin(
            source_metadata=source_metadata,
            extracted_text=text,
            pages=pages,
            tables=formatted_tables,
            extraction_metadata={
                "method": method,
                "job_id": None,
                "confidence": None,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "block_count": None,
            },
        )


# ===================================================================
# Internal helpers
# ===================================================================

def _assemble_twin(
    *,
    source_metadata: dict,
    extracted_text: str,
    pages: list[dict],
    tables: list[dict],
    extraction_metadata: dict,
) -> dict[str, Any]:
    """Build the canonical twin dict shared by both code-paths."""
    s3_key = source_metadata.get("s3_source_key", "")
    document_id = hashlib.sha256(s3_key.encode()).hexdigest()
    filename = os.path.basename(source_metadata.get("sp_path", "")) or s3_key.rsplit("/", 1)[-1]

    return {
        "schema_version": SCHEMA_VERSION,
        "document_id": document_id,
        "source_s3_key": s3_key,
        "source_sharepoint_url": source_metadata.get("source_sharepoint_url", ""),
        "filename": filename,
        "file_type": source_metadata.get("file_type", ""),
        "content_type": source_metadata.get("content_type", ""),
        "metadata": {
            "sp_library": source_metadata.get("sp_library", ""),
            "sp_path": source_metadata.get("sp_path", ""),
            "sp_item_id": source_metadata.get("sp_item_id", ""),
            "sp_last_modified": source_metadata.get("sp_last_modified", ""),
            "size_bytes": source_metadata.get("size_bytes", 0),
        },
        "extracted_text": extracted_text,
        "pages": pages,
        "tables": tables,
        "extraction_metadata": extraction_metadata,
    }


def _group_lines_by_page(blocks: list[dict]) -> dict[int, list[str]]:
    """Group LINE blocks by their ``Page`` number.

    Returns a dict mapping page number → list of line strings.
    """
    pages: dict[int, list[str]] = {}
    for block in blocks:
        if block.get("BlockType") != "LINE":
            continue
        page_num = block.get("Page", 1)
        pages.setdefault(page_num, []).append(block.get("Text", ""))
    return pages


def _extract_tables(
    blocks: list[dict],
    blocks_by_id: dict[str, dict],
) -> list[dict]:
    """Extract structured tables from Textract blocks.

    Each TABLE block's CHILD cells are resolved into rows and columns using
    the ``RowIndex`` / ``ColumnIndex`` fields on CELL blocks.
    """
    tables: list[dict] = []
    table_index = 0

    for block in blocks:
        if block.get("BlockType") != "TABLE":
            continue
        table_index += 1

        # Gather all CELL children
        cells: list[dict] = []
        for rel in block.get("Relationships", []):
            if rel["Type"] != "CHILD":
                continue
            for child_id in rel["Ids"]:
                child = blocks_by_id.get(child_id, {})
                if child.get("BlockType") == "CELL":
                    cells.append(child)

        if not cells:
            continue

        # Determine grid dimensions
        max_row = max(c.get("RowIndex", 1) for c in cells)
        max_col = max(c.get("ColumnIndex", 1) for c in cells)

        # Build a 2-D grid
        grid: list[list[str]] = [[""] * max_col for _ in range(max_row)]
        for cell in cells:
            r = cell.get("RowIndex", 1) - 1
            c = cell.get("ColumnIndex", 1) - 1
            cell_text = _cell_text(cell, blocks_by_id)
            grid[r][c] = cell_text

        tables.append({"table_index": table_index, "rows": grid})

    return tables


def _cell_text(cell: dict, blocks_by_id: dict[str, dict]) -> str:
    """Concatenate WORD block texts inside a CELL."""
    parts: list[str] = []
    for rel in cell.get("Relationships", []):
        if rel["Type"] != "CHILD":
            continue
        for word_id in rel["Ids"]:
            word = blocks_by_id.get(word_id, {})
            if word.get("BlockType") == "WORD":
                parts.append(word.get("Text", ""))
    return " ".join(parts)


def _average_confidence(blocks: list[dict]) -> float | None:
    """Compute mean confidence across all blocks that have a score."""
    scores = [b["Confidence"] for b in blocks if "Confidence" in b]
    if not scores:
        return None
    return round(sum(scores) / len(scores), 2)


def _pages_from_text(text: str) -> list[dict]:
    """Split direct-extract text into pages by ``--- … ---`` headers."""
    if not text.strip():
        return []

    # Split on "--- Slide N ---" or "--- SheetName ---" markers
    import re
    parts = re.split(r"^---\s+.+?\s+---$", text, flags=re.MULTILINE)

    # If no headers found, return as a single page
    if len(parts) <= 1:
        return [{"page_number": 1, "text": text.strip()}]

    pages: list[dict] = []
    for idx, part in enumerate(parts, 1):
        stripped = part.strip()
        if stripped:
            pages.append({"page_number": idx, "text": stripped})

    # Renumber sequentially (skipping empty splits)
    for i, page in enumerate(pages, 1):
        page["page_number"] = i

    return pages
