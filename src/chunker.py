"""Break JSON digital twins into chunks suitable for vector embedding.

This module bridges the ingestion service and the RAG pipeline.  It reads
the JSON twin documents produced by :mod:`digital_twin` and yields chunk
records ready for embedding and indexing.

Usage::

    from chunker import DocumentChunker

    chunker = DocumentChunker(chunk_size=512, chunk_overlap=50)
    chunks = chunker.chunk_document(twin_json)

    # Stream all twins from S3
    for chunk in chunker.chunk_all_documents(s3_client, bucket, "extracted/"):
        send_to_embedding_pipeline(chunk)
"""

import json
import logging
import re
from collections.abc import Generator
from typing import Any

logger = logging.getLogger(__name__)

# Approximate ratio: 1 token ≈ 0.75 words.
_TOKENS_TO_WORDS = 0.75

# Regex patterns for splitting text, ordered by preference.
_PARAGRAPH_BREAK = re.compile(r"\n\s*\n")
_SENTENCE_BREAK = re.compile(r"(?<=[.!?])\s+")
_WORD_BREAK = re.compile(r"\s+")


class DocumentChunker:
    """Split digital twin documents into overlapping chunks for embedding."""

    def __init__(self, chunk_size: int = 512, chunk_overlap: int = 50) -> None:
        """
        Parameters
        ----------
        chunk_size:
            Target tokens per chunk.  Approximated as ``word_count / 0.75``.
        chunk_overlap:
            Tokens of overlap between consecutive chunks.
        """
        if chunk_overlap >= chunk_size:
            raise ValueError("chunk_overlap must be less than chunk_size")
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self._target_words = int(chunk_size * _TOKENS_TO_WORDS)
        self._overlap_words = int(chunk_overlap * _TOKENS_TO_WORDS)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def chunk_document(self, twin_json: dict) -> list[dict]:
        """Chunk a single digital twin into embedding-ready records.

        Parameters
        ----------
        twin_json:
            A parsed digital twin document (schema version 2.0).

        Returns
        -------
        list[dict]
            List of chunk dicts, each with ``chunk_id``, ``text``,
            ``metadata``, etc.
        """
        document_id = twin_json.get("document_id", "")
        source_s3_key = twin_json.get("source_s3_key", "")
        filename = twin_json.get("filename", "")
        file_type = twin_json.get("file_type", "")
        twin_meta = twin_json.get("metadata", {})

        base_metadata = {
            "sp_site": twin_meta.get("sp_site", ""),
            "sp_library": twin_meta.get("sp_library", ""),
            "sp_path": twin_meta.get("sp_path", ""),
            "access_tags": twin_meta.get("access_tags", []),
            "author": twin_meta.get("author", ""),
            "last_modified": twin_meta.get("sp_last_modified", ""),
            "file_type": file_type,
            "page_numbers": [],
        }

        chunks: list[dict] = []

        # --- Text chunks (from pages) ---
        pages = twin_json.get("pages", [])
        extracted_text = twin_json.get("extracted_text", "")

        if pages:
            text_chunks = self._chunk_pages(pages)
        elif extracted_text:
            text_chunks = self._chunk_text(extracted_text)
        else:
            text_chunks = []

        for text, page_numbers in text_chunks:
            meta = {**base_metadata, "page_numbers": page_numbers}
            chunks.append(self._build_chunk(
                document_id=document_id,
                source_s3_key=source_s3_key,
                filename=filename,
                chunk_index=len(chunks),
                text=text,
                metadata=meta,
            ))

        # --- Table chunks (each table is its own chunk) ---
        tables = twin_json.get("tables", [])
        for table in tables:
            rows = table.get("rows", [])
            if not rows:
                continue
            table_text = json.dumps(rows, ensure_ascii=False)
            meta = {**base_metadata, "page_numbers": []}
            chunks.append(self._build_chunk(
                document_id=document_id,
                source_s3_key=source_s3_key,
                filename=filename,
                chunk_index=len(chunks),
                text=table_text,
                metadata=meta,
            ))

        # Back-fill total_chunks now that we know the final count.
        total = len(chunks)
        for chunk in chunks:
            chunk["total_chunks"] = total

        return chunks

    def chunk_all_documents(
        self,
        s3_client: Any,
        bucket: str,
        extracted_prefix: str = "extracted/",
    ) -> Generator[dict, None, None]:
        """Stream chunks from all twin JSON files in S3.

        Memory-efficient: yields one chunk at a time.

        Parameters
        ----------
        s3_client:
            A ``boto3`` S3 client.
        bucket:
            S3 bucket name.
        extracted_prefix:
            Key prefix for twin JSON files.
        """
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iter = paginator.paginate(Bucket=bucket, Prefix=extracted_prefix)

        for page in page_iter:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue

                try:
                    resp = s3_client.get_object(Bucket=bucket, Key=key)
                    twin_json = json.loads(resp["Body"].read())
                    chunks = self.chunk_document(twin_json)
                    yield from chunks
                except Exception:
                    logger.exception("Failed to chunk %s", key)

    # ------------------------------------------------------------------ #
    # Utility
    # ------------------------------------------------------------------ #

    @staticmethod
    def export_chunks_to_jsonl(
        chunks: Generator[dict, None, None] | list[dict],
        output_path: str,
        s3_client: Any | None = None,
    ) -> int:
        """Write chunks as JSONL (one JSON object per line).

        Parameters
        ----------
        chunks:
            Iterable of chunk dicts.
        output_path:
            Local file path, or an ``s3://bucket/key`` URI.
        s3_client:
            Required if *output_path* is an S3 URI.

        Returns
        -------
        int
            Number of chunks written.
        """
        if output_path.startswith("s3://"):
            return _write_jsonl_s3(chunks, output_path, s3_client)
        return _write_jsonl_local(chunks, output_path)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _chunk_pages(
        self, pages: list[dict],
    ) -> list[tuple[str, list[int]]]:
        """Chunk text from structured pages, tracking page provenance."""
        # Concatenate all page text with page boundary markers.
        segments: list[tuple[str, int]] = []
        for page in pages:
            page_num = page.get("page_number", 1)
            text = page.get("text", "").strip()
            if text:
                segments.append((text, page_num))

        if not segments:
            return []

        # Build a single text with page-boundary tracking.
        full_text = ""
        # Map: word index → page number
        word_page_map: list[int] = []

        for text, page_num in segments:
            if full_text:
                full_text += "\n\n"
            words = text.split()
            word_page_map.extend([page_num] * len(words))
            full_text += text

        # Split the full text into chunks.
        raw_chunks = self._split_text(full_text)

        # Map each chunk back to the pages it spans.
        result: list[tuple[str, list[int]]] = []
        word_offset = 0

        for chunk_text in raw_chunks:
            chunk_words = chunk_text.split()
            chunk_len = len(chunk_words)

            # Determine which pages this chunk spans.
            start = min(word_offset, len(word_page_map) - 1)
            end = min(word_offset + chunk_len - 1, len(word_page_map) - 1)

            if start <= end and word_page_map:
                page_nums = sorted(set(word_page_map[start:end + 1]))
            else:
                page_nums = []

            result.append((chunk_text, page_nums))

            # Advance by chunk_len minus overlap for the next chunk.
            word_offset += chunk_len - self._overlap_words

        return result

    def _chunk_text(self, text: str) -> list[tuple[str, list[int]]]:
        """Chunk flat text (no page structure)."""
        raw_chunks = self._split_text(text)
        return [(chunk, []) for chunk in raw_chunks]

    def _split_text(self, text: str) -> list[str]:
        """Split text into chunks with overlap, preferring natural breaks."""
        text = text.strip()
        if not text:
            return []

        words = text.split()
        if len(words) <= self._target_words:
            return [text]

        # Split into paragraphs first.
        paragraphs = _PARAGRAPH_BREAK.split(text)
        paragraphs = [p.strip() for p in paragraphs if p.strip()]

        # Build chunks by accumulating paragraphs up to target size,
        # then fall back to sentence and word splitting for oversized paragraphs.
        chunks: list[str] = []
        current_words: list[str] = []

        for para in paragraphs:
            para_words = para.split()

            if len(current_words) + len(para_words) <= self._target_words:
                current_words.extend(para_words)
            else:
                # Current buffer is full; emit it if non-empty.
                if current_words:
                    chunks.append(" ".join(current_words))
                    # Keep overlap from the end of the current chunk.
                    overlap = current_words[-self._overlap_words:] if self._overlap_words else []
                    current_words = list(overlap)

                # If the paragraph itself exceeds target, split by sentences.
                if len(para_words) > self._target_words:
                    sentence_chunks = self._split_by_sentences(para)
                    for sc in sentence_chunks:
                        sc_words = sc.split()
                        if len(current_words) + len(sc_words) <= self._target_words:
                            current_words.extend(sc_words)
                        else:
                            if current_words:
                                chunks.append(" ".join(current_words))
                                overlap = current_words[-self._overlap_words:] if self._overlap_words else []
                                current_words = list(overlap)
                            # If a single sentence is still too large, split by words.
                            if len(sc_words) > self._target_words:
                                word_chunks = self._split_by_words(sc_words)
                                for wc in word_chunks[:-1]:
                                    chunks.append(" ".join(wc))
                                current_words = list(word_chunks[-1]) if word_chunks else []
                            else:
                                current_words.extend(sc_words)
                else:
                    current_words.extend(para_words)

        if current_words:
            chunks.append(" ".join(current_words))

        return chunks

    def _split_by_sentences(self, text: str) -> list[str]:
        """Split a paragraph into sentences."""
        sentences = _SENTENCE_BREAK.split(text)
        return [s.strip() for s in sentences if s.strip()]

    def _split_by_words(self, words: list[str]) -> list[list[str]]:
        """Split a word list into target-sized sublists with overlap."""
        chunks: list[list[str]] = []
        step = self._target_words - self._overlap_words
        if step < 1:
            step = 1
        i = 0
        while i < len(words):
            chunks.append(words[i:i + self._target_words])
            i += step
        return chunks

    @staticmethod
    def _build_chunk(
        *,
        document_id: str,
        source_s3_key: str,
        filename: str,
        chunk_index: int,
        text: str,
        metadata: dict,
    ) -> dict:
        return {
            "chunk_id": f"{document_id}_{chunk_index}",
            "document_id": document_id,
            "source_s3_key": source_s3_key,
            "filename": filename,
            "chunk_index": chunk_index,
            "total_chunks": 0,  # back-filled after all chunks are created
            "text": text,
            "metadata": metadata,
        }


# ===================================================================
# File output helpers
# ===================================================================

def _write_jsonl_local(chunks, output_path: str) -> int:
    count = 0
    with open(output_path, "w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + "\n")
            count += 1
    logger.info("Wrote %d chunks to %s", count, output_path)
    return count


def _write_jsonl_s3(chunks, output_path: str, s3_client) -> int:
    if s3_client is None:
        raise ValueError("s3_client is required for S3 output paths")

    # Parse s3://bucket/key
    path = output_path[5:]  # strip "s3://"
    bucket, key = path.split("/", 1)

    lines: list[str] = []
    for chunk in chunks:
        lines.append(json.dumps(chunk, ensure_ascii=False))

    body = "\n".join(lines) + "\n" if lines else ""
    s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    logger.info("Wrote %d chunks to %s", len(lines), output_path)
    return len(lines)
