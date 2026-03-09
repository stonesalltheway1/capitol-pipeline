"""Chunking utilities for search indexing and embeddings."""

from __future__ import annotations

import re

from capitol_pipeline.config import Settings
from capitol_pipeline.models.search import SearchChunkRecord, SearchDocumentRecord


def estimate_tokens(text: str) -> int:
    """Cheap token estimate suitable for chunk sizing."""

    if not text.strip():
        return 0
    return max(1, round(len(text) / 4))


def normalize_chunk_text(text: str) -> str:
    """Normalize whitespace without flattening the entire document."""

    return re.sub(r"\n{3,}", "\n\n", text.replace("\r\n", "\n")).strip()


def split_paragraphs(text: str) -> list[str]:
    """Split text into paragraph-like units."""

    normalized = normalize_chunk_text(text)
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", normalized) if part.strip()]
    return paragraphs or ([normalized] if normalized else [])


def chunk_text(text: str, settings: Settings) -> list[str]:
    """Chunk text using a simple semantic-aware paragraph packer."""

    target_chars = max(800, settings.embedding_chunk_size)
    overlap_chars = max(0, min(settings.embedding_chunk_overlap, target_chars // 2))
    paragraphs = split_paragraphs(text)
    if not paragraphs:
        return []

    chunks: list[str] = []
    current = ""

    for paragraph in paragraphs:
        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if len(candidate) <= target_chars:
            current = candidate
            continue

        if current:
            chunks.append(current.strip())
        if len(paragraph) <= target_chars:
            current = paragraph
            continue

        start = 0
        while start < len(paragraph):
            end = min(len(paragraph), start + target_chars)
            piece = paragraph[start:end].strip()
            if piece:
                chunks.append(piece)
            if end >= len(paragraph):
                break
            start = max(end - overlap_chars, start + 1)
        current = ""

    if current.strip():
        chunks.append(current.strip())

    return chunks


def build_search_chunks(
    document: SearchDocumentRecord,
    settings: Settings,
) -> list[SearchChunkRecord]:
    """Turn a search document into indexable chunks."""

    chunks = chunk_text(document.content, settings)
    return [
        SearchChunkRecord(
            id=f"{document.id}:chunk:{index}",
            document_id=document.id,
            chunk_index=index,
            content=chunk,
            token_estimate=estimate_tokens(chunk),
            metadata={
                "sourceDocumentId": document.source_document_id,
                "source": document.source,
                "category": document.category,
                "tags": list(document.tags),
                "memberIds": list(document.member_ids),
                "assetTickers": list(document.asset_tickers),
            },
        )
        for index, chunk in enumerate(chunks)
    ]
