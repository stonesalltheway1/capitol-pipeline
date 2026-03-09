"""Embedding providers for Capitol Pipeline search indexing."""

from __future__ import annotations

from abc import ABC, abstractmethod

import httpx

from capitol_pipeline.config import Settings


class BaseEmbedder(ABC):
    """Abstract embedding interface."""

    dimensions: int

    @abstractmethod
    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts."""


class NoopEmbedder(BaseEmbedder):
    """Embedding provider that intentionally does nothing."""

    def __init__(self, dimensions: int) -> None:
        self.dimensions = dimensions

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [[] for _ in texts]


class OpenAIEmbedder(BaseEmbedder):
    """OpenAI embeddings via the REST API."""

    def __init__(self, settings: Settings) -> None:
        api_key = settings.openai_api_key
        if not api_key:
            raise RuntimeError("CAPITOL_OPENAI_API_KEY is required for OpenAI embeddings.")
        self.api_key = api_key
        self.base_url = settings.openai_base_url.rstrip("/")
        self.model = settings.openai_embedding_model
        self.dimensions = settings.openai_embedding_dimensions
        self.batch_size = settings.embedding_batch_size or 32

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        embeddings: list[list[float]] = []
        with httpx.Client(
            base_url=self.base_url,
            timeout=60.0,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        ) as client:
            for start in range(0, len(texts), self.batch_size):
                batch = texts[start : start + self.batch_size]
                response = client.post(
                    "/embeddings",
                    json={
                        "input": batch,
                        "model": self.model,
                        "dimensions": self.dimensions,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                data = payload.get("data") or []
                embeddings.extend(item["embedding"] for item in data)
        return embeddings


def get_embedder(settings: Settings) -> BaseEmbedder:
    """Return the configured embedding provider."""

    if settings.embedding_provider == "openai":
        return OpenAIEmbedder(settings)
    return NoopEmbedder(settings.embedding_dimensions)
