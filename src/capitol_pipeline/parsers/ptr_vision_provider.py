"""Model providers for the scanned-PTR vision reader.

:mod:`capitol_pipeline.parsers.ptr_vision` builds the page images, the prompt
and the JSON schema, reads every chunk twice, and reconciles the two reads.
Which model answers is this module's business.

Two providers ship:

``gemini`` (default)
    Google's Generative Language REST API, free tier. Read A and read B are
    two *different* model versions (``gemini-3.8-flash`` and
    ``gemini-3.5-flash``), because two versions genuinely disagree on hard
    filings and the reconciliation in ``ptr_vision`` is only worth having when
    the two reads are independent. Measured on the review queue: on Wied
    9115665 one version read Purchase / $1,001-$15,000 and the other Sale /
    $15,001-$50,000, and the second was right. A single read is not
    trustworthy; the disagreement is the product.

``anthropic``
    The original path, kept intact and never taken by default.

Both speak the same neutral request form, so ``ptr_vision`` never branches on
the provider:

* parts:  ``{"kind": "text", "text": str}``,
          ``{"kind": "image", "png": bytes}``,
          ``{"kind": "document", "pdf": bytes}``
* result: :class:`VisionResponse`

Gemini notes that cost real money to rediscover
--------------------------------------------------
* **Send rendered pages, never the PDF.** Handing Gemini the PDF as
  ``inline_data`` makes it rasterise at its own resolution and read every
  two-digit year one low (``11/05/25`` came back as 2024-11-05, twice, on
  Khanna 8221264). The same page rendered here at 200 DPI reads exactly.
  ``ptr_vision`` already renders; this module only refuses the document part.
* **The response schema is not JSON Schema.** ``generateContent`` takes an
  OpenAPI 3.0 subset: no ``additionalProperties``, no ``["string", "null"]``
  type unions, nullability is the ``nullable`` keyword.
  :func:`gemini_response_schema` translates ours.
* **Free tier means a request-per-minute ceiling, not a spending one.** Every
  call goes through a per-model pacer, and 429/503 back off and retry rather
  than failing the filing.
* Free-tier inputs may be used to improve Google's models. Everything sent
  here is a published federal disclosure, which is why that is acceptable;
  it is a stated decision, not an oversight.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import random
import threading
import time
from typing import Any, Protocol, TypedDict

logger = logging.getLogger(__name__)

PROVIDERS: tuple[str, ...] = ("gemini", "anthropic")
DEFAULT_PROVIDER = "gemini"

# -- Anthropic --------------------------------------------------------------

ANTHROPIC_MODEL_ID = "claude-opus-5"
ANTHROPIC_ORIENTATION_MODEL_ID = "claude-haiku-4-5"
VISION_TOOL_NAME = "record_ptr_transactions"

#: USD per MTok (input, output).
ANTHROPIC_PRICING: dict[str, tuple[float, float]] = {
    "claude-opus-5": (5.0, 25.0),
    "claude-opus-4-8": (5.0, 25.0),
    "claude-opus-4-7": (5.0, 25.0),
    "claude-opus-4-6": (5.0, 25.0),
    "claude-sonnet-5": (2.0, 10.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-haiku-4-5": (1.0, 5.0),
}

#: Family fallbacks for dated or future ids, checked by prefix in this order.
ANTHROPIC_FAMILY_PRICING: tuple[tuple[str, tuple[float, float]], ...] = (
    ("claude-opus", (5.0, 25.0)),
    ("claude-sonnet-5", (2.0, 10.0)),
    ("claude-sonnet", (3.0, 15.0)),
    ("claude-haiku", (1.0, 5.0)),
)

#: Unknown Anthropic model: assume the Opus tier so estimates err high.
ANTHROPIC_DEFAULT_PRICING: tuple[float, float] = (5.0, 25.0)

# -- Gemini -----------------------------------------------------------------

GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

#: Read A. Newest flash tier; free on the free tier.
GEMINI_MODEL_ID = "gemini-3.8-flash"
#: Read B. A different version on purpose: see the module docstring.
GEMINI_MODEL_B_ID = "gemini-3.5-flash"
#: ``gemini-2.5-flash`` is retired for new keys; do not put it back.
GEMINI_RETIRED_MODELS: frozenset[str] = frozenset({"gemini-2.5-flash", "gemini-2.5-flash-lite"})

#: Requests per minute per model. Google no longer publishes the free-tier
#: number and it is visible only in AI Studio; 10 is the conservative figure
#: the trackers report for free Flash. Override per run.
DEFAULT_GEMINI_RPM = 10.0
#: Attempts per request when the API answers 429 or 5xx.
DEFAULT_GEMINI_MAX_ATTEMPTS = 4
GEMINI_BACKOFF_BASE_SECONDS = 4.0
GEMINI_BACKOFF_MAX_SECONDS = 90.0
GEMINI_TIMEOUT_SECONDS = 300.0

#: Free of charge on the free tier, for every model this path uses.
GEMINI_PRICING: tuple[float, float] = (0.0, 0.0)

#: ``CAPITOL_PTR_VISION_EFFORT`` -> ``generationConfig.thinkingLevel``.
GEMINI_THINKING_LEVELS: dict[str, str] = {
    "low": "low",
    "medium": "medium",
    "high": "high",
    "xhigh": "high",
    "max": "high",
}

_EMPTY_USAGE: dict[str, int] = {"input": 0, "cache_read": 0, "cache_write": 0, "output": 0}


class VisionResponse(TypedDict):
    """One model answer, in the shape ``ptr_vision`` reasons about."""

    payload: dict[str, Any] | None
    text: str
    usage: dict[str, int]
    stop_reason: str | None
    detail: str | None
    model: str


class VisionProvider(Protocol):
    """What the vision reader needs from a model vendor."""

    name: str
    read_model: str
    read_model_b: str
    orientation_model: str

    def read(
        self,
        parts: list[dict[str, Any]],
        *,
        model: str,
        system: str,
        schema: dict[str, Any],
        effort: str,
        max_tokens: int,
        structured: bool,
    ) -> VisionResponse: ...

    def ask_short(
        self, parts: list[dict[str, Any]], *, model: str, system: str, max_tokens: int
    ) -> VisionResponse: ...

    def price(self, model: str) -> tuple[float, float]: ...

    def is_retryable(self, error: Exception) -> bool: ...

    def rejected_structured_output(self, error: Exception) -> bool: ...


# -- Helpers ----------------------------------------------------------------


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _env_float(name: str, default: float, *, low: float, high: float) -> float:
    raw = _env(name)
    if not raw:
        return default
    try:
        return min(high, max(low, float(raw)))
    except ValueError:
        logger.warning("ptr_vision_provider: ignoring non-numeric %s=%r", name, raw)
        return default


def resolve_provider_name() -> str:
    """The configured provider id, defaulting to the free one."""

    raw = _env("CAPITOL_PTR_VISION_PROVIDER").lower()
    if raw and raw not in PROVIDERS:
        logger.warning(
            "ptr_vision_provider: unknown CAPITOL_PTR_VISION_PROVIDER=%r; using %s",
            raw,
            DEFAULT_PROVIDER,
        )
        return DEFAULT_PROVIDER
    return raw or DEFAULT_PROVIDER


def _status_code(error: Exception) -> int:
    status = getattr(error, "status_code", None)
    if status is None:
        status = getattr(error, "status", None)
    try:
        return int(status)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


# -- Anthropic provider -----------------------------------------------------


class AnthropicProvider:
    """The original Claude path. Never selected by default."""

    name = "anthropic"

    def __init__(self, client_factory: Any) -> None:
        self._client_factory = client_factory
        self.read_model = _env("CAPITOL_PTR_VISION_MODEL") or ANTHROPIC_MODEL_ID
        self.read_model_b = self.read_model
        self.orientation_model = (
            _env("CAPITOL_PTR_VISION_ORIENTATION_MODEL") or ANTHROPIC_ORIENTATION_MODEL_ID
        )

    # -- content ---------------------------------------------------------

    @staticmethod
    def _block(part: dict[str, Any]) -> dict[str, Any]:
        kind = part.get("kind")
        if kind == "text":
            return {"type": "text", "text": str(part.get("text") or "")}
        if kind == "image":
            return {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": base64.standard_b64encode(part["png"]).decode("ascii"),
                },
            }
        if kind == "document":
            return {
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": base64.standard_b64encode(part["pdf"]).decode("ascii"),
                },
            }
        raise ValueError(f"unknown content part: {kind!r}")

    def content(self, parts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self._block(part) for part in parts]

    # -- requests --------------------------------------------------------

    def request_kwargs(
        self,
        content: list[dict[str, Any]],
        *,
        model: str,
        system: str,
        schema: dict[str, Any],
        effort: str,
        max_tokens: int,
        structured: bool,
        with_output_config: bool = True,
    ) -> dict[str, Any]:
        """The Messages request. ``with_output_config`` drops only for old SDKs."""

        kwargs: dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "system": [
                {"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}
            ],
            "thinking": {"type": "adaptive"},
            "messages": [{"role": "user", "content": content}],
        }
        if structured:
            kwargs["output_config"] = {
                "effort": effort,
                "format": {"type": "json_schema", "schema": schema},
            }
        else:
            if with_output_config:
                kwargs["output_config"] = {"effort": effort}
            kwargs["tools"] = [
                {
                    "name": VISION_TOOL_NAME,
                    "description": "Record every transaction row transcribed from the PTR pages.",
                    "strict": True,
                    "input_schema": schema,
                }
            ]
        return kwargs

    def _invoke(self, kwargs: dict[str, Any]) -> Any:
        """Prefer streaming (multi-page PTR reads are slow), else create."""

        client = self._client_factory()
        stream = getattr(getattr(client, "messages", None), "stream", None)
        if stream is not None:
            with stream(**kwargs) as active:
                return active.get_final_message()
        return client.messages.create(**kwargs)

    def read(
        self,
        parts: list[dict[str, Any]],
        *,
        model: str,
        system: str,
        schema: dict[str, Any],
        effort: str,
        max_tokens: int,
        structured: bool,
        with_output_config: bool = True,
    ) -> VisionResponse:
        kwargs = self.request_kwargs(
            self.content(parts),
            model=model,
            system=system,
            schema=schema,
            effort=effort,
            max_tokens=max_tokens,
            structured=structured,
            with_output_config=with_output_config,
        )
        message = self._invoke(kwargs)
        return self._response(message, model)

    def ask_short(
        self, parts: list[dict[str, Any]], *, model: str, system: str, max_tokens: int
    ) -> VisionResponse:
        client = self._client_factory()
        message = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": self.content(parts)}],
        )
        return self._response(message, model)

    # -- responses -------------------------------------------------------

    @staticmethod
    def _usage(usage: Any) -> dict[str, int]:
        if usage is None:
            return dict(_EMPTY_USAGE)
        return {
            "input": int(getattr(usage, "input_tokens", 0) or 0),
            "cache_read": int(getattr(usage, "cache_read_input_tokens", 0) or 0),
            "cache_write": int(getattr(usage, "cache_creation_input_tokens", 0) or 0),
            "output": int(getattr(usage, "output_tokens", 0) or 0),
        }

    @staticmethod
    def _payload(message: Any) -> dict[str, Any] | None:
        blocks = list(getattr(message, "content", None) or [])
        for block in blocks:
            if getattr(block, "type", None) == "tool_use":
                candidate = getattr(block, "input", None)
                if isinstance(candidate, dict):
                    return candidate
        for block in blocks:
            if getattr(block, "type", None) != "text":
                continue
            text = (getattr(block, "text", "") or "").strip()
            if not text:
                continue
            try:
                candidate = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict):
                return candidate
        return None

    @staticmethod
    def _text(message: Any) -> str:
        for block in list(getattr(message, "content", None) or []):
            if getattr(block, "type", None) == "text":
                return str(getattr(block, "text", "") or "")
        return ""

    def _response(self, message: Any, model: str) -> VisionResponse:
        details = getattr(message, "stop_details", None)
        category = (
            details.get("category")
            if isinstance(details, dict)
            else getattr(details, "category", None)
        )
        return VisionResponse(
            payload=self._payload(message),
            text=self._text(message),
            usage=self._usage(getattr(message, "usage", None)),
            stop_reason=getattr(message, "stop_reason", None),
            detail=str(category) if category is not None else None,
            model=model,
        )

    # -- policy ----------------------------------------------------------

    def price(self, model: str) -> tuple[float, float]:
        key = (model or "").strip().lower()
        if key in ANTHROPIC_PRICING:
            return ANTHROPIC_PRICING[key]
        for family, price in ANTHROPIC_FAMILY_PRICING:
            if key.startswith(family):
                return price
        return ANTHROPIC_DEFAULT_PRICING

    def is_retryable(self, error: Exception) -> bool:
        status = _status_code(error)
        if status == 429 or 500 <= status <= 599:
            return True
        return type(error).__name__ in {
            "RateLimitError",
            "InternalServerError",
            "APIConnectionError",
            "APITimeoutError",
            "OverloadedError",
        }

    def rejected_structured_output(self, error: Exception) -> bool:
        if _status_code(error) not in (400, 422):
            return False
        message = str(error).lower()
        return any(token in message for token in ("output_config", "json_schema", "output_format"))

    def has_credentials(self) -> bool:
        return bool(_env("ANTHROPIC_API_KEY") or _env("ANTHROPIC_AUTH_TOKEN"))

    def credentials_hint(self) -> str:
        return "anthropic credentials not configured (ANTHROPIC_API_KEY)"


# -- Gemini -----------------------------------------------------------------


class GeminiError(RuntimeError):
    """An HTTP failure from the Generative Language API."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"gemini {status_code}: {message}")
        self.status_code = status_code
        self.message = message


class _RateLimiter:
    """A minimum interval between calls, per model.

    The free tier is metered in requests per minute, so the whole cost model
    of this path is "do not go faster than N a minute". Sleeping here is
    cheaper than a 429 and a retry, and it keeps a long filing from burning
    the day's budget in its first thirty seconds.
    """

    def __init__(self, rpm: float) -> None:
        self.interval = 60.0 / rpm if rpm > 0 else 0.0
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()

    def wait(self, key: str, sleep: Any = time.sleep) -> float:
        if self.interval <= 0:
            return 0.0
        with self._lock:
            now = time.monotonic()
            earliest = self._last.get(key, 0.0) + self.interval
            delay = max(0.0, earliest - now)
            self._last[key] = now + delay
        if delay > 0:
            logger.debug("ptr_vision_provider: pacing %s for %.1fs", key, delay)
            sleep(delay)
        return delay


def gemini_response_schema(schema: Any) -> Any:
    """Translate a JSON Schema into the subset ``responseSchema`` accepts.

    Gemini's structured output is OpenAPI 3.0 shaped, not JSON Schema:
    ``additionalProperties`` is rejected, a ``["string", "null"]`` type union
    is rejected, and nullability is the ``nullable`` keyword. An
    ``anyOf: [{enum}, {type: null}]`` -- which exists in our schema only
    because Anthropic rejected a nullable enum -- collapses back into one
    nullable enum. ``propertyOrdering`` is emitted so the model fills the row
    in the order the form prints it.
    """

    if isinstance(schema, list):
        return [gemini_response_schema(entry) for entry in schema]
    if not isinstance(schema, dict):
        return schema

    out: dict[str, Any] = {}
    nullable = False

    variants = schema.get("anyOf") or schema.get("oneOf")
    if isinstance(variants, list):
        concrete = [
            variant
            for variant in variants
            if isinstance(variant, dict) and variant.get("type") != "null"
        ]
        nullable = len(concrete) != len(variants)
        if len(concrete) == 1:
            merged = {
                key: value
                for key, value in schema.items()
                if key not in ("anyOf", "oneOf")
            }
            merged.update(concrete[0])
            out = gemini_response_schema(merged)
            out["nullable"] = True
            return out
        if concrete:
            out["anyOf"] = [gemini_response_schema(variant) for variant in concrete]

    for key, value in schema.items():
        if key in ("additionalProperties", "anyOf", "oneOf", "$schema", "title"):
            continue
        if key == "type":
            if isinstance(value, list):
                concrete_types = [entry for entry in value if entry != "null"]
                nullable = nullable or len(concrete_types) != len(value)
                if not concrete_types:
                    continue
                out["type"] = concrete_types[0]
            else:
                out["type"] = value
        elif key == "properties" and isinstance(value, dict):
            out["properties"] = {
                name: gemini_response_schema(child) for name, child in value.items()
            }
            out["propertyOrdering"] = list(value)
        elif key == "items":
            out["items"] = gemini_response_schema(value)
        else:
            out[key] = value
    if nullable:
        out["nullable"] = True
    return out


class GeminiProvider:
    """Google Generative Language, free tier, over plain REST.

    ``google-genai`` is not installed on the box and this needs one POST, so
    the request is built here and sent with ``httpx`` (already a dependency).
    """

    name = "gemini"

    def __init__(self, api_key: str | None = None, rpm: float | None = None) -> None:
        self._api_key = api_key if api_key is not None else self._resolve_key()
        self.read_model = _env("CAPITOL_PTR_VISION_MODEL") or GEMINI_MODEL_ID
        self.read_model_b = _env("CAPITOL_PTR_VISION_MODEL_B") or GEMINI_MODEL_B_ID
        self.orientation_model = (
            _env("CAPITOL_PTR_VISION_ORIENTATION_MODEL") or self.read_model_b
        )
        for model in (self.read_model, self.read_model_b):
            if model in GEMINI_RETIRED_MODELS:
                logger.warning(
                    "ptr_vision_provider: %s is retired for new API keys and will 404", model
                )
        resolved_rpm = (
            rpm
            if rpm is not None
            else _env_float("CAPITOL_PTR_VISION_GEMINI_RPM", DEFAULT_GEMINI_RPM, low=0.0, high=600.0)
        )
        self.limiter = _RateLimiter(resolved_rpm)
        self.max_attempts = int(
            _env_float(
                "CAPITOL_PTR_VISION_GEMINI_MAX_ATTEMPTS",
                DEFAULT_GEMINI_MAX_ATTEMPTS,
                low=1,
                high=8,
            )
        )

    # -- credentials -----------------------------------------------------

    @staticmethod
    def _resolve_key() -> str:
        return _env("GEMINI_API_KEY") or _env("GOOGLE_API_KEY")

    def has_credentials(self) -> bool:
        return bool(self._api_key)

    def credentials_hint(self) -> str:
        return "gemini credentials not configured (GEMINI_API_KEY or GOOGLE_API_KEY)"

    # -- content ---------------------------------------------------------

    @staticmethod
    def _part(part: dict[str, Any]) -> dict[str, Any]:
        kind = part.get("kind")
        if kind == "text":
            return {"text": str(part.get("text") or "")}
        if kind == "image":
            return {
                "inlineData": {
                    "mimeType": "image/png",
                    "data": base64.standard_b64encode(part["png"]).decode("ascii"),
                }
            }
        if kind == "document":
            # Google rasterises a PDF at its own resolution and misreads
            # two-digit years one low. Rendered pages only.
            raise ValueError(
                "the gemini provider does not accept whole PDFs: render the pages first "
                "(install pymupdf, or set CAPITOL_PTR_VISION_PROVIDER=anthropic)"
            )
        raise ValueError(f"unknown content part: {kind!r}")

    def content(self, parts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self._part(part) for part in parts]

    # -- requests --------------------------------------------------------

    def request_body(
        self,
        parts: list[dict[str, Any]],
        *,
        system: str,
        schema: dict[str, Any] | None,
        effort: str,
        max_tokens: int,
    ) -> dict[str, Any]:
        generation: dict[str, Any] = {"maxOutputTokens": int(max_tokens)}
        if schema is not None:
            generation["responseMimeType"] = "application/json"
            generation["responseSchema"] = gemini_response_schema(schema)
        level = GEMINI_THINKING_LEVELS.get((effort or "").lower())
        if level:
            generation["thinkingConfig"] = {"thinkingLevel": level}
        return {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": self.content(parts)}],
            "generationConfig": generation,
        }

    def _post(self, model: str, body: dict[str, Any], *, sleep: Any = time.sleep) -> dict[str, Any]:
        """POST one request, pacing before it and backing off on 429/5xx."""

        import httpx

        url = GEMINI_ENDPOINT.format(model=model)
        headers = {"x-goog-api-key": self._api_key, "Content-Type": "application/json"}
        last: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            self.limiter.wait(model, sleep=sleep)
            try:
                response = httpx.post(
                    url, json=body, headers=headers, timeout=GEMINI_TIMEOUT_SECONDS
                )
            except Exception as error:  # noqa: BLE001 - transport, classified below
                last = error
                if attempt >= self.max_attempts:
                    break
                sleep(self._backoff_seconds(attempt, None))
                continue
            if response.status_code < 400:
                return response.json()
            detail = self._error_message(response)
            error = GeminiError(response.status_code, detail)
            last = error
            if attempt >= self.max_attempts or not self.is_retryable(error):
                break
            delay = self._backoff_seconds(attempt, self._retry_after(response, detail))
            logger.warning(
                "ptr_vision_provider: %s answered %d (%s); retrying in %.0fs (attempt %d/%d)",
                model,
                response.status_code,
                detail[:200],
                delay,
                attempt,
                self.max_attempts,
            )
            sleep(delay)
        raise last if last is not None else GeminiError(0, "no response")

    @staticmethod
    def _error_message(response: Any) -> str:
        try:
            body = response.json()
        except Exception:  # noqa: BLE001 - non-JSON error body
            return str(getattr(response, "text", ""))[:500]
        error = body.get("error") if isinstance(body, dict) else None
        if isinstance(error, dict):
            return str(error.get("message") or error)[:500]
        return json.dumps(body)[:500]

    @staticmethod
    def _retry_after(response: Any, detail: str) -> float | None:
        header = (getattr(response, "headers", None) or {}).get("retry-after")
        if header:
            try:
                return float(header)
            except (TypeError, ValueError):
                pass
        # Google puts a RetryInfo in the error details as "retryDelay": "31s".
        try:
            body = response.json()
        except Exception:  # noqa: BLE001
            return None
        error = body.get("error") if isinstance(body, dict) else None
        for entry in (error or {}).get("details") or []:
            delay = entry.get("retryDelay") if isinstance(entry, dict) else None
            if isinstance(delay, str) and delay.endswith("s"):
                try:
                    return float(delay[:-1])
                except ValueError:
                    continue
        return None

    @staticmethod
    def _backoff_seconds(attempt: int, retry_after: float | None) -> float:
        if retry_after is not None and retry_after > 0:
            return min(GEMINI_BACKOFF_MAX_SECONDS, retry_after + random.uniform(0.0, 1.0))
        delay = GEMINI_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
        return min(GEMINI_BACKOFF_MAX_SECONDS, delay + random.uniform(0.0, 1.0))

    # -- responses -------------------------------------------------------

    #: ``finishReason`` -> the reader's vocabulary.
    _STOP_REASONS: dict[str, str] = {
        "STOP": "end_turn",
        "MAX_TOKENS": "max_tokens",
        "SAFETY": "refusal",
        "RECITATION": "refusal",
        "BLOCKLIST": "refusal",
        "PROHIBITED_CONTENT": "refusal",
        "SPII": "refusal",
        "IMAGE_SAFETY": "refusal",
    }

    @classmethod
    def _response(cls, body: dict[str, Any], model: str) -> VisionResponse:
        candidates = body.get("candidates") or []
        candidate = candidates[0] if candidates else {}
        finish = str(candidate.get("finishReason") or "").upper()
        stop_reason = cls._STOP_REASONS.get(finish, finish.lower() or None)
        if not candidates and isinstance(body.get("promptFeedback"), dict):
            # The prompt itself was blocked: no candidate at all.
            stop_reason = "refusal"

        text_parts: list[str] = []
        for part in (candidate.get("content") or {}).get("parts") or []:
            if not isinstance(part, dict) or part.get("thought"):
                continue
            if isinstance(part.get("text"), str):
                text_parts.append(part["text"])
        text = "".join(text_parts).strip()

        payload: dict[str, Any] | None = None
        if text:
            payload = _loads_object(text)

        usage = body.get("usageMetadata") or {}
        normalized = {
            "input": int(usage.get("promptTokenCount") or 0),
            "cache_read": int(usage.get("cachedContentTokenCount") or 0),
            "cache_write": 0,
            # Thinking tokens are billed as output; on the free tier they cost
            # nothing but they still belong in the record.
            "output": int(usage.get("candidatesTokenCount") or 0)
            + int(usage.get("thoughtsTokenCount") or 0),
        }
        normalized["input"] = max(0, normalized["input"] - normalized["cache_read"])

        detail = None
        block = (body.get("promptFeedback") or {}).get("blockReason")
        if block:
            detail = str(block)
        elif finish and stop_reason == "refusal":
            detail = finish
        return VisionResponse(
            payload=payload,
            text=text,
            usage=normalized,
            stop_reason=stop_reason,
            detail=detail,
            model=model,
        )

    def read(
        self,
        parts: list[dict[str, Any]],
        *,
        model: str,
        system: str,
        schema: dict[str, Any],
        effort: str,
        max_tokens: int,
        structured: bool,
    ) -> VisionResponse:
        body = self.request_body(
            parts,
            system=system,
            # The downgrade for Gemini is "JSON without a schema": the prompt
            # already describes every field, and _coerce_transactions and the
            # example-row scrub validate what comes back either way.
            schema=schema if structured else None,
            effort=effort,
            max_tokens=max_tokens,
        )
        if not structured:
            body["generationConfig"]["responseMimeType"] = "application/json"
        return self._response(self._post(model, body), model)

    def ask_short(
        self, parts: list[dict[str, Any]], *, model: str, system: str, max_tokens: int
    ) -> VisionResponse:
        body = {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": self.content(parts)}],
            "generationConfig": {
                "maxOutputTokens": max(int(max_tokens), 16),
                "thinkingConfig": {"thinkingLevel": "minimal"},
            },
        }
        return self._response(self._post(model, body), model)

    # -- policy ----------------------------------------------------------

    def price(self, model: str) -> tuple[float, float]:
        return GEMINI_PRICING

    def is_retryable(self, error: Exception) -> bool:
        status = _status_code(error)
        if status == 429 or 500 <= status <= 599:
            return True
        return type(error).__name__ in {
            "ConnectError",
            "ConnectTimeout",
            "ReadTimeout",
            "WriteTimeout",
            "PoolTimeout",
            "RemoteProtocolError",
            "TimeoutException",
        }

    def rejected_structured_output(self, error: Exception) -> bool:
        if _status_code(error) not in (400, 422):
            return False
        message = str(error).lower()
        return any(
            token in message
            for token in ("responseschema", "response_schema", "schema", "responsemimetype")
        )


def _loads_object(text: str) -> dict[str, Any] | None:
    """Parse a JSON object out of a model answer.

    ``responseMimeType: application/json`` returns bare JSON, but a downgraded
    call can still wrap it in a fenced block, so the braces are located rather
    than assumed.
    """

    candidate = text.strip()
    if candidate.startswith("```"):
        candidate = candidate.strip("`")
        newline = candidate.find("\n")
        if newline != -1 and not candidate[:newline].strip().startswith("{"):
            candidate = candidate[newline + 1 :]
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        start, end = candidate.find("{"), candidate.rfind("}")
        if start == -1 or end <= start:
            return None
        try:
            parsed = json.loads(candidate[start : end + 1])
        except json.JSONDecodeError:
            return None
    return parsed if isinstance(parsed, dict) else None


# -- Factory ----------------------------------------------------------------


def build_provider(name: str, *, anthropic_client_factory: Any) -> Any:
    """Construct one provider by id."""

    if name == "anthropic":
        return AnthropicProvider(anthropic_client_factory)
    if name == "gemini":
        return GeminiProvider()
    raise ValueError(f"unknown vision provider: {name!r}")


def resolve_provider(*, anthropic_client_factory: Any) -> Any:
    """The provider ``CAPITOL_PTR_VISION_PROVIDER`` selects (default gemini)."""

    return build_provider(resolve_provider_name(), anthropic_client_factory=anthropic_client_factory)
