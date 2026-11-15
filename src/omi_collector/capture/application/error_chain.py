"""Bounded, address-redacted summaries of exception cause chains."""

from __future__ import annotations

import re

from ...config import DEFAULT_CONFIG

_BLE_ADDRESS = re.compile(r"(?i)(?<![0-9a-f])(?:[0-9a-f]{2}[:_-]){5}[0-9a-f]{2}(?![0-9a-f])")


def error_chain_message(error: BaseException) -> str:
    """Summarize an exception and its causes without raw Bluetooth addresses."""
    summaries: list[str] = []
    seen_exceptions: set[int] = set()
    seen_summaries: set[str] = set()
    cause: BaseException | None = error
    while cause is not None and len(summaries) < DEFAULT_CONFIG.observability.max_error_chain_entries:
        if id(cause) in seen_exceptions:
            break
        seen_exceptions.add(id(cause))
        summary = _bounded_error_summary(cause)
        if summary not in seen_summaries:
            summaries.append(summary)
            seen_summaries.add(summary)
        cause = cause.__cause__
    return " <- ".join(summaries)


def _bounded_error_summary(error: BaseException) -> str:
    max_chars = DEFAULT_CONFIG.observability.max_error_entry_chars
    type_name = type(error).__name__[: max_chars - 2]
    message_limit = max_chars - len(type_name) - 2
    message = _BLE_ADDRESS.sub("[BLE address]", str(error))
    return f"{type_name}: {message[:message_limit]}"
