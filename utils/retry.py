"""Backoff for read-only requests and client initialization."""

import asyncio
import logging
from typing import Awaitable, Callable, TypeVar

import aiohttp
from hyperliquid.utils.error import ClientError, ServerError


T = TypeVar("T")
logger = logging.getLogger(__name__)


def is_transient_api_error(exc: Exception) -> bool:
    if isinstance(exc, (ClientError, ServerError)):
        return exc.status_code == 429 or 500 <= exc.status_code < 600
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status == 429 or 500 <= exc.status < 600
    return isinstance(exc, (aiohttp.ClientConnectionError, asyncio.TimeoutError))


async def retry_api_read(operation: Callable[[], Awaitable[T]], *, context: str) -> T:
    """Retry transient failures until recovery or cancellation; never use for orders."""
    delay = 5.0
    while True:
        try:
            return await operation()
        except Exception as exc:
            if not is_transient_api_error(exc):
                raise
            status = getattr(exc, "status_code", getattr(exc, "status", None))
            message = (
                f"[API-RETRY] {context}: {type(exc).__name__}"
                f"{f' HTTP {status}' if status is not None else ''}. "
                f"Sleeping {delay:.1f}s before retry."
            )
            print(message)
            logger.warning(message)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)
