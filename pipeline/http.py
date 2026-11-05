"""HTTP requests that retry the way a 24/7 job needs.

Retries on: 429 (too many requests), any 5xx, timeouts and network errors. Waits 1, 2, 4 ...
seconds (plus a little randomness so several clients don't retry in step), at most
HTTP_BACKOFF_MAX, and honours a Retry-After header. Other 4xx errors are not retried: asking
again will not help. After HTTP_RETRIES attempts the last error is raised.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time

import httpx

from .config import HTTP_BACKOFF_MAX, HTTP_RETRIES

log = logging.getLogger(__name__)


class GiveUp(Exception):
    """A request failed in a way that retrying will not fix, or ran out of attempts."""


def retryable(reply: httpx.Response) -> bool:
    return reply.status_code == 429 or reply.status_code >= 500


def backoff(attempt: int, reply: httpx.Response | None = None, rng=random.random) -> float:
    if reply is not None:
        after = reply.headers.get("Retry-After", "")
        if after.strip().isdigit():
            return min(float(after), HTTP_BACKOFF_MAX)
    return min(2.0 ** attempt, HTTP_BACKOFF_MAX) * (0.75 + 0.5 * rng())


def request(client: httpx.Client, method: str, url: str, retries: int = HTTP_RETRIES,
            sleep=time.sleep, **kwargs) -> httpx.Response:
    for attempt in range(retries):
        reply = None
        try:
            reply = client.request(method, url, **kwargs)
            if not retryable(reply):
                if reply.is_error:
                    raise GiveUp(f"{method} {url}: HTTP {reply.status_code}")
                return reply
            problem = f"HTTP {reply.status_code}"
        except httpx.HTTPError as error:  # timeouts, connection errors
            problem = str(error) or type(error).__name__
        if attempt == retries - 1:
            raise GiveUp(f"{method} {url}: {problem} (gave up after {retries} attempts)")
        wait = backoff(attempt, reply)
        log.warning("%s %s failed (%s), retrying in %.1fs", method, url, problem, wait)
        sleep(wait)
    raise GiveUp("unreachable")


async def arequest(client: httpx.AsyncClient, method: str, url: str, retries: int = HTTP_RETRIES,
                   sleep=asyncio.sleep, **kwargs) -> httpx.Response:
    for attempt in range(retries):
        reply = None
        try:
            reply = await client.request(method, url, **kwargs)
            if not retryable(reply):
                if reply.is_error:
                    raise GiveUp(f"{method} {url}: HTTP {reply.status_code}")
                return reply
            problem = f"HTTP {reply.status_code}"
        except httpx.HTTPError as error:
            problem = str(error) or type(error).__name__
        if attempt == retries - 1:
            raise GiveUp(f"{method} {url}: {problem} (gave up after {retries} attempts)")
        wait = backoff(attempt, reply)
        log.warning("%s %s failed (%s), retrying in %.1fs", method, url, problem, wait)
        await sleep(wait)
    raise GiveUp("unreachable")
