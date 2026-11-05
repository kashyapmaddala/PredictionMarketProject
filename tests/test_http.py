"""HTTP retries: 429, 5xx and network errors are retried; other errors are not."""
import asyncio

import httpx
import pytest

from pipeline import http


def client(responses, calls):
    def handler(request):
        calls.append(request)
        item = responses[min(len(calls) - 1, len(responses) - 1)]
        if isinstance(item, Exception):
            raise item
        return item
    return handler


def test_retries_then_succeeds():
    calls, waits = [], []
    responses = [httpx.Response(503), httpx.ConnectError("down"), httpx.Response(429, headers={"Retry-After": "7"}),
                 httpx.Response(200, text="ok")]
    with httpx.Client(transport=httpx.MockTransport(client(responses, calls))) as c:
        reply = http.request(c, "GET", "http://x/y", sleep=waits.append)
    assert reply.text == "ok" and len(calls) == 4
    assert waits[2] == 7.0  # Retry-After honoured
    assert 0.75 <= waits[0] <= 1.25


def test_gives_up_after_the_last_attempt_and_on_client_errors():
    calls = []
    with httpx.Client(transport=httpx.MockTransport(client([httpx.Response(500)], calls))) as c:
        with pytest.raises(http.GiveUp, match="gave up after 3 attempts"):
            http.request(c, "GET", "http://x", retries=3, sleep=lambda _s: None)
    assert len(calls) == 3
    calls = []
    with httpx.Client(transport=httpx.MockTransport(client([httpx.Response(404)], calls))) as c:
        with pytest.raises(http.GiveUp, match="HTTP 404"):
            http.request(c, "GET", "http://x", sleep=lambda _s: None)
    assert len(calls) == 1


def test_async_version():
    async def main():
        calls = []
        responses = [httpx.ReadTimeout("slow"), httpx.Response(502), httpx.Response(200, text="ok")]
        async with httpx.AsyncClient(transport=httpx.MockTransport(client(responses, calls))) as c:
            reply = await http.arequest(c, "POST", "http://x", sleep=lambda _s: asyncio.sleep(0))
            assert reply.text == "ok"
        calls.clear()
        async with httpx.AsyncClient(transport=httpx.MockTransport(client([httpx.Response(500)], calls))) as c:
            with pytest.raises(http.GiveUp, match="gave up after 2 attempts"):
                await http.arequest(c, "POST", "http://x", retries=2, sleep=lambda _s: asyncio.sleep(0))
        calls.clear()
        async with httpx.AsyncClient(transport=httpx.MockTransport(client([httpx.Response(400)], calls))) as c:
            with pytest.raises(http.GiveUp, match="HTTP 400"):
                await http.arequest(c, "POST", "http://x")
    asyncio.run(main())


def test_backoff_is_capped():
    assert http.backoff(20, rng=lambda: 0.5) == http.HTTP_BACKOFF_MAX
    assert http.backoff(0, httpx.Response(429, headers={"Retry-After": "99999"})) == http.HTTP_BACKOFF_MAX
