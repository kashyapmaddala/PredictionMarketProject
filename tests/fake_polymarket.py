"""A local stand-in for Polymarket's market WebSocket (and its REST /books), for tests.

Each connection: waits for the subscription frame, records it, sends an initial dump (a list of
books, one per subscribed token), answers "PING" with "PONG", and then sends a price change for
one of its tokens every `tick` seconds. `behaviour` lets a test make the server misbehave:

    "normal"     as above
    "silent"     after the dump, never send anything again (not even PONG)
    "abort"      after the dump and `abort_after` changes, drop the TCP connection (no close frame)
    "malformed"  send a few broken frames (text that is not JSON, a binary frame) then carry on
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import time

from websockets.asyncio.server import serve


def book(token, stamp):
    return {"event_type": "book", "market": "0xm", "asset_id": token, "timestamp": str(stamp),
            "bids": [{"price": "0.40", "size": "10"}], "asks": [{"price": "0.60", "size": "20"}],
            "tick_size": "0.01", "hash": "h"}


class FakePolymarket:
    def __init__(self, behaviour="normal", tick=0.05, abort_after=3, first_behaviour_only=True):
        self.behaviour = behaviour
        self.tick = tick
        self.abort_after = abort_after
        self.first_behaviour_only = first_behaviour_only  # later connections behave normally
        self.subscriptions: list[dict] = []
        self.pings = 0
        self.connections = 0
        self.server = None
        self.url = None

    async def __aenter__(self):
        self.server = await serve(self.handler, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        self.url = f"ws://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *exc):
        self.server.close()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.server.wait_closed(), 2)

    async def handler(self, ws):
        self.connections += 1
        behaviour = self.behaviour if (self.connections == 1 or not self.first_behaviour_only) else "normal"
        request = json.loads(await ws.recv())
        self.subscriptions.append(request)
        tokens = request["assets_ids"]
        stamp = int(time.time() * 1000)
        await ws.send(json.dumps([book(t, stamp) for t in tokens]))
        silent = behaviour == "silent"
        sent = 0

        async def answer_pings():
            async for frame in ws:
                if frame == "PING":
                    self.pings += 1
                    if not silent:
                        await ws.send("PONG")

        pinger = asyncio.create_task(answer_pings())
        try:
            if behaviour == "malformed":
                await ws.send("INVALID OPERATION")
                await ws.send(b"\x00\x01binary")
            while True:
                await asyncio.sleep(self.tick)
                if silent:
                    continue
                sent += 1
                token = tokens[sent % len(tokens)]
                size = str(sent)
                change = {"event_type": "price_change", "market": "0xm", "timestamp": str(int(time.time() * 1000)),
                          "price_changes": [{"asset_id": token, "price": "0.40", "size": size, "side": "BUY",
                                             "hash": "x", "best_bid": "0.40", "best_ask": "0.60"}]}
                await ws.send(json.dumps(change))
                if behaviour == "abort" and sent >= self.abort_after:
                    ws.transport.abort()  # the TCP connection just dies: no close frame
                    return
        except Exception:  # noqa: BLE001 (the client went away)
            return
        finally:
            pinger.cancel()
