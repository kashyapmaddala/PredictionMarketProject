"""Turn Polymarket's raw messages into typed events, checking every field.

Polymarket's market channel sends JSON text frames. One frame holds one event or a list of them
(the first frame after subscribing is a list with a full book per token). Event types seen in
2026-11 data: book, price_change, last_trade_price, tick_size_change. Anything else, and anything
that fails a check below, becomes an Unsupported event (never silently dropped): the table builder
saves those to the unsupported_events table and the validator counts them.

Numbers arrive as decimal strings ("0.981", "1743.67"). They are parsed to Decimal, never float,
and must fit the clean tables' columns: at most SCALE decimals and below 10**(PRECISION-SCALE).
Prices must be between 0 and 1. A value that does not fit makes its event Unsupported, so the
build never rounds silently.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

# Clean-table number format (see pipeline/polymarket/schemas.py for the reasoning)
PRECISION = 18
SCALE = 6
LIMIT = Decimal(10) ** (PRECISION - SCALE)
_QUANTUM = Decimal(1).scaleb(-SCALE)

SIDES = {"BUY": "bid", "SELL": "ask"}


class BadField(ValueError):
    """A field is missing or does not pass its check."""


def number(value, name: str, *, price: bool = False, required: bool = True) -> Decimal | None:
    """An exact decimal from a string (or int). Raises BadField if it is not a valid, fitting number."""
    if value is None or value == "":
        if required:
            raise BadField(f"{name} missing")
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise BadField(f"{name} is {type(value).__name__}, expected a decimal string")
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError):
        raise BadField(f"{name} {value!r} is not a number") from None
    if not result.is_finite() or result < 0:
        raise BadField(f"{name} {value!r} is not a finite non-negative number")
    if price and result > 1:
        raise BadField(f"{name} {value!r} is above 1")
    if result >= LIMIT:
        raise BadField(f"{name} {value!r} is too large for the tables")
    if result.as_tuple().exponent < -SCALE and result != result.quantize(_QUANTUM):
        raise BadField(f"{name} {value!r} has more than {SCALE} decimals")
    return result


def stamp(value, name: str = "timestamp", required: bool = True) -> int | None:
    """Exchange time in milliseconds since 1970 (UTC)."""
    if value is None or value == "":
        if required:
            raise BadField(f"{name} missing")
        return None
    if isinstance(value, bool):
        raise BadField(f"{name} is a boolean")
    text = str(value)
    if not text.isdigit() or len(text) > 15:
        raise BadField(f"{name} {value!r} is not a millisecond timestamp")
    return int(text)


def text_field(event: dict, name: str, required: bool = True) -> str | None:
    value = event.get(name)
    if value is None or value == "":
        if required:
            raise BadField(f"{name} missing")
        return None
    if not isinstance(value, str):
        raise BadField(f"{name} is {type(value).__name__}, expected text")
    return value


# ---------- the events ----------

@dataclass(slots=True)
class BookEvent:
    """A full order book for one token (live "book" message, or one entry of a REST reply)."""

    token: str
    market: str | None
    stamp: int
    bids: dict  # {price: size}, sizes > 0
    asks: dict
    bid_levels: list  # [(price, size)] in the order received (for the snapshot table)
    ask_levels: list
    tick: Decimal | None
    hash: str | None
    kind: str = "book"


@dataclass(slots=True)
class Change:
    """One level of one token's book now has this total size."""

    token: str
    side: str  # "bid" or "ask"
    price: Decimal
    size: Decimal
    best_bid: Decimal | None  # Polymarket's best prices right after this change
    best_ask: Decimal | None
    hash: str | None
    index: int  # position inside its price_change message


@dataclass(slots=True)
class PriceChange:
    market: str | None
    stamp: int
    changes: list = field(default_factory=list)
    kind: str = "price_change"


@dataclass(slots=True)
class Trade:
    token: str
    market: str | None
    stamp: int
    price: Decimal
    size: Decimal | None
    side: str | None  # "buy" or "sell" as reported by Polymarket
    fee_rate_bps: Decimal | None
    transaction_hash: str | None
    kind: str = "last_trade_price"


@dataclass(slots=True)
class TickSizeChange:
    token: str
    market: str | None
    stamp: int | None
    old: Decimal | None
    new: Decimal
    kind: str = "tick_size_change"


@dataclass(slots=True)
class Unsupported:
    """Something we could not use. `payload` is the exact text, so nothing is lost."""

    kind: str  # the event_type, or "not_json" / "not_an_object" / "rest_books"
    reason: str
    payload: str


# ---------- parsing ----------

def parse_levels(levels, name: str) -> tuple[dict, list]:
    if not isinstance(levels, list):
        raise BadField(f"{name} is not a list")
    book, ordered = {}, []
    for level in levels:
        if not isinstance(level, dict):
            raise BadField(f"{name} has a level that is not an object")
        price = number(level.get("price"), f"{name} price", price=True)
        size = number(level.get("size"), f"{name} size")
        if price in book:
            raise BadField(f"{name} lists price {price} twice")
        ordered.append((price, size))
        if size > 0:
            book[price] = size
    return book, ordered


def parse_book(event: dict) -> BookEvent:
    bids, bid_levels = parse_levels(event.get("bids"), "bids")
    asks, ask_levels = parse_levels(event.get("asks"), "asks")
    return BookEvent(
        token=text_field(event, "asset_id"), market=text_field(event, "market", required=False),
        stamp=stamp(event.get("timestamp")), bids=bids, asks=asks,
        bid_levels=bid_levels, ask_levels=ask_levels,
        tick=number(event.get("tick_size"), "tick_size", price=True, required=False),
        hash=text_field(event, "hash", required=False))


def parse_price_change(event: dict) -> PriceChange:
    changes = event.get("price_changes")
    if not isinstance(changes, list) or not changes:
        raise BadField("price_changes is not a non-empty list")
    out = PriceChange(market=text_field(event, "market", required=False), stamp=stamp(event.get("timestamp")))
    for index, change in enumerate(changes):
        if not isinstance(change, dict):
            raise BadField("a price change is not an object")
        side = SIDES.get(change.get("side"))
        if side is None:
            raise BadField(f"side {change.get('side')!r} is not BUY or SELL")
        out.changes.append(Change(
            token=text_field(change, "asset_id"), side=side,
            price=number(change.get("price"), "price", price=True), size=number(change.get("size"), "size"),
            best_bid=number(change.get("best_bid"), "best_bid", price=True, required=False),
            best_ask=number(change.get("best_ask"), "best_ask", price=True, required=False),
            hash=text_field(change, "hash", required=False), index=index))
    return out


def parse_trade(event: dict) -> Trade:
    side = event.get("side")
    if side not in (None, "", "BUY", "SELL"):
        raise BadField(f"side {side!r} is not BUY or SELL")
    return Trade(
        token=text_field(event, "asset_id"), market=text_field(event, "market", required=False),
        stamp=stamp(event.get("timestamp")), price=number(event.get("price"), "price", price=True),
        size=number(event.get("size"), "size", required=False),
        side=side.lower() if side else None,
        fee_rate_bps=number(event.get("fee_rate_bps"), "fee_rate_bps", required=False),
        transaction_hash=text_field(event, "transaction_hash", required=False))


def parse_tick_size_change(event: dict) -> TickSizeChange:
    return TickSizeChange(
        token=text_field(event, "asset_id"), market=text_field(event, "market", required=False),
        stamp=stamp(event.get("timestamp"), required=False),
        old=number(event.get("old_tick_size"), "old_tick_size", price=True, required=False),
        new=number(event.get("new_tick_size"), "new_tick_size", price=True))


PARSERS = {
    "book": parse_book,
    "price_change": parse_price_change,
    "last_trade_price": parse_trade,
    "tick_size_change": parse_tick_size_change,
}


def event_type(event: dict) -> str | None:
    kind = event.get("event_type")
    if kind is None and "price_changes" in event:
        return "price_change"  # an early message format had no event_type on price changes
    return kind if isinstance(kind, str) else None


def parse_event(event) -> object:
    if not isinstance(event, dict):
        return Unsupported("not_an_object", "event is not a JSON object", json.dumps(event, sort_keys=True))
    kind = event_type(event)
    parser = PARSERS.get(kind)
    if parser is None:
        return Unsupported(str(kind), "unknown event type", json.dumps(event, sort_keys=True))
    try:
        return parser(event)
    except BadField as problem:
        return Unsupported(kind, str(problem), json.dumps(event, sort_keys=True))


def parse_message(raw) -> list:
    """One saved live message (the exact text) -> a list of events, in order."""
    if not isinstance(raw, str):
        return [Unsupported("not_json", "message is not text", json.dumps(raw, sort_keys=True))]
    try:
        data = json.loads(raw)
    except ValueError:
        return [Unsupported("not_json", "message is not JSON", raw)]
    items = data if isinstance(data, list) else [data]
    return [parse_event(item) for item in items]


def parse_rest_books(raw) -> list:
    """A saved REST /books reply -> BookEvents (and Unsupported for entries that fail checks)."""
    if not isinstance(raw, str):
        return [Unsupported("rest_books", "reply is not text", json.dumps(raw, sort_keys=True))]
    try:
        data = json.loads(raw)
    except ValueError:
        return [Unsupported("rest_books", "reply is not JSON", raw)]
    if not isinstance(data, list):
        return [Unsupported("rest_books", "reply is not a list", raw)]
    out = []
    for item in data:
        if not isinstance(item, dict):
            out.append(Unsupported("rest_books", "entry is not an object", json.dumps(item, sort_keys=True)))
            continue
        try:
            out.append(parse_book(item))
        except BadField as problem:
            out.append(Unsupported("rest_books", str(problem), json.dumps(item, sort_keys=True)))
    return out
