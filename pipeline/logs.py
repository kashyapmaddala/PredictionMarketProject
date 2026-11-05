"""Logging for the unattended scripts: UTC timestamps and levels on every line.

    2026-11-05 14:36:05Z INFO collector: 1/1 connections up | 7,612 messages saved ...

setup() is called by the command-line scripts, never on import.
"""
from __future__ import annotations

import logging
import sys
import time

FORMAT = "%(asctime)sZ %(levelname)s %(name)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class UTCFormatter(logging.Formatter):
    converter = time.gmtime


def setup(level: int = logging.INFO, stream=None) -> None:
    """Send log lines to stderr (or `stream`). Safe to call more than once."""
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(UTCFormatter(FORMAT, DATE_FORMAT))
    root = logging.getLogger()
    for old in list(root.handlers):
        if getattr(old, "_pipeline", False):
            root.removeHandler(old)
    handler._pipeline = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(level)
    # httpx logs every request at INFO; keep the log readable
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
