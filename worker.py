import asyncio
import time
from collections import deque
from typing import List

from models import ChatMessage

recent_buf: deque[ChatMessage] = deque()
write_queue: asyncio.Queue[ChatMessage] = asyncio.Queue()

_RECENT_WINDOW = 60  # seconds — must be >> batch interval (0.2s)


def buf_append(msg: ChatMessage) -> None:
    recent_buf.append(msg)


def buf_snapshot() -> List[ChatMessage]:
    """Atomic point-in-time snapshot. Safe: no yields, asyncio can't interleave."""
    return list(recent_buf)


def _trim() -> None:
    cutoff = time.time() - _RECENT_WINDOW
    while recent_buf and recent_buf[0].ts < cutoff:
        recent_buf.popleft()


async def batch_writer() -> None:
    """Drains write_queue and bulk-inserts into Postgres every 200ms."""
    from db import bulk_insert
    while True:
        await asyncio.sleep(0.2)
        batch: List[ChatMessage] = []
        while True:
            try:
                batch.append(write_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if batch:
            try:
                await bulk_insert(batch)
            except Exception as e:
                print(f"[batch_writer] {e}")


async def buf_trimmer() -> None:
    """Evicts entries older than _RECENT_WINDOW from recent_buf every minute."""
    while True:
        await asyncio.sleep(60)
        _trim()
