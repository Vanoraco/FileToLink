# Thunder/utils/stream_guard.py

"""Timeout guard for the streaming path.

A stalled stream must never pin a client slot forever. ``pump_chunks`` bounds
both the wait for the next upstream chunk (Telegram) and the downstream write
(slow/half-open HTTP client) with the same idle timeout. When either side stops
making progress the ``asyncio.TimeoutError`` propagates so the caller can drop
the connection and release whatever the stream was holding.
"""

import asyncio
from typing import AsyncIterator, Awaitable, Callable


async def pump_chunks(
    source: AsyncIterator[bytes],
    sink: Callable[[bytes], Awaitable[bool]],
    *,
    timeout: float,
) -> None:
    """Forward chunks from ``source`` to ``sink``, aborting if either stalls.

    ``sink`` returns ``True`` to keep going, ``False`` to stop early (e.g. the
    requested range is complete). Empty chunks are skipped. Raises
    ``asyncio.TimeoutError`` if a chunk cannot be produced or delivered within
    ``timeout`` seconds.
    """
    while True:
        try:
            chunk = await asyncio.wait_for(source.__anext__(), timeout)
        except StopAsyncIteration:
            return
        if not chunk:
            continue
        if not await asyncio.wait_for(sink(chunk), timeout):
            return
