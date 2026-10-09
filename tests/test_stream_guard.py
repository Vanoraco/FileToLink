"""Self-check for the streaming timeout guard. Run: python3 tests/test_stream_guard.py"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Thunder.utils.stream_guard import pump_chunks


async def _gen(chunks, delays=()):
    for i, chunk in enumerate(chunks):
        if i < len(delays) and delays[i]:
            await asyncio.sleep(delays[i])
        yield chunk


async def _expect_timeout(coro):
    try:
        await coro
    except asyncio.TimeoutError:
        return True
    raise AssertionError("expected asyncio.TimeoutError")


async def main():
    # normal: forwards chunks, sink stops early on False
    seen = []

    async def sink(chunk):
        seen.append(chunk)
        return len(seen) < 2

    await pump_chunks(_gen([b"a", b"b", b"c"]), sink, timeout=1)
    assert seen == [b"a", b"b"], seen

    # upstream stall (Telegram stops producing) -> timeout
    async def keep(_):
        return True

    await _expect_timeout(pump_chunks(_gen([b"a", b"stall"], delays=(0, 5)),
                                      keep, timeout=0.1))

    # downstream stall (client stops reading) -> timeout
    async def slow_sink(_):
        await asyncio.sleep(5)
        return True

    await _expect_timeout(pump_chunks(_gen([b"a"]), slow_sink, timeout=0.1))

    print("stream_guard tests passed")


if __name__ == "__main__":
    asyncio.run(main())
