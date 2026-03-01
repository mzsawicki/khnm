import asyncio
import datetime
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime.datetime: ...

    async def sleep(self, time_seconds: float) -> None: ...


class UtcClock(Clock):
    def now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc)

    async def sleep(self, time_seconds: float) -> None:
        await asyncio.sleep(time_seconds)
