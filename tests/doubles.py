import datetime

from aio_pika import Message
from aio_pika.abc import AbstractChannel

from khnm.types import SuccessT


class TestClock:
    def __init__(self, start_time: datetime.datetime) -> None:
        self.time = start_time

    def now(self) -> datetime.datetime:
        return self.time

    async def sleep(self, time_seconds: int) -> None:
        self.time += datetime.timedelta(seconds=time_seconds)


class FailingMessageSender:
    def __init__(self, fails_count: int) -> None:
        self._fails_count = fails_count
        self._current_fails = 0

    async def __call__(
        self, channel: AbstractChannel, message: Message, pipe: str
    ) -> SuccessT:
        if self._current_fails < self._fails_count:
            self._current_fails += 1
            return False
        return True
