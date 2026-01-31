import datetime
import decimal
import uuid
from decimal import Decimal

import pydantic
from aio_pika import Message
from aio_pika.abc import AbstractChannel, AbstractQueue
from aiormq import ChannelNotFoundEntity

from khnm.time import Clock
from khnm.types import SuccessT, CallbackOutputT, CallbackInputT


class FakeClock(Clock):
    def __init__(self, start_time: datetime.datetime) -> None:
        self.time = start_time

    def now(self) -> datetime.datetime:
        return self.time

    async def sleep(self, time_seconds: float) -> None:
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


class RaisingMessageSender:
    async def __call__(
        self, channel: AbstractChannel, message: Message, pipe: str
    ) -> SuccessT:
        raise Exception()


class FailingQueueGetter:
    def __init__(self, fails_count: int) -> None:
        self._fails_count = fails_count
        self._current_fails = 0

    async def __call__(self, channel: AbstractChannel, queue: str) -> AbstractQueue:
        if self._current_fails < self._fails_count:
            self._current_fails += 1
            raise ChannelNotFoundEntity()
        return await channel.get_queue(queue)


class SampleNestedObject(pydantic.BaseModel):
    text: str = "Nested object"


class SampleDataObject(pydantic.BaseModel):
    guid: uuid.UUID = uuid.uuid4()
    text: str = "Hello World"
    time: datetime.datetime = datetime.datetime(2026, 1, 1, 12, 34, 56)
    integer: int = 1
    float_: float = 3.14
    decimal_: Decimal = Decimal(1.234)
    obj: SampleNestedObject = SampleNestedObject()


class SyncProcessorSpy:
    def __init__(self):
        self._calls_count = 0

    def __call__(self, obj: CallbackInputT) -> CallbackOutputT:
        self._calls_count += 1
        return SampleDataObject()

    @property
    def calls_count(self) -> int:
        return self._calls_count
