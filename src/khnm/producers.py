from contextlib import asynccontextmanager
from typing import Optional, Protocol, AsyncGenerator

from aio_pika import Message
from aio_pika.abc import AbstractRobustConnection, AbstractChannel

from khnm.pipes import declare_pipe, send_with_backoff, send_message
from khnm.time import Clock, LocalTimeClock
from khnm.types import SenderT


class Producer(Protocol):
    async def publish(self, message: Message) -> None: ...
    @property
    def is_closed(self) -> bool: ...


class AmqpProducer(Producer):
    def __init__(
        self,
        channel: AbstractChannel,
        pipe: str,
        backoff_seconds: float = 0.1,
        max_retries: Optional[int] = None,
        exponential_backoff: bool = False,
        max_backoff_seconds: Optional[float] = None,
        apply_jitter: bool = False,
        persistent_messages: bool = False,
        clock: Clock = LocalTimeClock(),
        sender: SenderT = send_message,
    ) -> None:
        self._channel = channel
        self._pipe = pipe
        self._backoff_seconds = backoff_seconds
        self._max_retries = max_retries
        self._exponential_backoff = exponential_backoff
        self._max_backoff_seconds = max_backoff_seconds
        self._apply_jitter = apply_jitter
        self._persistent_messages = persistent_messages
        self._clock = clock
        self._sender = sender

    async def publish(self, message: Message) -> None:
        await send_with_backoff(
            channel=self._channel,
            sender=self._sender,
            message=message,
            pipe=self._pipe,
            backoff_seconds=self._backoff_seconds,
            max_retries=self._max_retries,
            exponential_backoff=self._exponential_backoff,
            max_backoff_seconds=self._max_backoff_seconds,
            apply_jitter=self._apply_jitter,
            persistent=self._persistent_messages,
            clock=self._clock,
        )

    @property
    def is_closed(self) -> bool:
        return self._channel.is_closed


@asynccontextmanager
async def make_producer(
    amqp_connection: AbstractRobustConnection,
    pipe: str,
    size: Optional[int] = None,
    durable: bool = False,
    backoff_seconds: float = 0.1,
    max_retries: Optional[int] = None,
    exponential_backoff: bool = False,
    max_backoff_seconds: Optional[float] = None,
    apply_jitter: bool = False,
    clock: Clock = LocalTimeClock(),
    sender: SenderT = send_message,
) -> AsyncGenerator[Producer, None]:
    channel = await amqp_connection.channel()
    await declare_pipe(channel=channel, name=pipe, size=size, durable=durable)
    try:
        yield AmqpProducer(
            channel=channel,
            pipe=pipe,
            backoff_seconds=backoff_seconds,
            max_retries=max_retries,
            exponential_backoff=exponential_backoff,
            max_backoff_seconds=max_backoff_seconds,
            apply_jitter=apply_jitter,
            clock=clock,
            sender=sender,
        )
    finally:
        await channel.close()
