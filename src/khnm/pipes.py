import random
from typing import Optional

from aio_pika import Message, DeliveryMode
from aio_pika.abc import ExchangeType, AbstractChannel, AbstractQueue
from aiormq import DeliveryError, ChannelNotFoundEntity

from khnm.types import SenderT, QueueGetterT
from khnm.time import Clock, UtcClock
from khnm.types import SuccessT


def get_exchange_name(pipe_name: str) -> str:
    return f"khnm.ex.{pipe_name}"


def get_queue_name(pipe_name: str) -> str:
    return f"khnm.q.{pipe_name}"


async def declare_pipe(
    channel: AbstractChannel,
    name: str,
    size: Optional[int] = None,
    durable: bool = False,
) -> None:
    exchange_name = get_exchange_name(name)
    queue_name = get_queue_name(name)
    exchange = await channel.declare_exchange(exchange_name, ExchangeType.DIRECT)
    if size is not None:
        queue = await channel.declare_queue(
            name=queue_name,
            durable=durable,
            arguments={"x-max-length": size, "x-overflow": "reject-publish"},
        )
    else:
        queue = await channel.declare_queue(name=queue_name, durable=durable)
    await queue.bind(exchange, routing_key=queue_name)


async def send_message(
    channel: AbstractChannel, message: Message, pipe: str, persistent: bool = False
) -> SuccessT:
    if persistent:
        message.delivery_mode = DeliveryMode.PERSISTENT
    exchange = await channel.get_exchange(get_exchange_name(pipe), ensure=False)
    try:
        await exchange.publish(message, routing_key=get_queue_name(pipe))
    except DeliveryError as e:
        if e.frame.name == "Basic.Nack":
            return False
        raise
    return True


async def send_with_backoff(
    channel: AbstractChannel,
    sender: SenderT,
    message: Message,
    pipe: str,
    backoff_seconds: float = 0.1,
    max_retries: Optional[int] = None,
    exponential_backoff: bool = False,
    max_backoff_seconds: Optional[float] = None,
    apply_jitter: bool = False,
    persistent: bool = False,
    clock: Clock = UtcClock(),
) -> SuccessT:
    retries = 0
    sent = await sender(channel, message, pipe, persistent)
    if max_retries is None:
        while not sent:
            wait_time_seconds = (
                backoff_seconds
                if not exponential_backoff
                else backoff_seconds * pow(2, retries)
            )
            if (
                max_backoff_seconds is not None
                and wait_time_seconds > max_backoff_seconds
            ):
                wait_time_seconds = max_backoff_seconds
            if apply_jitter:
                wait_time_seconds = random.uniform(0, wait_time_seconds)
            await clock.sleep(wait_time_seconds)
            sent = await sender(channel, message, pipe, persistent)
            retries += 1
    else:
        while not sent and retries < max_retries:
            sent = await sender(channel, message, pipe, persistent)
            if not sent:
                wait_time_seconds = (
                    backoff_seconds
                    if not exponential_backoff
                    else backoff_seconds * pow(2, retries)
                )
                if (
                    max_backoff_seconds is not None
                    and wait_time_seconds > max_backoff_seconds
                ):
                    wait_time_seconds = max_backoff_seconds
                if apply_jitter:
                    wait_time_seconds = random.uniform(0, wait_time_seconds)
                await clock.sleep(wait_time_seconds)
                retries += 1
    return sent


async def get_queue(channel: AbstractChannel, queue: str) -> AbstractQueue:
    return await channel.get_queue(queue)


async def wait_for_pipe(
    channel: AbstractChannel,
    pipe: str,
    backoff_seconds: float = 0.1,
    max_retries: Optional[int] = None,
    clock: Clock = UtcClock(),
    getter: QueueGetterT = get_queue,
) -> SuccessT:
    queue = get_queue_name(pipe)
    if max_retries is None:
        return await _wait_for_queue_infinitely(
            channel, queue, backoff_seconds, clock, getter
        )
    elif max_retries == 0:
        return await _check_for_queue_once(channel, queue, getter)
    else:
        return await _wait_for_queue_with_backoff(
            channel, queue, max_retries, backoff_seconds, clock, getter
        )


async def _wait_for_queue_infinitely(
    channel: AbstractChannel,
    queue: str,
    backoff_seconds: float,
    clock: Clock,
    getter: QueueGetterT,
) -> SuccessT:
    while True:
        try:
            await getter(channel, queue)
            return True
        except ChannelNotFoundEntity:
            await clock.sleep(backoff_seconds)


async def _check_for_queue_once(
    channel: AbstractChannel, queue: str, getter: QueueGetterT
) -> SuccessT:
    try:
        await getter(channel, queue)
    except ChannelNotFoundEntity:
        return False
    return True


async def _wait_for_queue_with_backoff(
    channel: AbstractChannel,
    queue: str,
    max_retries: int,
    backoff_seconds: float,
    clock: Clock,
    getter: QueueGetterT,
) -> SuccessT:
    success = False
    retries = 0
    while not success and retries < max_retries:
        try:
            await getter(channel, queue)
            success = True
        except ChannelNotFoundEntity:
            retries += 1
            await clock.sleep(backoff_seconds)
    return success
