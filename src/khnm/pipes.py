import random
from typing import Optional

from aio_pika import Message
from aio_pika.abc import ExchangeType, AbstractChannel
from aiormq import DeliveryError

from khnm.types import SenderT
from khnm.time import Clock, LocalTimeClock
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
    channel: AbstractChannel, message: Message, pipe: str
) -> SuccessT:
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
    clock: Clock = LocalTimeClock(),
) -> SuccessT:
    retries = 0
    sent = await sender(channel, message, pipe)
    if max_retries is None:
        while not sent:
            sent = await sender(channel, message, pipe)
    else:
        while not sent and retries < max_retries:
            sent = await sender(channel, message, pipe)
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
