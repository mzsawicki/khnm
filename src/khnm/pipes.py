from typing import Optional

from aio_pika import Message
from aio_pika.abc import ExchangeType, AbstractChannel
from aiormq import DeliveryError

from src.khnm.time import Clock, LocalTimeClock
from src.khnm.types import Success


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
) -> Success:
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
    message: Message,
    pipe: str,
    backoff_seconds: float = 0.1,
    max_retries: int = 0,
    clock: Clock = LocalTimeClock(),
) -> Success:
    sent = False
    retries = 0
    while not sent and retries < max_retries:
        sent = await send_message(channel, message, pipe)
        if not sent:
            await clock.sleep(backoff_seconds)
            retries += 1
    return sent
