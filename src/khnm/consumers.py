from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, AbstractAsyncContextManager
from typing import Optional, AsyncIterator

from aio_pika.abc import (
    AbstractRobustConnection,
    AbstractChannel,
    AbstractIncomingMessage,
)

from khnm.exceptions import UpstreamPipeUnavailable
from khnm.pipes import get_queue, wait_for_pipe, get_queue_name
from khnm.time import Clock, LocalTimeClock
from khnm.types import QueueGetterT


async def consume(
    amqp_connection: AbstractRobustConnection,
    upstream_pipe: str,
    upstream_connection_max_retries: Optional[int] = None,
    upstream_connection_backoff_seconds: float = 1.0,
    upstream_queue_getter: QueueGetterT = get_queue,
    prefetch_count: Optional[int] = None,
    clock: Clock = LocalTimeClock(),
) -> AsyncGenerator[AbstractAsyncContextManager[AbstractIncomingMessage], None]:
    channel = await _connect(
        connection=amqp_connection,
        upstream_pipe=upstream_pipe,
        upstream_connection_backoff_seconds=upstream_connection_backoff_seconds,
        upstream_connection_max_retries=upstream_connection_max_retries,
        prefetch_count=prefetch_count,
        queue_getter=upstream_queue_getter,
        clock=clock,
    )
    try:
        queue = await upstream_queue_getter(channel, get_queue_name(upstream_pipe))
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                yield _handle_message(message)
    finally:
        await channel.close()


async def _connect(
    connection: AbstractRobustConnection,
    upstream_pipe: str,
    upstream_connection_backoff_seconds: float = 1.0,
    upstream_connection_max_retries: Optional[int] = None,
    prefetch_count: Optional[int] = None,
    queue_getter: QueueGetterT = get_queue,
    clock: Clock = LocalTimeClock(),
) -> AbstractChannel:
    channel = await connection.channel()
    if prefetch_count is not None:
        await channel.set_qos(prefetch_count=prefetch_count)
    success = await wait_for_pipe(
        channel,
        upstream_pipe,
        upstream_connection_backoff_seconds,
        upstream_connection_max_retries,
        clock,
        queue_getter,
    )
    if not success:
        raise UpstreamPipeUnavailable(f"Upstream unavailable: {upstream_pipe}")
    return channel


@asynccontextmanager
async def _handle_message(
    message: AbstractIncomingMessage,
) -> AsyncIterator[AbstractIncomingMessage]:
    try:
        yield message
    except Exception:
        await message.nack(requeue=True)
        raise
    else:
        await message.ack()
