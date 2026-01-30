from aio_pika import Message
from aio_pika.abc import AbstractRobustConnection

from khnm.consumers import consume
from khnm.pipes import declare_pipe, send_message, get_queue_name
from tests.utils import get_queue_messages_count


async def test_consuming_message_removes_it_from_queue(
    amqp_connection: AbstractRobustConnection,
    sample_message: Message,
    pipe: str = "test",
) -> None:
    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe)
        await send_message(channel, sample_message, pipe)
    async for message in consume(amqp_connection, pipe):
        async with message:
            break
    messages_count = await get_queue_messages_count(
        amqp_connection, get_queue_name(pipe)
    )
    assert messages_count == 0


async def test_on_exception_raised_the_message_is_requeued(
    amqp_connection: AbstractRobustConnection,
    sample_message: Message,
    pipe: str = "test",
) -> None:
    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe)
        await send_message(channel, sample_message, pipe)
    async for message in consume(amqp_connection, pipe):
        try:
            async with message:
                raise Exception()
        except Exception:
            break
    messages_count = await get_queue_messages_count(
        amqp_connection, get_queue_name(pipe)
    )
    assert messages_count == 1
