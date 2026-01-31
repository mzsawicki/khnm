import pytest
from aio_pika import Message
from aio_pika.abc import AbstractRobustConnection

from khnm.pipes import get_queue_name
from khnm.producers import make_producer
from tests.doubles import RaisingMessageSender
from tests.utils import get_queue_messages_count


async def test_producing_message_adds_it_to_queue(
    amqp_connection: AbstractRobustConnection,
    sample_message: Message,
    pipe: str = "test",
) -> None:
    async with make_producer(amqp_connection, pipe) as producer:
        await producer.publish(sample_message)
    messages_count = await get_queue_messages_count(
        amqp_connection, get_queue_name(pipe)
    )
    assert messages_count == 1


async def test_producer_channel_is_closed_properly_on_exception(
    amqp_connection: AbstractRobustConnection,
    sample_message: Message,
    pipe: str = "test",
) -> None:
    async with make_producer(
        amqp_connection, pipe, sender=RaisingMessageSender()
    ) as producer:
        with pytest.raises(Exception):
            await producer.publish(sample_message)
    assert producer.is_closed
