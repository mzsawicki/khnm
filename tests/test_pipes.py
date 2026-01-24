from typing import Optional

import pytest
from aio_pika import Message
from aio_pika.abc import AbstractRobustConnection

from src.khnm.pipes import declare_pipe, send_message


@pytest.mark.parametrize("pipe_size", [None, 1, 4, 1024])
async def test_message_is_published_when_pipe_max_size_not_exceeded(
    amqp_connection: AbstractRobustConnection,
    sample_message: Message,
    pipe_size: Optional[int],
    pipe: str = "test",
) -> None:
    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe, pipe_size)
        message_sent = await send_message(channel, sample_message, pipe)
    assert message_sent


@pytest.mark.parametrize("pipe_size", [0, 1, 4, 1024])
async def test_message_is_rejected_if_pipe_max_size_exceeded(
    amqp_connection: AbstractRobustConnection,
    sample_message: Message,
    pipe_size: int,
    pipe: str = "test",
) -> None:
    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe, pipe_size)
        for _ in range(pipe_size):
            await send_message(channel, sample_message, pipe)
        overflowing_message_sent = await send_message(channel, sample_message, pipe)
    assert not overflowing_message_sent
