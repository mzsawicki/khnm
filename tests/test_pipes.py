import asyncio
from typing import Optional

import pytest
from aio_pika import Message
from aio_pika.abc import AbstractRobustConnection

from src.khnm.time import Clock
from src.khnm.pipes import declare_pipe, send_message, send_with_backoff


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


@pytest.mark.parametrize("max_retries", [0, 1, 3, 1024])
async def test_message_is_retried_until_max_retries_exceeded(
    amqp_connection: AbstractRobustConnection,
    clock: Clock,
    sample_message: Message,
    max_retries: int,
    backoff_seconds: float = 1.0,
    pipe: str = "test",
) -> None:
    start_time = clock.now()
    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe, size=0)
        await send_with_backoff(
            channel,
            send_message,
            sample_message,
            pipe,
            backoff_seconds,
            max_retries,
            clock,
        )
    end_time = clock.now()
    time_delta = end_time - start_time
    assert time_delta.total_seconds() == backoff_seconds * max_retries


async def test_zero_retries_does_not_prevent_sending(
    amqp_connection: AbstractRobustConnection,
    clock: Clock,
    sample_message: Message,
    max_retries: int = 0,
    pipe: str = "test",
) -> None:
    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe)
        success = await send_with_backoff(
            channel,
            send_message,
            sample_message,
            pipe,
            max_retries=max_retries,
            clock=clock,
        )
    assert success
