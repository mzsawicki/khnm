from typing import Optional

import pytest
from aio_pika import Message
from aio_pika.abc import AbstractRobustConnection

from khnm.time import Clock
from khnm.pipes import declare_pipe, send_message, send_with_backoff, wait_for_pipe
from khnm.types import QueueGetterT
from tests.doubles import FailingMessageSender, FakeClock, FailingQueueGetter


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
            backoff_seconds=backoff_seconds,
            max_retries=max_retries,
            clock=clock,
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


async def test_no_max_retries_allow_infinite_backoff(
    amqp_connection: AbstractRobustConnection,
    clock: Clock,
    sample_message: Message,
    sender=FailingMessageSender(fails_count=999999),
    pipe: str = "test",
) -> None:
    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe)
        success = await send_with_backoff(
            channel, sender, sample_message, pipe, max_retries=None, clock=clock
        )
    assert success


@pytest.mark.parametrize(
    "max_retries,expected_backoff_sum",
    [
        (1, 1),
        (2, 3),
        (3, 7),
        (4, 15),
        (5, 31),
        (6, 63),
        (7, 127),
        (8, 255),
        (9, 511),
        (10, 1023),
    ],
)
async def test_exponential_backoff_waits_correct_times(
    amqp_connection: AbstractRobustConnection,
    clock: Clock,
    sample_message: Message,
    max_retries: int,
    expected_backoff_sum: float,
    initial_backoff_seconds: float = 1.0,
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
            backoff_seconds=initial_backoff_seconds,
            max_retries=max_retries,
            exponential_backoff=True,
            clock=clock,
        )
    end_time = clock.now()
    time_delta = end_time - start_time
    assert time_delta.total_seconds() == expected_backoff_sum


@pytest.mark.parametrize(
    "max_retries,expected_backoff_sum",
    [
        (1, 1),
        (2, 3),
        (3, 7),
        (4, 15),
        (5, 31),
        (6, 63),
        (7, 123),
        (8, 183),
        (9, 243),
        (10, 303),
    ],
)
async def test_max_backoff_cuts_out_exponential_backoff_time(
    amqp_connection: AbstractRobustConnection,
    clock: Clock,
    sample_message: Message,
    max_retries: int,
    expected_backoff_sum: float,
    initial_backoff_seconds: float = 1.0,
    max_backoff_seconds: float = 60,
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
            backoff_seconds=initial_backoff_seconds,
            max_retries=max_retries,
            exponential_backoff=True,
            max_backoff_seconds=max_backoff_seconds,
            clock=clock,
        )
    end_time = clock.now()
    time_delta = end_time - start_time
    assert time_delta.total_seconds() == expected_backoff_sum


@pytest.mark.parametrize(
    "max_retries,initial_backoff_seconds",
    [
        (1, 1),
        (2, 3),
        (3, 7),
        (4, 15),
        (5, 31),
        (6, 63),
        (7, 123),
        (8, 183),
        (9, 243),
        (10, 303),
    ],
)
async def test_jitter_randomizes_backoff_times(
    amqp_connection: AbstractRobustConnection,
    clock: Clock,
    sample_message: Message,
    max_retries: int,
    initial_backoff_seconds: float,
    pipe: str = "test",
) -> None:
    clock_1 = FakeClock(clock.now())
    clock_2 = FakeClock(clock.now())

    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe, size=0)

        await send_with_backoff(
            channel,
            send_message,
            sample_message,
            pipe,
            backoff_seconds=initial_backoff_seconds,
            exponential_backoff=True,
            max_retries=max_retries,
            apply_jitter=True,
            clock=clock_1,
        )

        await send_with_backoff(
            channel,
            send_message,
            sample_message,
            pipe,
            backoff_seconds=initial_backoff_seconds,
            exponential_backoff=True,
            max_retries=max_retries,
            apply_jitter=True,
            clock=clock_2,
        )

    assert clock_1.time != clock_2.time


async def test_waiting_for_pipe_returns_true_when_queue_exists(
    amqp_connection: AbstractRobustConnection, pipe: str = "test"
) -> None:
    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe, size=0)
        success = await wait_for_pipe(channel, pipe, max_retries=0)
    assert success


async def test_waiting_for_pipe_fails_if_pipe_not_declared(
    amqp_connection: AbstractRobustConnection, pipe: str = "test", max_retries: int = 0
) -> None:
    async with amqp_connection.channel() as channel:
        success = await wait_for_pipe(channel, pipe, max_retries=max_retries)
    assert not success


@pytest.mark.parametrize("max_retries", [0, 1, 3, 4, 16, 32, 64, 1024])
async def test_waiting_for_pipe_retries_until_max_retries(
    amqp_connection: AbstractRobustConnection,
    max_retries: int,
    clock: Clock,
    queue_getter: QueueGetterT = FailingQueueGetter(9999),
    pipe: str = "test",
    backoff_seconds: float = 1.0,
) -> None:
    start_time = clock.now()
    async with amqp_connection.channel() as channel:
        await wait_for_pipe(
            channel,
            pipe,
            backoff_seconds=backoff_seconds,
            max_retries=max_retries,
            clock=clock,
            getter=queue_getter,
        )
    end_time = clock.now()
    time_delta = end_time - start_time
    seconds_passed = time_delta.total_seconds()
    assert seconds_passed == backoff_seconds * max_retries


async def test_if_no_max_retries_pipe_is_waited_for_infinitely(
    amqp_connection: AbstractRobustConnection,
    clock: Clock,
    queue_getter: QueueGetterT = FailingQueueGetter(9999),
    pipe: str = "test",
) -> None:
    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe, size=0)
        success = await wait_for_pipe(
            channel, pipe, clock=clock, max_retries=None, getter=queue_getter
        )
    assert success


async def test_backoff_is_respected_when_retries_set_infinite(
    amqp_connection: AbstractRobustConnection,
    clock: Clock,
    sample_message: Message,
    pipe: str = "test",
) -> None:
    fails_count = 10
    start_time = clock.now()
    sender = FailingMessageSender(fails_count)
    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe)
        await send_with_backoff(
            channel,
            sender,
            sample_message,
            pipe,
            backoff_seconds=1,
            exponential_backoff=False,
            max_retries=None,
            apply_jitter=False,
            clock=clock,
        )
    stop_time = clock.now()
    delta = stop_time - start_time
    seconds_passed = delta.total_seconds()
    assert seconds_passed == fails_count


async def test_exponential_backoff_is_respected_when_retries_set_infinite(
    amqp_connection: AbstractRobustConnection,
    clock: Clock,
    sample_message: Message,
    pipe: str = "test",
) -> None:
    fails_count = 10
    start_time = clock.now()
    sender = FailingMessageSender(fails_count)
    async with amqp_connection.channel() as channel:
        await declare_pipe(channel, pipe)
        await send_with_backoff(
            channel,
            sender,
            sample_message,
            pipe,
            backoff_seconds=1,
            exponential_backoff=True,
            max_retries=None,
            apply_jitter=False,
            clock=clock,
        )
    stop_time = clock.now()
    delta = stop_time - start_time
    seconds_passed = delta.total_seconds()
    assert seconds_passed == 2**10 - 1
