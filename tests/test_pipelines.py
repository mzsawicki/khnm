import asyncio
import threading
from typing import AsyncGenerator, Optional, cast, Any

import pytest
from aio_pika.abc import AbstractRobustConnection

from khnm.exceptions import NodeKwargsInvalid
from khnm.pipelines import make_pipeline
from tests.doubles import (
    generate_random_numbers_async,
    async_callback_stub,
    AsyncCallbackSpy,
    SampleDataObject,
)
from tests.utils import timeout


async def test_pipeline_sends_messages_source_to_sink(
    amqp_connection: AbstractRobustConnection,
) -> None:
    spy = AsyncCallbackSpy()

    pipeline = (
        make_pipeline()
        .add("test-source", generate_random_numbers_async)
        .add("test-node", async_callback_stub)
        .add("test-sink", spy)
        .build()
    )

    tasks = [
        asyncio.create_task(pipeline.run(amqp_connection, "test-source")),
        asyncio.create_task(pipeline.run(amqp_connection, "test-node")),
        asyncio.create_task(pipeline.run(amqp_connection, "test-sink")),
    ]

    async def spy_got_message() -> bool:
        return spy.received_obj is not None

    success = await timeout(spy_got_message, awaited_result=True, timeout_seconds=10)

    for task in tasks:
        task.cancel()

    assert success is True


async def test_pipeline_with_multiple_intermediate_nodes(
    amqp_connection: AbstractRobustConnection,
) -> None:
    spy = AsyncCallbackSpy()

    pipeline = (
        make_pipeline()
        .add("test-source", generate_random_numbers_async)
        .add("test-node-1", async_callback_stub)
        .add("test-node-2", async_callback_stub)
        .add("test-sink", spy)
        .build()
    )

    tasks = [
        asyncio.create_task(pipeline.run(amqp_connection, "test-source")),
        asyncio.create_task(pipeline.run(amqp_connection, "test-node-1")),
        asyncio.create_task(pipeline.run(amqp_connection, "test-node-2")),
        asyncio.create_task(pipeline.run(amqp_connection, "test-sink")),
    ]

    async def spy_got_message() -> bool:
        return spy.received_obj is not None

    success = await timeout(spy_got_message, awaited_result=True, timeout_seconds=10)

    for task in tasks:
        task.cancel()

    assert success is True


async def test_processing_through_pipeline_gives_correct_result(
    amqp_connection: AbstractRobustConnection,
) -> None:
    spy = AsyncCallbackSpy()

    async def generate_1() -> AsyncGenerator[SampleDataObject, None]:
        yield SampleDataObject(integer=2)

    def add(obj: SampleDataObject) -> SampleDataObject:
        return SampleDataObject(integer=obj.integer + 1)

    def multiply(obj: SampleDataObject) -> SampleDataObject:
        return SampleDataObject(integer=obj.integer * 3)

    pipeline = (
        make_pipeline()
        .add("number-source", generate_1)
        .add("add", add)
        .add("multiply", multiply)
        .add("result", spy)
        .build()
    )

    tasks = [
        asyncio.create_task(pipeline.run(amqp_connection, "number-source")),
        asyncio.create_task(pipeline.run(amqp_connection, "add")),
        asyncio.create_task(pipeline.run(amqp_connection, "multiply")),
        asyncio.create_task(pipeline.run(amqp_connection, "result")),
    ]

    async def get_result_from_spy() -> Optional[int]:
        obj = spy.received_obj
        if obj:
            result_ = cast(SampleDataObject, obj)
            return result_.integer
        return None

    result = await timeout(get_result_from_spy, awaited_result=9, timeout_seconds=10)

    for task in tasks:
        task.cancel()

    assert result == 9


async def test_processing_through_pipeline_gives_correct_result_with_custom_parameters(
    amqp_connection: AbstractRobustConnection,
) -> None:
    spy = AsyncCallbackSpy()

    async def generate_1() -> AsyncGenerator[SampleDataObject, None]:
        yield SampleDataObject(integer=2)

    def add(obj: SampleDataObject) -> SampleDataObject:
        return SampleDataObject(integer=obj.integer + 1)

    def multiply(obj: SampleDataObject) -> SampleDataObject:
        return SampleDataObject(integer=obj.integer * 3)

    pipeline = (
        make_pipeline()
        .add("number-source", generate_1, pipe_length=128)
        .add("add", add, pipe_length=256)
        .add("multiply", multiply, pipe_length=512)
        .add("result", spy)
        .build()
    )

    tasks = [
        asyncio.create_task(pipeline.run(amqp_connection, "number-source")),
        asyncio.create_task(pipeline.run(amqp_connection, "add")),
        asyncio.create_task(pipeline.run(amqp_connection, "multiply")),
        asyncio.create_task(pipeline.run(amqp_connection, "result")),
    ]

    async def get_result_from_spy() -> Optional[int]:
        obj = spy.received_obj
        if obj:
            result_ = cast(SampleDataObject, obj)
            return result_.integer
        return None

    result = await timeout(get_result_from_spy, awaited_result=9, timeout_seconds=10)

    for task in tasks:
        task.cancel()

    assert result == 9


async def test_node_runs_on_multiple_threads(
    amqp_connection: AbstractRobustConnection,
) -> None:
    threads: int = 4
    timeout_seconds: int = 5
    barrier = threading.Barrier(threads)
    observed_thread_ids = []
    lock = threading.Lock()
    spy = AsyncCallbackSpy()

    async def did_spy_got_result() -> bool:
        obj = spy.received_obj
        return obj is not None

    def slow_callback(obj: SampleDataObject) -> SampleDataObject:
        with lock:
            observed_thread_ids.append(threading.current_thread().ident)
        barrier.wait(timeout=timeout_seconds)
        return obj

    pipeline = (
        make_pipeline()
        .add("source", generate_random_numbers_async)
        .add("node", slow_callback)
        .add("sink", spy)
        .build()
    )

    tasks = [
        asyncio.create_task(pipeline.run(amqp_connection, "source")),
        asyncio.create_task(pipeline.run(amqp_connection, "node", threads=threads)),
        asyncio.create_task(pipeline.run(amqp_connection, "sink")),
    ]

    success = await timeout(
        did_spy_got_result,
        awaited_result=True,
        timeout_seconds=timeout_seconds * 2,
    )

    for task in tasks:
        task.cancel()

    assert success is True and len(set(observed_thread_ids)) == threads


async def test_sequential_node_is_detected_by_barrier(
    amqp_connection: AbstractRobustConnection,
) -> None:
    threads: int = 1
    timeout_seconds: int = 2
    barrier = threading.Barrier(threads + 1)  # always unsatisfiable with threads=1
    spy = AsyncCallbackSpy()

    async def did_spy_get_result() -> bool:
        return spy.received_obj is not None

    def slow_callback(obj: SampleDataObject) -> SampleDataObject:
        barrier.wait(timeout=timeout_seconds)
        return obj

    pipeline = (
        make_pipeline()
        .add("source", generate_random_numbers_async)
        .add("node", slow_callback)
        .add("sink", spy)
        .build()
    )

    tasks = [
        asyncio.create_task(pipeline.run(amqp_connection, "source")),
        asyncio.create_task(pipeline.run(amqp_connection, "node", threads=threads)),
        asyncio.create_task(pipeline.run(amqp_connection, "sink")),
    ]

    success = await timeout(
        did_spy_get_result,
        awaited_result=True,
        timeout_seconds=timeout_seconds * 2,
    )

    for task in tasks:
        task.cancel()

    assert success is not True
    assert barrier.broken


async def test_sink_runs_on_multiple_threads(
    amqp_connection: AbstractRobustConnection,
) -> None:
    threads: int = 4
    timeout_seconds: int = 5
    barrier = threading.Barrier(threads)
    observed_thread_ids = []
    lock = threading.Lock()
    processed = []

    def sink_callback(obj: SampleDataObject) -> None:
        with lock:
            observed_thread_ids.append(threading.current_thread().ident)
        barrier.wait(timeout=timeout_seconds)
        processed.append(True)

    pipeline = (
        make_pipeline()
        .add("source", generate_random_numbers_async)
        .add("sink", sink_callback)
        .build()
    )

    tasks = [
        asyncio.create_task(pipeline.run(amqp_connection, "source")),
        asyncio.create_task(pipeline.run(amqp_connection, "sink", threads=threads)),
    ]

    async def did_process() -> bool:
        return len(processed) > 0

    success = await timeout(
        did_process,
        awaited_result=True,
        timeout_seconds=timeout_seconds * 2,
    )

    for task in tasks:
        task.cancel()

    assert success is True and len(set(observed_thread_ids)) == threads


async def test_sequential_sink_is_detected_by_barrier(
    amqp_connection: AbstractRobustConnection,
) -> None:
    threads: int = 1
    timeout_seconds: int = 2
    barrier = threading.Barrier(threads + 1)  # always unsatisfiable with threads=1
    processed = []

    def sink_callback(obj: SampleDataObject) -> None:
        barrier.wait(timeout=timeout_seconds)
        processed.append(True)

    pipeline = (
        make_pipeline()
        .add("source", generate_random_numbers_async)
        .add("sink", sink_callback)
        .build()
    )

    tasks = [
        asyncio.create_task(pipeline.run(amqp_connection, "source")),
        asyncio.create_task(pipeline.run(amqp_connection, "sink", threads=threads)),
    ]

    async def did_process() -> bool:
        return len(processed) > 0

    success = await timeout(
        did_process,
        awaited_result=True,
        timeout_seconds=timeout_seconds * 2,
    )

    for task in tasks:
        task.cancel()

    assert success is not True
    assert barrier.broken


@pytest.mark.parametrize(
    "kwarg_name,kwarg_value",
    [
        ("connection_max_retries", 1),
        ("connection_backoff_seconds", 1),
        ("prefetch_count", 1),
    ],
)
async def test_source_node_kwargs_raises_exception_at_invalid_kwargs(
    amqp_connection: AbstractRobustConnection,
    kwarg_name: str,
    kwarg_value: Any,
) -> None:
    with pytest.raises(NodeKwargsInvalid):
        (
            make_pipeline()
            .add("source", generate_random_numbers_async, **{kwarg_name: kwarg_value})
            .add("sink", lambda obj: None)
            .build()
        )


@pytest.mark.parametrize(
    "kwarg_name,kwarg_value",
    [
        ("pipe_length", 1),
        ("durable", True),
        ("backoff_seconds", 1),
        ("max_retries", 1),
        ("exponential_backoff", True),
        ("max_backoff_seconds", 1.0),
        ("apply_jitter", True),
    ],
)
async def test_sink_node_kwargs_raises_exception_at_invalid_kwargs(
    amqp_connection: AbstractRobustConnection,
    kwarg_name: str,
    kwarg_value: Any,
) -> None:
    with pytest.raises(NodeKwargsInvalid):
        (
            make_pipeline()
            .add("source", generate_random_numbers_async)
            .add("sink", lambda obj: None, **{kwarg_name: kwarg_value})
            .build()
        )
