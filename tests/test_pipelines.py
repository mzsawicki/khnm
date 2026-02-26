import asyncio
from typing import AsyncGenerator, Optional, cast

from aio_pika.abc import AbstractRobustConnection

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
