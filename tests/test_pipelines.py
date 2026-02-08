import asyncio
from typing import AsyncGenerator, Optional

import pydantic
from aio_pika.abc import AbstractRobustConnection

from khnm.pipelines import PipelineBuilder
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
        PipelineBuilder(amqp_connection)
        .with_source("test-source", generate_random_numbers_async)
        .with_node("test-node", async_callback_stub)
        .with_sink("test-sink", spy)
        .build()
    )

    tasks = [
        asyncio.create_task(pipeline.run("test-source")),
        asyncio.create_task(pipeline.run("test-node")),
        asyncio.create_task(pipeline.run("test-sink")),
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
        PipelineBuilder(amqp_connection)
        .with_source("test-source", generate_random_numbers_async)
        .with_node("test-node-1", async_callback_stub)
        .with_node("test-node-2", async_callback_stub)
        .with_sink("test-sink", spy)
        .build()
    )

    tasks = [
        asyncio.create_task(pipeline.run("test-source")),
        asyncio.create_task(pipeline.run("test-node-1")),
        asyncio.create_task(pipeline.run("test-node-2")),
        asyncio.create_task(pipeline.run("test-sink")),
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
        PipelineBuilder(amqp_connection)
        .with_source("number-source", generate_1)
        .with_node("add", add)
        .with_node("multiply", multiply)
        .with_sink("result", spy)
        .build()
    )

    tasks = [
        asyncio.create_task(pipeline.run("number-source")),
        asyncio.create_task(pipeline.run("add")),
        asyncio.create_task(pipeline.run("multiply")),
        asyncio.create_task(pipeline.run("result")),
    ]

    async def get_result_from_spy() -> Optional[int]:
        obj = spy.received_obj
        if obj:
            assert isinstance(obj, SampleDataObject)
            result_ = obj.integer
            return result_
        return None

    result = await timeout(get_result_from_spy, awaited_result=9, timeout_seconds=10)

    for task in tasks:
        task.cancel()

    assert result == 9
