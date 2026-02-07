import asyncio

from aio_pika.abc import AbstractRobustConnection

from khnm.pipelines import PipelineBuilder
from tests.doubles import (
    generate_random_numbers_async,
    async_callback_stub,
    AsyncCallbackSpy,
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
