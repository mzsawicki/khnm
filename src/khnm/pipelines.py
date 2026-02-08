import inspect
from collections.abc import Iterable
from dataclasses import dataclass
from typing import (
    Self,
    Callable,
    Protocol,
    List,
    Union,
    AsyncGenerator,
    cast,
    Generator,
    Awaitable,
)

import pydantic
from aio_pika.abc import AbstractRobustConnection
from pydantic import BaseModel

from khnm.consumers import consume
from khnm.producers import make_producer, Producer
from khnm.serialization import pydantic_model_to_message, message_to_pydantic_model
from khnm.types import GeneratorCallbackT, CallbackT, CallbackOutputT


class Bag(pydantic.BaseModel):
    model_config = {"extra": "allow"}


class Runner(Protocol):
    @property
    def name(self) -> str:
        pass

    async def run(self) -> None:
        pass


class Source(Runner):
    def __init__(
        self,
        connection: AbstractRobustConnection,
        name: str,
        downstream_pipe: str,
        callback: GeneratorCallbackT,
    ) -> None:
        self._connection = connection
        self._name = name
        self._downstream_pipe = downstream_pipe
        self._callback = callback

    @property
    def name(self) -> str:
        return self._name

    async def run(self) -> None:
        is_callback_async = _is_callback_async(self._callback)
        if is_callback_async:
            await self._run_async()
        else:
            await self._run_sync()

    async def _run_async(self) -> None:
        await self._run_async_callback()

    async def _run_sync(self) -> None:
        await self._run_sync_callback()

    async def _run_async_callback(self) -> None:
        async with make_producer(self._connection, self._downstream_pipe) as producer:
            async for obj in cast(
                AsyncGenerator[CallbackOutputT, None], self._callback()
            ):
                assert isinstance(obj, pydantic.BaseModel)
                message = pydantic_model_to_message(obj)
                await producer.publish(message)

    async def _run_sync_callback(self) -> None:
        async with make_producer(self._connection, self._downstream_pipe) as producer:
            for obj in cast(Generator[CallbackOutputT, None, None], self._callback()):
                assert isinstance(obj, pydantic.BaseModel)
                message = pydantic_model_to_message(obj)
                await producer.publish(message)


class Node(Runner):
    def __init__(
        self,
        connection: AbstractRobustConnection,
        name: str,
        upstream_pipe: str,
        downstream_pipe: str,
        callback: CallbackT,
    ) -> None:
        self._connection = connection
        self._name = name
        self._upstream_pipe = upstream_pipe
        self._downstream_pipe = downstream_pipe
        self._callback = callback

    @property
    def name(self) -> str:
        return self._name

    async def run(self) -> None:
        is_callback_async = _is_callback_async(self._callback)
        if is_callback_async:
            await self._run_async()
        else:
            await self._run_sync()

    async def _run_async(self) -> None:
        while True:
            await self._run_async_callback()

    async def _run_sync(self) -> None:
        while True:
            await self._run_sync_callback()

    async def _run_async_callback(self) -> None:
        async with make_producer(self._connection, self._downstream_pipe) as producer:
            async for message in consume(self._connection, self._upstream_pipe):
                async with message as current_message:
                    obj = message_to_pydantic_model(current_message, Bag)
                    result = await cast(Awaitable[CallbackOutputT], self._callback(obj))
                    await _handle_callback_output(result, producer)

    async def _run_sync_callback(self) -> None:
        async with make_producer(self._connection, self._downstream_pipe) as producer:
            async for message in consume(self._connection, self._upstream_pipe):
                async with message as current_message:
                    obj = message_to_pydantic_model(current_message, Bag)
                    result = cast(CallbackOutputT, self._callback(obj))
                    await _handle_callback_output(result, producer)


class Sink(Runner):
    def __init__(
        self,
        connection: AbstractRobustConnection,
        name: str,
        upstream_pipe: str,
        callback: CallbackT,
    ) -> None:
        self._connection = connection
        self._name = name
        self._upstream_pipe = upstream_pipe
        self._callback = callback

    @property
    def name(self) -> str:
        return self._name

    async def run(self) -> None:
        is_callback_async = _is_callback_async(self._callback)
        if is_callback_async:
            await self._run_async()
        else:
            await self._run_sync()

    async def _run_async(self) -> None:
        while True:
            await self._run_async_callback()

    async def _run_sync(self) -> None:
        while True:
            await self._run_sync_callback()

    async def _run_async_callback(self) -> None:
        async for message in consume(self._connection, self._upstream_pipe):
            async with message as current_message:
                obj = message_to_pydantic_model(current_message, Bag)
                await cast(Awaitable[CallbackOutputT], self._callback(obj))

    async def _run_sync_callback(self) -> None:
        async for message in consume(self._connection, self._upstream_pipe):
            async with message as current_message:
                obj = message_to_pydantic_model(current_message, Bag)
                cast(CallbackOutputT, self._callback(obj))


@dataclass(init=True, frozen=True)
class RunnerDefinition:
    name: str
    callback: Union[CallbackT, GeneratorCallbackT]


class PipelineBuilder:
    def __init__(self, connection: AbstractRobustConnection):
        self._connection = connection
        self._runner_definitions: List[RunnerDefinition] = []

    def with_source(self, name: str, callback: GeneratorCallbackT) -> Self:
        self._runner_definitions.append(
            RunnerDefinition(
                name=name,
                callback=callback,
            )
        )
        return self

    def with_node(self, name: str, callback: CallbackT) -> Self:
        self._runner_definitions.append(
            RunnerDefinition(
                name=name,
                callback=callback,
            )
        )
        return self

    def with_sink(self, name: str, callback: CallbackT) -> Self:
        self._runner_definitions.append(
            RunnerDefinition(
                name=name,
                callback=callback,
            )
        )
        return self

    def build(self) -> Pipeline:
        runners: List[Runner] = []
        source_definition = self._runner_definitions.pop(0)
        next_node_definition = self._runner_definitions.pop(0)
        runners.append(
            Source(
                connection=self._connection,
                name=source_definition.name,
                downstream_pipe=next_node_definition.name,
                callback=cast(GeneratorCallbackT, source_definition.callback),
            )
        )
        previous_node_definition = source_definition
        while self._runner_definitions:
            current_node_definition = next_node_definition
            next_node_definition = self._runner_definitions.pop(0)
            runners.append(
                Node(
                    connection=self._connection,
                    name=current_node_definition.name,
                    upstream_pipe=current_node_definition.name,
                    downstream_pipe=next_node_definition.name,
                    callback=cast(CallbackT, current_node_definition.callback),
                )
            )
            previous_node_definition = current_node_definition
        runners.append(
            Sink(
                connection=self._connection,
                name=next_node_definition.name,
                upstream_pipe=next_node_definition.name,
                callback=cast(CallbackT, next_node_definition.callback),
            )
        )
        return Pipeline(runners)


class Pipeline:
    def __init__(self, runners: List[Runner]) -> None:
        self._runners = runners
        self._runners_map = {runner.name: runner for runner in runners}

    async def run(self, name: str) -> None:
        runner = self._runners_map.get(name)
        if not runner:
            raise ValueError(f"Runner {name} not found in the pipeline")
        await runner.run()


async def _handle_callback_output(result: CallbackOutputT, producer: Producer) -> None:
    if _is_callback_result_iterable(result):
        results_serialized = [
            pydantic_model_to_message(item)
            for item in cast(Iterable[BaseModel], result)
        ]
        for item_serialized in results_serialized:
            await producer.publish(item_serialized)
    else:
        assert isinstance(result, BaseModel)
        result_serialized = pydantic_model_to_message(result)
        await producer.publish(result_serialized)


def _is_callback_async(callback: Callable[..., ...]) -> bool:
    target = callback
    if not inspect.isfunction(callback) and not inspect.ismethod(callback):
        if hasattr(callback, "__call__"):
            target = callback.__call__
        else:
            return False

    return inspect.iscoroutinefunction(target) or inspect.isasyncgenfunction(target)


def _is_callback_result_iterable(
    value: Union[pydantic.BaseModel, Iterable[pydantic.BaseModel]],
) -> bool:
    return isinstance(value, Iterable) and not isinstance(value, BaseModel)
