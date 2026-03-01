import asyncio
import inspect
from asyncio import AbstractEventLoop
from collections.abc import Iterable
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass, field
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
    Optional,
    TypedDict,
    Unpack,
    AsyncContextManager,
    get_type_hints,
)

import pydantic
from aio_pika.abc import AbstractRobustConnection, AbstractIncomingMessage
from pydantic import BaseModel

from khnm.consumers import consume
from khnm.exceptions import NodeKwargsInvalid
from khnm.producers import make_producer, Producer
from khnm.serialization import pydantic_model_to_message, message_to_pydantic_model
from khnm.time import Clock, UtcClock
from khnm.types import GeneratorCallbackT, CallbackT, CallbackOutputT


class Bag(pydantic.BaseModel):
    model_config = {"extra": "allow"}


class Runner(Protocol):
    @property
    def name(self) -> str:
        pass

    async def run(self, connection: AbstractRobustConnection, threads: int) -> None:
        pass


class Source(Runner):
    def __init__(
        self,
        name: str,
        downstream_pipe: str,
        callback: GeneratorCallbackT,
        pipe_length: Optional[int] = None,
        durable: bool = False,
        backoff_seconds: float = 0.1,
        max_retries: Optional[int] = None,
        exponential_backoff: bool = False,
        max_backoff_seconds: Optional[float] = None,
        apply_jitter: bool = False,
        clock: Clock = UtcClock(),
    ) -> None:
        self._name = name
        self._downstream_pipe = downstream_pipe
        self._callback = callback
        self._pipe_length = pipe_length
        self._durable = durable
        self._backoff_seconds = backoff_seconds
        self._max_retries = max_retries
        self._exponential_backoff = exponential_backoff
        self._max_backoff_seconds = max_backoff_seconds
        self._apply_jitter = apply_jitter
        self._clock = clock

    @property
    def name(self) -> str:
        return self._name

    async def run(self, connection: AbstractRobustConnection, threads: int) -> None:
        is_callback_async = _is_callback_async(self._callback)
        if is_callback_async:
            await self._run_async(connection)
        else:
            await self._run_sync(connection, threads)

    async def _run_async(self, connection: AbstractRobustConnection) -> None:
        await self._run_async_callback(connection)

    async def _run_sync(
        self, connection: AbstractRobustConnection, threads: int
    ) -> None:
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=threads) as executor:
            await self._run_sync_callback(connection, loop, executor)

    async def _run_async_callback(self, connection: AbstractRobustConnection) -> None:
        async with make_producer(
            connection,
            self._downstream_pipe,
            size=self._pipe_length,
            durable=self._durable,
            backoff_seconds=self._backoff_seconds,
            max_retries=self._max_retries,
            exponential_backoff=self._exponential_backoff,
            max_backoff_seconds=self._max_backoff_seconds,
            apply_jitter=self._apply_jitter,
            persistent_messages=self._durable,
            clock=self._clock,
        ) as producer:
            async for obj in cast(
                AsyncGenerator[CallbackOutputT, None], self._callback()
            ):
                assert isinstance(obj, pydantic.BaseModel)
                message = pydantic_model_to_message(obj)
                await producer.publish(message)

    async def _run_sync_callback(
        self,
        connection: AbstractRobustConnection,
        loop: AbstractEventLoop,
        executor: ThreadPoolExecutor,
    ) -> None:
        async with make_producer(
            connection,
            self._downstream_pipe,
            size=self._pipe_length,
            durable=self._durable,
            backoff_seconds=self._backoff_seconds,
            max_retries=self._max_retries,
            exponential_backoff=self._exponential_backoff,
            max_backoff_seconds=self._max_backoff_seconds,
            apply_jitter=self._apply_jitter,
            persistent_messages=self._durable,
            clock=self._clock,
        ) as producer:
            generator = cast(Generator[CallbackOutputT, None, None], self._callback())
            sentinel = object()
            while True:
                obj = await loop.run_in_executor(
                    executor, lambda: next(generator, sentinel)
                )
                if obj is sentinel:
                    break
                assert isinstance(obj, pydantic.BaseModel)
                message = pydantic_model_to_message(obj)
                await producer.publish(message)


class Node(Runner):
    def __init__(
        self,
        name: str,
        upstream_pipe: str,
        downstream_pipe: str,
        callback: CallbackT,
        pipe_length: Optional[int] = None,
        durable: bool = False,
        backoff_seconds: float = 0.1,
        max_retries: Optional[int] = None,
        exponential_backoff: bool = False,
        max_backoff_seconds: Optional[float] = None,
        apply_jitter: bool = False,
        connection_max_retries: Optional[int] = None,
        connection_backoff_seconds: float = 1.0,
        prefetch_count: Optional[int] = None,
        clock: Clock = UtcClock(),
    ) -> None:
        self._name = name
        self._upstream_pipe = upstream_pipe
        self._downstream_pipe = downstream_pipe
        self._callback = callback
        self._pipe_length = pipe_length
        self._durable = durable
        self._backoff_seconds = backoff_seconds
        self._max_retries = max_retries
        self._exponential_backoff = exponential_backoff
        self._max_backoff_seconds = max_backoff_seconds
        self._apply_jitter = apply_jitter
        self._connection_max_retries = connection_max_retries
        self._connection_backoff_seconds = connection_backoff_seconds
        self._prefetch_count = prefetch_count
        self._clock = clock

    @property
    def name(self) -> str:
        return self._name

    async def run(self, connection: AbstractRobustConnection, threads: int) -> None:
        is_callback_async = _is_callback_async(self._callback)
        if is_callback_async:
            await self._run_async(connection)
        else:
            await self._run_sync(connection, threads)

    async def _run_async(self, connection: AbstractRobustConnection) -> None:
        while True:
            await self._run_async_callback(connection)

    async def _run_sync(
        self, connection: AbstractRobustConnection, threads: int
    ) -> None:
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=threads) as executor:
            while True:
                await self._run_sync_callback(connection, loop, executor, threads)

    async def _run_async_callback(self, connection: AbstractRobustConnection) -> None:
        async with make_producer(
            connection,
            self._downstream_pipe,
            size=self._pipe_length,
            durable=self._durable,
            backoff_seconds=self._backoff_seconds,
            max_retries=self._max_retries,
            exponential_backoff=self._exponential_backoff,
            max_backoff_seconds=self._max_backoff_seconds,
            apply_jitter=self._apply_jitter,
            persistent_messages=self._durable,
            clock=self._clock,
        ) as producer:
            async for message in consume(
                connection,
                self._upstream_pipe,
                upstream_connection_max_retries=self._connection_max_retries,
                upstream_connection_backoff_seconds=self._connection_backoff_seconds,
                prefetch_count=self._prefetch_count,
                clock=self._clock,
            ):
                async with message as current_message:
                    obj = message_to_pydantic_model(current_message, Bag)
                    result = await cast(Awaitable[CallbackOutputT], self._callback(obj))
                    if result is not None:
                        await _handle_callback_output(result, producer)

    async def _run_sync_callback(
        self,
        connection: AbstractRobustConnection,
        loop: AbstractEventLoop,
        executor: ThreadPoolExecutor,
        threads: int,
    ) -> None:
        async with make_producer(
            connection,
            self._downstream_pipe,
            size=self._pipe_length,
            durable=self._durable,
            backoff_seconds=self._backoff_seconds,
            max_retries=self._max_retries,
            exponential_backoff=self._exponential_backoff,
            max_backoff_seconds=self._max_backoff_seconds,
            apply_jitter=self._apply_jitter,
            persistent_messages=self._durable,
            clock=self._clock,
        ) as producer:
            semaphore = asyncio.Semaphore(threads)
            pending = set()

            async def process(
                message_context: AsyncContextManager[AbstractIncomingMessage],
            ) -> None:
                async with semaphore:
                    async with message_context as current_message:
                        obj = message_to_pydantic_model(current_message, Bag)
                        result = cast(
                            CallbackOutputT,
                            await loop.run_in_executor(executor, self._callback, obj),
                        )
                        if result is not None:
                            await _handle_callback_output(result, producer)

            async for message in consume(
                connection,
                self._upstream_pipe,
                upstream_connection_max_retries=self._connection_max_retries,
                upstream_connection_backoff_seconds=self._connection_backoff_seconds,
                prefetch_count=self._prefetch_count,
                clock=self._clock,
            ):
                task = asyncio.create_task(process(message))
                pending.add(task)
                task.add_done_callback(pending.discard)

            if pending:
                await asyncio.gather(*pending)


class Sink(Runner):
    def __init__(
        self,
        name: str,
        upstream_pipe: str,
        callback: CallbackT,
        connection_max_retries: Optional[int] = None,
        connection_backoff_seconds: float = 1.0,
        prefetch_count: Optional[int] = None,
        clock: Clock = UtcClock(),
    ) -> None:
        self._name = name
        self._upstream_pipe = upstream_pipe
        self._callback = callback
        self._connection_max_retries = connection_max_retries
        self._connection_backoff_seconds = connection_backoff_seconds
        self._prefetch_count = prefetch_count
        self._clock = clock

    @property
    def name(self) -> str:
        return self._name

    async def run(self, connection: AbstractRobustConnection, threads: int) -> None:
        is_callback_async = _is_callback_async(self._callback)
        if is_callback_async:
            await self._run_async(connection)
        else:
            await self._run_sync(connection, threads)

    async def _run_async(self, connection: AbstractRobustConnection) -> None:
        while True:
            await self._run_async_callback(connection)

    async def _run_sync(
        self, connection: AbstractRobustConnection, threads: int
    ) -> None:
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=threads) as executor:
            while True:
                await self._run_sync_callback(connection, loop, executor, threads)

    async def _run_async_callback(self, connection: AbstractRobustConnection) -> None:
        async for message in consume(
            connection,
            self._upstream_pipe,
            upstream_connection_max_retries=self._connection_max_retries,
            upstream_connection_backoff_seconds=self._connection_backoff_seconds,
            prefetch_count=self._prefetch_count,
            clock=self._clock,
        ):
            async with message as current_message:
                obj = message_to_pydantic_model(current_message, Bag)
                await cast(Awaitable[CallbackOutputT], self._callback(obj))

    async def _run_sync_callback(
        self,
        connection: AbstractRobustConnection,
        loop: AbstractEventLoop,
        executor: ThreadPoolExecutor,
        threads: int,
    ) -> None:
        semaphore = asyncio.Semaphore(threads)
        pending = set()

        async def process(
            message_context: AsyncContextManager[AbstractIncomingMessage],
        ) -> None:
            async with semaphore:
                async with message_context as current_message:
                    obj = message_to_pydantic_model(current_message, Bag)
                    await loop.run_in_executor(executor, self._callback, obj)

        async for message in consume(
            connection,
            self._upstream_pipe,
            upstream_connection_max_retries=self._connection_max_retries,
            upstream_connection_backoff_seconds=self._connection_backoff_seconds,
            prefetch_count=self._prefetch_count,
            clock=self._clock,
        ):
            task = asyncio.create_task(process(message))
            pending.add(task)
            task.add_done_callback(pending.discard)

        if pending:
            await asyncio.gather(*pending)


class SinkKwargs(TypedDict, total=False):
    connection_max_retries: int
    connection_backoff_seconds: float
    prefetch_count: int


class SourceKwargs(TypedDict, total=False):
    pipe_length: int
    durable: bool
    backoff_seconds: float
    max_retries: int
    exponential_backoff: bool
    max_backoff_seconds: float
    apply_jitter: bool


class NodeKwargs(SourceKwargs, SinkKwargs):
    pass


RunnerKwargs = Union[SourceKwargs, SinkKwargs, NodeKwargs]


@dataclass(init=True, frozen=True)
class RunnerDefinition:
    name: str
    callback: Union[CallbackT, GeneratorCallbackT]
    kwargs: NodeKwargs = field(default_factory=NodeKwargs)


class PipelineBuilder:
    def __init__(self):
        self._runner_definitions: List[RunnerDefinition] = []

    def add(
        self,
        name: str,
        callback: Union[GeneratorCallbackT, CallbackT],
        **kwargs: Unpack[NodeKwargs],
    ) -> Self:
        self._runner_definitions.append(
            RunnerDefinition(
                name=name, callback=callback, kwargs=cast(NodeKwargs, kwargs)
            )
        )
        return self

    def build(self) -> Pipeline:
        runners: List[Runner] = []
        source_definition = self._runner_definitions.pop(0)
        next_node_definition = self._runner_definitions.pop(0)
        _validate_source_kwargs(**source_definition.kwargs)
        runners.append(
            Source(
                name=source_definition.name,
                downstream_pipe=next_node_definition.name,
                callback=cast(GeneratorCallbackT, source_definition.callback),
                **cast(SourceKwargs, source_definition.kwargs),
            )
        )
        while self._runner_definitions:
            current_node_definition = next_node_definition
            next_node_definition = self._runner_definitions.pop(0)
            runners.append(
                Node(
                    name=current_node_definition.name,
                    upstream_pipe=current_node_definition.name,
                    downstream_pipe=next_node_definition.name,
                    callback=cast(CallbackT, current_node_definition.callback),
                    **current_node_definition.kwargs,
                )
            )
        _validate_sink_kwargs(**next_node_definition.kwargs)
        runners.append(
            Sink(
                name=next_node_definition.name,
                upstream_pipe=next_node_definition.name,
                callback=cast(CallbackT, next_node_definition.callback),
                **cast(SinkKwargs, next_node_definition.kwargs),
            )
        )
        return Pipeline(runners)


class Pipeline:
    def __init__(self, runners: List[Runner]) -> None:
        self._runners = runners
        self._runners_map = {runner.name: runner for runner in runners}

    async def run(
        self, connection: AbstractRobustConnection, name: str, threads: int = 1
    ) -> None:
        runner = self._runners_map.get(name)
        if not runner:
            raise ValueError(f"Runner {name} not found in the pipeline")
        await runner.run(connection, threads)


def make_pipeline() -> PipelineBuilder:
    return PipelineBuilder()


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
    value: CallbackOutputT,
) -> bool:
    return isinstance(value, Iterable) and not isinstance(value, BaseModel)


def _validate_source_kwargs(**kwargs: Unpack[NodeKwargs]) -> None:
    for kwarg_name, _ in kwargs.items():
        if kwarg_name not in get_type_hints(SourceKwargs):
            raise NodeKwargsInvalid(f"Invalid argument for source node: {kwarg_name}")


def _validate_sink_kwargs(**kwargs: Unpack[NodeKwargs]) -> None:
    for kwarg_name, _ in kwargs.items():
        if kwarg_name not in get_type_hints(SinkKwargs):
            raise NodeKwargsInvalid(f"Invalid argument for sink node: {kwarg_name}")
