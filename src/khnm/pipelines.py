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
from aio_pika import Message
from aio_pika.abc import AbstractRobustConnection, AbstractIncomingMessage
from pydantic import BaseModel

from khnm.consumers import consume
from khnm.exceptions import NodeKwargsInvalid, PipelineDefinitionInvalid
from khnm.pipes import get_dlq_name, get_queue_name
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

    def _make_producer(
        self, connection: AbstractRobustConnection
    ) -> AsyncContextManager[Producer]:
        return make_producer(
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
        )

    async def _run_async_callback(self, connection: AbstractRobustConnection) -> None:
        async with self._make_producer(connection) as producer:
            callback_generator = self._callback()
            if not isinstance(callback_generator, AsyncGenerator):
                raise TypeError(
                    f"Expected AsyncGenerator as callback, got: {type(callback_generator)}"
                )
            async for obj in callback_generator:
                if not isinstance(obj, pydantic.BaseModel):
                    raise TypeError(
                        f"Source callback must yield BaseModel instances, got {type(obj).__name__}"
                    )
                message = pydantic_model_to_message(obj)
                await producer.publish(message)

    async def _run_sync_callback(
        self,
        connection: AbstractRobustConnection,
        loop: AbstractEventLoop,
        executor: ThreadPoolExecutor,
    ) -> None:
        async with self._make_producer(connection) as producer:
            callback_generator = self._callback()
            if not isinstance(callback_generator, Generator):
                raise TypeError(
                    f"Expected Generator as callback, got: {type(callback_generator)}"
                )
            sentinel = object()
            while True:
                obj = await loop.run_in_executor(
                    executor, lambda: next(callback_generator, sentinel)
                )
                if obj is sentinel:
                    break
                if not isinstance(obj, pydantic.BaseModel):
                    raise TypeError(
                        f"Source callback must yield BaseModel instances, got {type(obj).__name__}"
                    )
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
        dlq: bool = True,
        max_callback_retries: int = 3,
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
        self._dlq = dlq
        self._max_callback_retries = max_callback_retries
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

    async def _make_producer(
        self, connection: AbstractRobustConnection
    ) -> AsyncContextManager[Producer]:
        if self._dlq:
            await self._make_dlq(connection)
        return make_producer(
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
        )

    async def _make_dlq(self, connection: AbstractRobustConnection) -> None:
        async with connection.channel() as channel:
            await channel.declare_queue(get_dlq_name(self._name), durable=True)

    def _consume(
        self, connection: AbstractRobustConnection
    ) -> AsyncGenerator[AsyncContextManager[AbstractIncomingMessage], None]:
        return consume(
            connection,
            self._upstream_pipe,
            upstream_connection_max_retries=self._connection_max_retries,
            upstream_connection_backoff_seconds=self._connection_backoff_seconds,
            prefetch_count=self._prefetch_count,
            clock=self._clock,
        )

    async def _run_async_callback(self, connection: AbstractRobustConnection) -> None:
        async with await self._make_producer(connection) as producer:
            async for message in self._consume(connection):
                async with message as current_message:
                    obj = message_to_pydantic_model(current_message, Bag)
                    awaitable = self._callback(obj)
                    if not isinstance(awaitable, Awaitable):
                        raise TypeError(
                            f"Expected callback to be awaitable, got {type(awaitable)}"
                        )
                    try:
                        result = await awaitable
                    except Exception:
                        await self._handle_callback_failure(connection, current_message)
                    else:
                        if result is not None:
                            await _handle_callback_output(result, producer)

    async def _run_sync_callback(
        self,
        connection: AbstractRobustConnection,
        loop: AbstractEventLoop,
        executor: ThreadPoolExecutor,
        threads: int,
    ) -> None:
        async with await self._make_producer(connection) as producer:

            async def process(
                message_context: AsyncContextManager[AbstractIncomingMessage],
            ) -> None:
                async with message_context as current_message:
                    obj = message_to_pydantic_model(current_message, Bag)
                    if not isinstance(self._callback, Callable):
                        raise TypeError(
                            f"Expected callback to be callable, got {type(self._callback)}"
                        )
                    try:
                        result = await loop.run_in_executor(
                            executor, self._callback, obj
                        )
                    except Exception:
                        await self._handle_callback_failure(connection, current_message)
                        return
                    if result is not None:
                        await _handle_callback_output(result, producer)

            await _dispatch_concurrent(self._consume(connection), threads, process)

    async def _handle_callback_failure(
        self, connection: AbstractRobustConnection, message: AbstractIncomingMessage
    ) -> None:
        retry_count_header = message.headers.get("x-callback-attempts-count", 0)
        retry_count = retry_count_header if isinstance(retry_count_header, int) else 0
        new_headers = {**message.headers, "x-callback-attempts-count": retry_count + 1}
        new_message = Message(body=message.body, headers=new_headers)
        if retry_count < self._max_callback_retries:
            target_queue = get_queue_name(self._upstream_pipe)
        else:
            target_queue = get_dlq_name(self._name)
        async with connection.channel() as channel:
            await channel.default_exchange.publish(
                new_message, routing_key=target_queue
            )


class Sink(Runner):
    def __init__(
        self,
        name: str,
        upstream_pipe: str,
        callback: CallbackT,
        connection_max_retries: Optional[int] = None,
        connection_backoff_seconds: float = 1.0,
        prefetch_count: Optional[int] = None,
        dlq: bool = True,
        max_callback_retries: int = 0,
        clock: Clock = UtcClock(),
    ) -> None:
        self._name = name
        self._upstream_pipe = upstream_pipe
        self._callback = callback
        self._connection_max_retries = connection_max_retries
        self._connection_backoff_seconds = connection_backoff_seconds
        self._prefetch_count = prefetch_count
        self._dlq = dlq
        self._max_callback_retries = max_callback_retries
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

    def _consume(
        self, connection: AbstractRobustConnection
    ) -> AsyncGenerator[AsyncContextManager[AbstractIncomingMessage], None]:
        return consume(
            connection,
            self._upstream_pipe,
            upstream_connection_max_retries=self._connection_max_retries,
            upstream_connection_backoff_seconds=self._connection_backoff_seconds,
            prefetch_count=self._prefetch_count,
            clock=self._clock,
        )

    async def _run_async_callback(self, connection: AbstractRobustConnection) -> None:
        async for message in self._consume(connection):
            async with message as current_message:
                obj = message_to_pydantic_model(current_message, Bag)
                awaitable = self._callback(obj)
                if not isinstance(awaitable, Awaitable):
                    raise TypeError(
                        f"Expected callback to be awaitable, got {type(awaitable).__name__}"
                    )
                await awaitable

    async def _run_sync_callback(
        self,
        connection: AbstractRobustConnection,
        loop: AbstractEventLoop,
        executor: ThreadPoolExecutor,
        threads: int,
    ) -> None:
        async def process(
            message_context: AsyncContextManager[AbstractIncomingMessage],
        ) -> None:
            async with message_context as current_message:
                obj = message_to_pydantic_model(current_message, Bag)
                await loop.run_in_executor(executor, self._callback, obj)

        await _dispatch_concurrent(self._consume(connection), threads, process)


class SinkKwargs(TypedDict, total=False):
    connection_max_retries: int
    connection_backoff_seconds: float
    prefetch_count: int
    dlq: bool
    max_callback_retries: int


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
class SourceDefinition:
    name: str
    callback: GeneratorCallbackT
    kwargs: SourceKwargs = field(default_factory=SourceKwargs)


@dataclass(init=True, frozen=True)
class NodeDefinition:
    name: str
    callback: CallbackT
    kwargs: SinkKwargs = field(default_factory=NodeKwargs)


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


class PipelineBuilder:
    def __init__(self, name: Optional[str] = None) -> None:
        self._name = name
        self._source_definition: Optional[SourceDefinition] = None
        self._node_definitions: List[NodeDefinition] = []
        self._sink_definition: Optional[NodeDefinition] = None

    def source(
        self, name: str, callback: GeneratorCallbackT, **kwargs: Unpack[SourceKwargs]
    ) -> Self:
        _validate_source_kwargs(**kwargs)
        full_name = _determine_full_node_name(name, self._name)
        self._source_definition = SourceDefinition(
            name=full_name, callback=callback, kwargs=SourceKwargs(**kwargs)
        )
        return self

    def node(
        self, name: str, callback: CallbackT, **kwargs: Unpack[NodeKwargs]
    ) -> Self:
        full_name = _determine_full_node_name(name, self._name)
        self._node_definitions.append(
            NodeDefinition(
                name=full_name, callback=callback, kwargs=NodeKwargs(**kwargs)
            )
        )
        return self

    def sink(
        self, name: str, callback: CallbackT, **kwargs: Unpack[SinkKwargs]
    ) -> Self:
        _validate_sink_kwargs(**kwargs)
        full_name = _determine_full_node_name(name, self._name)
        self._sink_definition = NodeDefinition(
            name=full_name, callback=callback, kwargs=SinkKwargs(**kwargs)
        )
        return self

    def build(self) -> Pipeline:
        if self._source_definition is None or self._sink_definition is None:
            raise PipelineDefinitionInvalid(
                "Pipeline must have at least source and sink"
            )
        node_definitions = list(self._node_definitions)
        runners: List[Runner] = []
        first_downstream = (
            node_definitions[0].name if node_definitions else self._sink_definition.name
        )
        runners.append(
            Source(
                name=self._source_definition.name,
                downstream_pipe=first_downstream,
                callback=self._source_definition.callback,
                **self._source_definition.kwargs,
            )
        )
        for i, node_def in enumerate(node_definitions):
            downstream = (
                node_definitions[i + 1]
                if i + 1 < len(node_definitions)
                else self._sink_definition
            )
            runners.append(
                Node(
                    name=node_def.name,
                    upstream_pipe=node_def.name,
                    downstream_pipe=downstream.name,
                    callback=node_def.callback,
                    **node_def.kwargs,
                )
            )
        runners.append(
            Sink(
                name=self._sink_definition.name,
                upstream_pipe=self._sink_definition.name,
                callback=self._sink_definition.callback,
                **self._sink_definition.kwargs,
            )
        )
        return Pipeline(runners)


def make_pipeline(name: Optional[str] = None) -> PipelineBuilder:
    return PipelineBuilder(name)


async def _dispatch_concurrent(
    consume_iter: AsyncGenerator[AsyncContextManager[AbstractIncomingMessage], None],
    threads: int,
    process_fn: Callable[
        [AsyncContextManager[AbstractIncomingMessage]], Awaitable[None]
    ],
) -> None:
    semaphore = asyncio.Semaphore(threads)
    pending = set()

    async def gated(
        message_context: AsyncContextManager[AbstractIncomingMessage],
    ) -> None:
        async with semaphore:
            await process_fn(message_context)

    async for message in consume_iter:
        task = asyncio.create_task(gated(message))
        pending.add(task)
        task.add_done_callback(pending.discard)

    if pending:
        await asyncio.gather(*pending)


async def _handle_callback_output(result: CallbackOutputT, producer: Producer) -> None:
    if _is_callback_result_iterable(result):
        results_serialized = [
            pydantic_model_to_message(item)
            for item in cast(Iterable[BaseModel], result)
        ]
        for item_serialized in results_serialized:
            await producer.publish(item_serialized)
    else:
        if not isinstance(result, BaseModel):
            raise TypeError(
                f"Callback must return a BaseModel instance, got {type(result).__name__}"
            )
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


def _determine_full_node_name(
    node_name: str, pipeline_name: Optional[str] = None
) -> str:
    if pipeline_name is not None:
        return f"{pipeline_name}.{node_name}"
    else:
        return node_name
