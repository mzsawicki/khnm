from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Union, Iterable, TypeVar, AsyncGenerator, Generator, Protocol

import pydantic
from aio_pika import Message
from aio_pika.abc import AbstractChannel, AbstractQueue


T_contravariant = TypeVar(
    "T_contravariant", bound=pydantic.BaseModel, contravariant=True
)
T_covariant = TypeVar("T_covariant", bound=pydantic.BaseModel, covariant=True)


class SyncCallbackProtocol(Protocol[T_contravariant, T_covariant]):
    def __call__(
        self, obj: T_contravariant
    ) -> Union[T_covariant, Iterable[T_covariant], None]: ...


class AsyncCallbackProtocol(Protocol[T_contravariant, T_covariant]):
    def __call__(
        self, obj: T_contravariant
    ) -> Awaitable[Union[T_covariant, Iterable[T_covariant], None]]: ...


type SuccessT = bool
type SenderT = Callable[[AbstractChannel, Message, str], Awaitable[SuccessT]]
type QueueGetterT = Callable[[AbstractChannel, str], Awaitable[AbstractQueue]]
type ConsumerT = Callable[..., AbstractAsyncContextManager[Message]]
MessageObjectT = TypeVar("MessageObjectT", bound=pydantic.BaseModel)
type CallbackInputT = pydantic.BaseModel
type CallbackOutputT = Union[pydantic.BaseModel, Iterable[pydantic.BaseModel]]
type SyncGeneratorCallbackT = Callable[..., Generator[CallbackOutputT, None, None]]
type AsyncGeneratorCallbackT = Callable[..., AsyncGenerator[CallbackOutputT]]
type GeneratorCallbackT = Union[SyncGeneratorCallbackT, AsyncGeneratorCallbackT]
type SyncCallbackT = Callable[[CallbackInputT], CallbackOutputT]
type AsyncCallbackT = Callable[[CallbackInputT], Awaitable[CallbackOutputT]]
type CallbackT = Union[
    SyncCallbackProtocol,
    AsyncCallbackProtocol,
]
