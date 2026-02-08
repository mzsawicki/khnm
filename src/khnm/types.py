from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Union, Iterable, TypeVar, AsyncGenerator, Generator, Protocol

import pydantic
from aio_pika import Message
from aio_pika.abc import AbstractChannel, AbstractQueue


type SuccessT = bool
type SenderT = Callable[[AbstractChannel, Message, str], Awaitable[SuccessT]]
type QueueGetterT = Callable[[AbstractChannel, str], Awaitable[AbstractQueue]]
type ConsumerT = Callable[..., AbstractAsyncContextManager[Message]]
MessageObjectT = TypeVar("MessageObjectT", bound=pydantic.BaseModel)
type CallbackInputT = pydantic.BaseModel
type CallbackOutputT = Union[pydantic.BaseModel, Iterable[pydantic.BaseModel], None]
type SyncGeneratorCallbackT = Callable[..., Generator[CallbackOutputT, None, None]]
type AsyncGeneratorCallbackT = Callable[..., AsyncGenerator[CallbackOutputT]]
type GeneratorCallbackT = Union[SyncGeneratorCallbackT, AsyncGeneratorCallbackT]
type SyncCallbackT = Callable[..., CallbackOutputT]
type AsyncCallbackT = Callable[..., Awaitable[CallbackOutputT]]
type CallbackT = Union[
    SyncCallbackT,
    AsyncCallbackT,
]
