from collections.abc import Awaitable, Callable

from aio_pika import Message
from aio_pika.abc import AbstractChannel


type SuccessT = bool
type SenderT = Callable[[AbstractChannel, Message, str], Awaitable[SuccessT]]
