from typing import Union

import pydantic
from aio_pika import Message
from aio_pika.abc import AbstractIncomingMessage

from khnm.types import MessageObjectT


def pydantic_model_to_message(
    obj: pydantic.BaseModel,
) -> Message:
    bytes_ = obj.model_dump_json().encode("utf-8")
    return Message(bytes_)


def message_to_pydantic_model(
    message: Union[Message, AbstractIncomingMessage], model_class: type[MessageObjectT]
) -> MessageObjectT:
    decoded = message.body.decode("utf-8")
    obj = model_class.model_validate_json(decoded)
    return obj
