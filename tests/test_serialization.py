from khnm.serialization import pydantic_model_to_message, message_to_pydantic_model
from tests.doubles import SampleDataObject


async def test_string_is_serialized_correctly(
    data_object: SampleDataObject = SampleDataObject(),
) -> None:
    message = pydantic_model_to_message(data_object)
    deserialized_obj = message_to_pydantic_model(message, SampleDataObject)
    assert deserialized_obj.text == data_object.text


async def test_uuid_is_serialized_correctly(
    data_object: SampleDataObject = SampleDataObject(),
) -> None:
    message = pydantic_model_to_message(data_object)
    deserialized_obj = message_to_pydantic_model(message, SampleDataObject)
    assert deserialized_obj.guid == data_object.guid


async def test_time_is_serialized_correctly(
    data_object: SampleDataObject = SampleDataObject(),
) -> None:
    message = pydantic_model_to_message(data_object)
    deserialized_obj = message_to_pydantic_model(message, SampleDataObject)
    assert deserialized_obj.time == data_object.time


async def test_integer_is_serialized_correctly(
    data_object: SampleDataObject = SampleDataObject(),
) -> None:
    message = pydantic_model_to_message(data_object)
    deserialized_obj = message_to_pydantic_model(message, SampleDataObject)
    assert deserialized_obj.integer == data_object.integer


async def test_float_is_serialized_correctly(
    data_object: SampleDataObject = SampleDataObject(),
) -> None:
    message = pydantic_model_to_message(data_object)
    deserialized_obj = message_to_pydantic_model(message, SampleDataObject)
    assert deserialized_obj.float_ == data_object.float_


async def test_decimal_is_serialized_correctly(
    data_object: SampleDataObject = SampleDataObject(),
) -> None:
    message = pydantic_model_to_message(data_object)
    deserialized_obj = message_to_pydantic_model(message, SampleDataObject)
    assert deserialized_obj.decimal_ == data_object.decimal_


async def test_nested_object_is_serialized_correctly(
    data_object: SampleDataObject = SampleDataObject(),
) -> None:
    message = pydantic_model_to_message(data_object)
    deserialized_obj = message_to_pydantic_model(message, SampleDataObject)
    assert deserialized_obj.obj.text == data_object.obj.text
