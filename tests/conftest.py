import datetime
from typing import Generator, AsyncGenerator

import pytest
from aio_pika import connect_robust, Message
from aio_pika.abc import AbstractRobustConnection
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from testcontainers.rabbitmq import RabbitMqContainer

from khnm.config import KhnmSettings
from tests.doubles import TestClock
from tests.utils import create_vhost, delete_vhost


@pytest.fixture(scope="session")
def rabbitmq_container() -> Generator[RabbitMqContainer, None, None]:
    with (
        RabbitMqContainer("rabbitmq:management")
        .with_exposed_ports(5672, 15672)
        .waiting_for(
            LogMessageWaitStrategy("Server startup complete")
        ) as rabbitmq_container
    ):
        yield rabbitmq_container


@pytest.fixture(scope="session")
def rabbitmq_host(rabbitmq_container: RabbitMqContainer) -> str:
    return rabbitmq_container.get_container_host_ip()


@pytest.fixture(scope="session")
def rabbitmq_port(rabbitmq_container: RabbitMqContainer) -> int:
    return rabbitmq_container.get_exposed_port(5672)


@pytest.fixture(scope="session")
def rabbitmq_management_port(rabbitmq_container: RabbitMqContainer) -> int:
    return rabbitmq_container.get_exposed_port(15672)


@pytest.fixture(scope="function")
async def rabbitmq_vhost(
    rabbitmq_host: str, rabbitmq_management_port: int
) -> AsyncGenerator[str, None]:
    url = f"http://{rabbitmq_host}:{rabbitmq_management_port}"
    vhost_name = "test"
    await create_vhost(url, vhost_name)
    yield vhost_name
    await delete_vhost(url, vhost_name)


@pytest.fixture(scope="function")
async def settings(
    rabbitmq_host: str, rabbitmq_port: int, rabbitmq_vhost: str
) -> KhnmSettings:
    return KhnmSettings(
        RABBITMQ_CONNECTION_STRING=f"amqp://guest:guest@{rabbitmq_host}:{rabbitmq_port}/{rabbitmq_vhost}"
    )


@pytest.fixture(scope="function")
async def amqp_connection(
    settings: KhnmSettings,
) -> AsyncGenerator[AbstractRobustConnection, None]:
    connection = await connect_robust(url=settings.RABBITMQ_CONNECTION_STRING)
    yield connection
    await connection.close()


@pytest.fixture(scope="session")
def sample_message() -> Message:
    return Message("Hello world".encode("utf-8"))


@pytest.fixture(scope="function")
def clock() -> TestClock:
    return TestClock(datetime.datetime(2026, 1, 25, 0, 0, 0))
