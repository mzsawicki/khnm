import asyncio
import datetime
import typing
from typing import Any, Callable, Awaitable

import httpx
from aio_pika.abc import AbstractRobustConnection


async def create_vhost(url: str, vhost_name: str) -> None:
    async with httpx.AsyncClient() as client:
        await client.put(f"{url}/api/vhosts/{vhost_name}", auth=("guest", "guest"))
        await client.put(
            f"{url}/api/vhosts/{vhost_name}/guest",
            auth=("guest", "guest"),
            json={"configure": ".*", "write": ".*", "read": ".*"},
        )


async def delete_vhost(url: str, vhost_name: str) -> None:
    async with httpx.AsyncClient() as client:
        await client.delete(f"{url}/api/vhosts/{vhost_name}", auth=("guest", "guest"))


async def get_queue_messages_count(
    connection: AbstractRobustConnection, queue_name: str
) -> int:
    async with connection.channel() as channel:
        queue = await channel.declare_queue(queue_name, passive=True)
        count = queue.declaration_result.message_count
    assert count is not None
    return count


async def timeout(
    coro: Callable[..., Awaitable[Any]],
    /,
    *args: Any,
    awaited_result: Any,
    timeout_seconds: int,
    interval_seconds: int = 1,
) -> typing.Any:
    start_time = datetime.datetime.now()
    timeout_point = start_time + datetime.timedelta(seconds=timeout_seconds)
    result = None
    while result != awaited_result and datetime.datetime.now() < timeout_point:
        result = await coro(*args)
        await asyncio.sleep(interval_seconds)
    return result
