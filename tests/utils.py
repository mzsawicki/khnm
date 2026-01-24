import httpx


async def create_vhost(url: str, vhost_name: str) -> None:
    async with httpx.AsyncClient() as client:
        await client.put(f"{url}/api/vhosts/{vhost_name}", auth=("guest", "guest"))
        await client.put(f"{url}/api/vhosts/{vhost_name}/guest", auth=("guest", "guest"), json={
                "configure": ".*",
                "write": ".*",
                "read": ".*"
            }
        )


async def delete_vhost(url: str, vhost_name: str) -> None:
    async with httpx.AsyncClient() as client:
        await client.delete(f"{url}/api/vhosts/{vhost_name}", auth=("guest", "guest"))