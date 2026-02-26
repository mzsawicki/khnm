import asyncio
import random
import uuid
from typing import AsyncGenerator, List

import pydantic

import khnm


class Task(pydantic.BaseModel):
    correlation_id: uuid.UUID = uuid.uuid4()
    data: List[int] = pydantic.Field(default_factory=list)


class SplitTask(pydantic.BaseModel):
    correlation_id: uuid.UUID = uuid.uuid4()
    data: int


async def generate() -> AsyncGenerator[Task, None]:
    for _ in range(100):
        numbers = [random.randint(1, 10) for _ in range(10)]
        yield Task(data=numbers)


def split(task: Task) -> List[SplitTask]:
    tasks = []
    for number in task.data:
        tasks.append(SplitTask(correlation_id=task.correlation_id, data=number))
    return tasks


async def sleep(task: SplitTask) -> SplitTask:
    await asyncio.sleep(random.randint(1, 10))
    return task


def multiply(task: SplitTask) -> SplitTask:
    return SplitTask(data=task.data * 100)


def print_result(task: SplitTask) -> None:
    print(task.data)


pipeline = (
    khnm.make_pipeline()
    .add("generator", generate, pipe_length=1024)
    .add("splitter", split, pipe_length=2048)
    .add("sleeper", sleep, pipe_length=5076)
    .add("multiplier", multiply, pipe_length=1152)
    .add("printer", print_result)
    .build()
)
