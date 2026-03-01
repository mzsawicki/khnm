# khnm

A pipeline framework for building message-processing workflows on top of RabbitMQ. Define pipelines as chains of callbacks connected by queues, and run each stage independently with configurable concurrency.

## Requirements

- Python >= 3.14
- RabbitMQ

## Installation

```bash
pip install khnm
```

## Quick start

Define your data models and callbacks, then chain them into a pipeline:

```python
import khnm
import pydantic
from typing import AsyncGenerator, List


class Task(pydantic.BaseModel):
    data: List[int]


class Result(pydantic.BaseModel):
    data: int


async def generate() -> AsyncGenerator[Task, None]:
    for i in range(100):
        yield Task(data=[i, i + 1, i + 2])


def split(task: Task) -> List[Result]:
    return [Result(data=n) for n in task.data]


def print_result(result: Result) -> None:
    print(result.data)


pipeline = (
    khnm.make_pipeline()
    .add("generator", generate, pipe_length=128)
    .add("splitter", split, pipe_length=128, prefetch_count=1)
    .add("printer", print_result, prefetch_count=1)
    .build()
)
```

Run each stage as a separate process:

```bash
export RABBITMQ_URL="amqp://guest:guest@localhost/"

khnm my_app:pipeline generator
khnm my_app:pipeline splitter
khnm my_app:pipeline printer
```

## Pipeline structure

A pipeline is an ordered chain of stages:

- **Source** (first stage) -- a sync or async generator that produces messages. Runs until the generator is exhausted, then exits. It does not restart or reconnect; when the source process finishes, no further messages will be produced.
- **Node** (middle stages) -- a sync or async function that consumes a message and returns one, many, or no output messages
- **Sink** (last stage) -- a sync or async function that consumes messages without forwarding

All messages are [Pydantic](https://docs.pydantic.dev/) `BaseModel` instances, serialized automatically between stages via RabbitMQ queues.

## Callbacks

Callbacks are automatically detected as sync or async. Both styles work interchangeably:

```python
# Sync
def process(task: Task) -> Result:
    return Result(data=task.data * 2)

# Async
async def process(task: Task) -> Result:
    await some_io()
    return Result(data=task.data * 2)
```

A callback can return:
- A single `BaseModel` -- published as one message
- A list of `BaseModel` -- each item published as a separate message
- `None` -- nothing is published (filtering)

## Concurrency

### Async tasks

Run multiple concurrent asyncio tasks per process with `--tasks`:

```bash
khnm my_app:pipeline splitter --tasks 4
```

Each task gets its own consumer loop. This is the primary concurrency lever for async callbacks.

### Threads

Run sync callbacks on a thread pool with `--threads`:

```bash
khnm my_app:pipeline splitter --threads 4
```

This offloads blocking sync callbacks to a `ThreadPoolExecutor`, preventing them from blocking the event loop. Has no effect on async callbacks.

## Per-stage options

Options are passed as keyword arguments to `.add()`:

```python
khnm.make_pipeline()
    .add("source", generate, pipe_length=256, durable=True)
    .add("worker", process,
         pipe_length=128,
         prefetch_count=10,
         backoff_seconds=0.5,
         max_retries=5,
         exponential_backoff=True,
         max_backoff_seconds=30.0,
         apply_jitter=True,
         connection_max_retries=10,
         connection_backoff_seconds=2.0)
    .add("sink", output, prefetch_count=1)
    .build()
```

| Option | Applies to | Description                                                                                                                                |
|---|---|--------------------------------------------------------------------------------------------------------------------------------------------|
| `pipe_length` | Source, Node | Max queue length                                                                                                                           |
| `durable` | Source, Node | Persist messages to disk                                                                                                                   |
| `prefetch_count` | Node, Sink | RabbitMQ QoS -- max unacknowledged messages per consumer. Important! If you use multiple threads, set this to no lower than threads count! |
| `backoff_seconds` | Source, Node | Initial backoff for publish retries                                                                                                        |
| `max_retries` | Source, Node | Max publish retry attempts                                                                                                                 |
| `exponential_backoff` | Source, Node | Enable exponential backoff                                                                                                                 |
| `max_backoff_seconds` | Source, Node | Cap on backoff duration                                                                                                                    |
| `apply_jitter` | Source, Node | Add randomness to backoff                                                                                                                  |
| `connection_max_retries` | Node, Sink | Max connection retry attempts                                                                                                              |
| `connection_backoff_seconds` | Node, Sink | Backoff between connection retries                                                                                                         |

## CLI reference:

```
khnm <module:pipeline> <node> [--tasks N] [--threads N]
```

- `module:pipeline` -- import path to the built `Pipeline` object (e.g. `my_app:pipeline`)
- `node` -- name of the stage to run
- `--tasks N` -- number of concurrent async tasks (default: 1)
- `--threads N` -- number of threads for sync callbacks (default: 1)

The `RABBITMQ_URL` environment variable must be set.
