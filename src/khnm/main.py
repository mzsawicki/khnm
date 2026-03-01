import argparse
import asyncio
import importlib
import os

from aio_pika import connect_robust

from khnm.pipelines import Pipeline


def load_pipeline_object(import_string: str) -> Pipeline:
    module_name, attribute_name = import_string.split(":", 1)
    module = importlib.import_module(module_name)
    pipeline = getattr(module, attribute_name)
    return pipeline


async def run_node(
    pipeline: Pipeline,
    node_name: str,
    connection_string: str,
    tasks: int = 1,
    threads: int = 1,
) -> None:
    connection = await connect_robust(connection_string)
    await asyncio.gather(
        *[pipeline.run(connection, node_name, threads) for _ in range(tasks)]
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "pipeline",
        help="Where to look for pipeline object, in format 'module:attribute' (e.g. 'main:pipeline')",
    )
    parser.add_argument("node", help="Node name to run")
    parser.add_argument(
        "--tasks",
        type=int,
        default=1,
        help="Number of concurrent async tasks to run",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=1,
        help="Number of concurrent threads to run (sync callbacks only)",
    )
    args = parser.parse_args()

    conn_string = os.getenv("RABBITMQ_URL")
    if not conn_string:
        raise SystemExit("RABBITMQ_URL not set")
    if args.tasks < 1:
        raise SystemExit("Number of tasks must be greater than 0")
    if args.threads < 1:
        raise SystemExit("Number of threads must be greater than 0")

    pipeline = load_pipeline_object(args.pipeline)
    asyncio.run(run_node(pipeline, args.node, conn_string, args.tasks, args.threads))
