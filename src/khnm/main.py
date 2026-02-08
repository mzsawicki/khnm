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


async def run_node(pipeline: Pipeline, node_name: str, connection_string: str) -> None:
    connection = await connect_robust(connection_string)
    await pipeline.run(connection, node_name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "pipeline",
        help="Where to look for pipeline object, in format 'module:attribute' (e.g. 'main:pipeline')",
    )
    parser.add_argument("node", help="Node name to run")
    args = parser.parse_args()

    conn_string = os.getenv("RABBITMQ_URL")
    assert conn_string

    pipeline = load_pipeline_object(args.pipeline)
    asyncio.run(run_node(pipeline, args.node, conn_string))
