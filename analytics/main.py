import asyncio
import json
import os
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI, HTTPException

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://loyalty:loyalty_pass@rabbitmq:5672/",
)

received_events = 0
stores = {}
rabbit_connection = None
consumer_task = None


def empty_store_stats(store_id):
    return {
        "store_id": store_id,
        "accepted_operations": {
            "purchase": 0,
            "return": 0,
            "redeem": 0,
        },
        "rejected_operations": 0,
        "points_accrued": 0,
        "points_reversed": 0,
        "points_spent": 0,
    }


async def consume_events():
    global received_events, rabbit_connection

    rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await rabbit_connection.channel()
    await channel.set_qos(prefetch_count=1)

    exchange = await channel.declare_exchange(
        "loyalty.events",
        aio_pika.ExchangeType.FANOUT,
        durable=True,
    )
    queue = await channel.declare_queue(
        "analytics.events",
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    await queue.bind(exchange)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process(requeue=True):
                event = json.loads(message.body.decode("utf-8"))
                received_events += 1

                store_id = event["store_id"]
                if store_id not in stores:
                    stores[store_id] = empty_store_stats(store_id)

                stats = stores[store_id]

                if event["status"] == "rejected":
                    stats["rejected_operations"] += 1
                    continue

                operation_type = event["type"]
                stats["accepted_operations"][operation_type] += 1
                delta = int(event["points_delta"])

                if operation_type == "purchase" and delta > 0:
                    stats["points_accrued"] += delta
                elif operation_type == "return":
                    stats["points_reversed"] += abs(delta)
                elif operation_type == "redeem":
                    stats["points_spent"] += abs(delta)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_task

    consumer_task = asyncio.create_task(consume_events())
    yield

    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

    if rabbit_connection is not None:
        await rabbit_connection.close()


app = FastAPI(title="Loyalty Analytics", lifespan=lifespan)


@app.get("/stores")
async def get_stores():
    return list(stores.values())


@app.get("/stores/{store_id}")
async def get_store(store_id: str):
    if store_id not in stores:
        raise HTTPException(status_code=404, detail="Магазин не найден")
    return stores[store_id]


@app.get("/stats")
async def get_stats():
    return {"received_events": received_events}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "analytics"}
