import asyncio
import json
import logging
import os
import random
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI, Query

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://loyalty:loyalty_pass@rabbitmq:5672/",
)
MIN_DELAY_MS = int(os.getenv("MIN_DELAY_MS", "200"))
MAX_DELAY_MS = int(os.getenv("MAX_DELAY_MS", "1500"))
UNSUBSCRIBED = {
    item.strip()
    for item in os.getenv("UNSUBSCRIBED", "").split(",")
    if item.strip()
}

notifications = []
received_events = 0
sent_notifications = 0
rabbit_connection = None
consumer_task = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("notifier")


def notification_text(event):
    if event["status"] == "rejected":
        return f"Операция отклонена: {event['reject_reason']}"

    delta = int(event["points_delta"])
    balance = event["balance_after"]

    if delta > 0:
        return f"Начислено {delta} баллов, баланс {balance}"

    if delta < 0:
        return f"Списано {abs(delta)} баллов, баланс {balance}"

    return f"Операция выполнена, баланс {balance}"


async def consume_events():
    global received_events, sent_notifications, rabbit_connection

    rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await rabbit_connection.channel()
    await channel.set_qos(prefetch_count=1)

    exchange = await channel.declare_exchange(
        "loyalty.events",
        aio_pika.ExchangeType.FANOUT,
        durable=True,
    )
    queue = await channel.declare_queue(
        "notifier.events",
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    await queue.bind(exchange)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process(requeue=True):
                event = json.loads(message.body.decode("utf-8"))
                received_events += 1

                customer_id = event["customer_id"]

                if customer_id in UNSUBSCRIBED:
                    logger.info(
                        "Уведомление не отправлено: покупатель %s отписан",
                        customer_id,
                    )
                    continue

                delay = random.uniform(MIN_DELAY_MS, MAX_DELAY_MS) / 1000
                await asyncio.sleep(delay)

                notification = {
                    "operation_id": event["operation_id"],
                    "customer_id": customer_id,
                    "store_id": event["store_id"],
                    "text": notification_text(event),
                    "created_at": event["processed_at"],
                }

                notifications.append(notification)
                sent_notifications += 1
                logger.info("Отправлено уведомление: %s", notification)


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


app = FastAPI(title="Loyalty Notifier", lifespan=lifespan)


@app.get("/notifications")
async def get_notifications(customer_id: str = Query(...)):
    return [
        item for item in notifications
        if item["customer_id"] == customer_id
    ]


@app.get("/stats")
async def get_stats():
    return {
        "received_events": received_events,
        "sent_notifications": sent_notifications,
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "notifier"}
