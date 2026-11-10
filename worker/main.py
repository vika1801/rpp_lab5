import asyncio
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import aio_pika
from sqlalchemy import select

from database import SessionLocal, engine
from models import Account, Base, Operation
from domain import calculate_operation

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://loyalty:loyalty_pass@rabbitmq:5672/",
)


def make_event(record):
    """Формирует событие по уже сохранённой операции."""
    return {
        "operation_id": record.operation_id,
        "type": record.type,
        "customer_id": record.customer_id,
        "store_id": record.store_id,
        "status": record.status,
        "points_delta": record.points_delta,
        "balance_after": str(record.balance_after),
        "reject_reason": record.reject_reason,
        "processed_at": record.processed_at.isoformat(),
    }


async def process_operation(data):
    """
    Сначала фиксирует операцию в PostgreSQL.
    Возвращает событие только после успешного завершения транзакции.
    """
    async with SessionLocal() as session:
        async with session.begin():
            old_record = await session.get(Operation, data["operation_id"])

            # Повторная доставка после сбоя публикации события:
            # повторно используем сохранённый результат и публикуем событие ещё раз.
            if old_record is not None:
                return make_event(old_record)

            account = await session.get(Account, data["customer_id"])
            if account is None:
                account = Account(
                    customer_id=data["customer_id"],
                    balance=Decimal("0"),
                )
                session.add(account)
                await session.flush()

            purchase = None
            already_returned = False
            duplicate_purchase = False

            if data["type"] == "purchase":
                query = select(Operation).where(
                    Operation.type == "purchase",
                    Operation.check_number == data["check_number"],
                    Operation.status == "applied",
                )
                duplicate_purchase = (await session.scalar(query)) is not None

            elif data["type"] == "return":
                query = select(Operation).where(
                    Operation.type == "purchase",
                    Operation.check_number == data["check_number"],
                    Operation.status == "applied",
                )
                purchase = await session.scalar(query)

                if purchase is not None:
                    return_query = select(Operation).where(
                        Operation.type == "return",
                        Operation.check_number == data["check_number"],
                        Operation.status == "applied",
                    )
                    already_returned = (
                        await session.scalar(return_query)
                    ) is not None

            domain_data = dict(data)
            domain_data["duplicate_purchase"] = duplicate_purchase

            result = calculate_operation(
                domain_data,
                account.balance,
                purchase=purchase,
                already_returned=already_returned,
            )

            account.balance = result["balance_after"]

            occurred_at = datetime.fromisoformat(data["occurred_at"])
            if occurred_at.tzinfo is None:
                occurred_at = occurred_at.replace(tzinfo=timezone.utc)

            record = Operation(
                operation_id=data["operation_id"],
                type=data["type"],
                customer_id=data["customer_id"],
                store_id=data["store_id"],
                check_number=data["check_number"],
                amount=(
                    Decimal(data["amount"])
                    if data.get("amount") is not None
                    else None
                ),
                points_delta=result["points_delta"],
                balance_after=result["balance_after"],
                status=result["status"],
                reject_reason=result["reject_reason"],
                occurred_at=occurred_at,
                processed_at=datetime.now(timezone.utc),
            )
            session.add(record)

        # Выходим из session.begin(): транзакция уже зафиксирована.
        return make_event(record)


async def handle_message(message, event_exchange):
    async with message.process(requeue=True):
        data = json.loads(message.body.decode("utf-8"))

        # Сначала сохраняем результат операции в БД.
        event_data = await process_operation(data)

        # Затем публикуем событие и ждём подтверждения RabbitMQ.
        event_message = aio_pika.Message(
            body=json.dumps(event_data).encode("utf-8"),
            message_id=event_data["operation_id"],
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        )
        await event_exchange.publish(
            event_message,
            routing_key="",
            mandatory=False,
        )
        # ACK исходного сообщения произойдёт после выхода из message.process.


async def main():
    # Создание таблиц автоматически при запуске worker.
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel(publisher_confirms=True)
    await channel.set_qos(prefetch_count=1)

    # Очередь входящих операций.
    direct_exchange = await channel.declare_exchange(
        "loyalty",
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )
    operations_queue = await channel.declare_queue(
        "operations",
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    await operations_queue.bind(direct_exchange, routing_key="operation")

    # Fanout-обменник и обе очереди событий объявляются до начала обработки.
    event_exchange = await channel.declare_exchange(
        "loyalty.events",
        aio_pika.ExchangeType.FANOUT,
        durable=True,
    )
    notifier_queue = await channel.declare_queue(
        "notifier.events",
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    analytics_queue = await channel.declare_queue(
        "analytics.events",
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    await notifier_queue.bind(event_exchange)
    await analytics_queue.bind(event_exchange)

    # Файл используется Docker healthcheck: таблицы и очереди уже готовы.
    with open("/tmp/worker-ready", "w", encoding="utf-8") as file:
        file.write("ready")

    print("Worker запущен. Ожидание операций...")
    await operations_queue.consume(
        lambda message: handle_message(message, event_exchange)
    )

    try:
        await asyncio.Future()
    finally:
        await connection.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
