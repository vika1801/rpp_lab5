import asyncio
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import aio_pika
from sqlalchemy import select, text

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

    async with SessionLocal() as session:
        async with session.begin():

            old_record = await session.get(
                Operation,
                data["operation_id"]
            )

            if old_record is not None:
                return make_event(old_record)

            purchase = None
            already_returned = False
            duplicate_purchase = False

            domain_data = data.copy()

            # Поиск исходной покупки при возврате
            if data["type"] == "return":
                query = select(Operation).where(
                    Operation.type == "purchase",
                    Operation.check_number == data["check_number"],
                    Operation.status == "applied",
                )

                purchase = await session.scalar(query)

            # Проверка, не был ли чек уже возвращён
            if purchase is not None:
                return_query = select(Operation).where(
                    Operation.type == "return",
                    Operation.check_number == data["check_number"],
                    Operation.status == "applied",
                )

                already_returned = (
                    await session.scalar(return_query)
                ) is not None

            # При возврате определяем покупателя по исходному чеку
            if data["type"] == "return" and purchase is not None:
                customer_id = purchase.customer_id
            else:
                customer_id = data["customer_id"]

            # Получение счёта покупателя
            account = await session.get(Account, customer_id)

            if account is None:
                account = Account(
                    customer_id=customer_id,
                    balance=Decimal("0"),
                )

                session.add(account)
                await session.flush()

            domain_data["duplicate_purchase"] = duplicate_purchase

            # Расчёт результата операции
            result = calculate_operation(
                domain_data,
                account.balance,
                purchase=purchase,
                already_returned=already_returned,
            )

            # Генерация общего номера для новой покупки
            if data["type"] == "purchase":
                counter_result = await session.execute(
                    text(
                        "UPDATE receipt_counter "
                        "SET last_number = last_number + 1 "
                        "WHERE id = 1 "
                        "RETURNING last_number"
                    )
                )

                data["check_number"] = str(
                    counter_result.scalar_one()
                )

            # Обновление баланса
            account.balance = result["balance_after"]

            occurred_at = datetime.fromisoformat(data["occurred_at"])

            if occurred_at.tzinfo is None:
                occurred_at = occurred_at.replace(
                    tzinfo=timezone.utc
                )

            # Создание записи об операции
            record = Operation(
                operation_id=data["operation_id"],
                type=data["type"],
                customer_id=customer_id,
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

        return make_event(record)


async def handle_message(message, event_exchange):
    async with message.process(requeue=True):
        data = json.loads(message.body.decode("utf-8"))

        event_data = await process_operation(data)

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


async def main():

    # Создание таблиц базы данных
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    # Инициализация счётчика чеков
    async with engine.begin() as connection:
        await connection.execute(
            text("""
                INSERT INTO receipt_counter (id, last_number)
                SELECT 1, COALESCE(
                    MAX(
                        CASE
                            WHEN check_number ~ '^[0-9]+$'
                            THEN check_number::BIGINT
                        END
                    ), 0
                )
                FROM operations
                WHERE type = 'purchase'
                  AND status = 'applied'
                ON CONFLICT (id) DO NOTHING
            """)
        )

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel(publisher_confirms=True)

    await channel.set_qos(prefetch_count=1)

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

    await operations_queue.bind(
        direct_exchange,
        routing_key="operation",
    )

    # Fanout-обменник и обе очереди событий
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