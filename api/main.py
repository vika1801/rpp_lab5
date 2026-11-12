import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Literal

import aio_pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import desc, select, text

from database import SessionLocal, engine
from models import Account, Operation

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://loyalty:loyalty_pass@rabbitmq:5672/",
)

rabbit_connection = None
exchange = None


class OperationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation_id: str = Field(min_length=1)
    type: Literal["purchase", "return", "redeem"]
    customer_id: str = Field(min_length=1)
    store_id: str = Field(min_length=1)
    check_number: str | None = None
    amount: Decimal | None = None
    points: int | None = None
    occurred_at: datetime

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive(cls, value):
        if value is not None and value <= 0:
            raise ValueError("amount должен быть положительным")
        return value

    @field_validator("points")
    @classmethod
    def points_must_be_positive(cls, value):
        if value is not None and value <= 0:
            raise ValueError("points должен быть положительным")
        return value

    @model_validator(mode="after")
    def validate_fields_by_type(self):
        if self.type in ("purchase", "redeem") and self.amount is None:
            raise ValueError("Для purchase и redeem обязательно поле amount")
        if self.type == "return" and self.amount is not None:
            raise ValueError("Для return поле amount должно отсутствовать")
        if self.type == "redeem" and self.points is None:
            raise ValueError("Для redeem обязательно поле points")
        if self.type != "redeem" and self.points is not None:
            raise ValueError("Поле points допускается только для redeem")
        if self.type == "return" and not self.check_number:
            raise ValueError("Для возврата обязательно поле check_number")
        return self


@asynccontextmanager
async def lifespan(app: FastAPI):
    global rabbit_connection, exchange

    rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await rabbit_connection.channel(
        publisher_confirms=True,
        on_return_raises=True,
    )

    exchange = await channel.declare_exchange(
        "loyalty", aio_pika.ExchangeType.DIRECT, durable=True
    )
    queue = await channel.declare_queue(
        "operations",
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    await queue.bind(exchange, routing_key="operation")

    yield

    if rabbit_connection is not None:
        await rabbit_connection.close()
    await engine.dispose()


app = FastAPI(title="Loyalty API", lifespan=lifespan)


@app.post("/operations", status_code=202)
async def create_operation(operation: OperationRequest):
    data = operation.model_dump(mode="json", exclude_none=True)
    message = aio_pika.Message(
        body=json.dumps(data).encode("utf-8"),
        message_id=operation.operation_id,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        content_type="application/json",
    )

    try:
        # При publisher confirms ошибка публикации приводит к исключению.
        await exchange.publish(message, routing_key="operation", mandatory=True)
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="RabbitMQ не подтвердил публикацию сообщения",
        )

    return {"operation_id": operation.operation_id}


@app.get("/operations/{operation_id}")
async def get_operation(operation_id: str):
    async with SessionLocal() as session:
        item = await session.get(Operation, operation_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Операция ещё не обработана")

        return {
            "operation_id": item.operation_id,
            "status": item.status,
            "points_delta": item.points_delta,
            "balance_after": str(item.balance_after),
            "reject_reason": item.reject_reason,
            "check_number": item.check_number,
        }


@app.get("/accounts/{customer_id}")
async def get_account(customer_id: str):
    async with SessionLocal() as session:
        account = await session.get(Account, customer_id)
        if account is None:
            raise HTTPException(status_code=404, detail="Покупатель не найден")

        query = (
            select(Operation)
            .where(Operation.customer_id == customer_id)
            .order_by(desc(Operation.processed_at))
            .limit(20)
        )
        operations = (await session.scalars(query)).all()

        return {
            "customer_id": customer_id,
            "balance": str(account.balance),
            "operations": [
                {
                    "operation_id": item.operation_id,
                    "type": item.type,
                    "store_id": item.store_id,
                    "check_number": item.check_number,
                    "amount": str(item.amount) if item.amount is not None else None,
                    "points_delta": item.points_delta,
                    "balance_after": str(item.balance_after),
                    "status": item.status,
                    "reject_reason": item.reject_reason,
                    "occurred_at": item.occurred_at.isoformat(),
                    "processed_at": item.processed_at.isoformat(),
                    "check_number": item.check_number,
                }
                for item in operations
            ],
        }


@app.get("/health")
async def health():
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1"))
        if rabbit_connection is None or rabbit_connection.is_closed:
            raise RuntimeError("RabbitMQ недоступен")
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="Сервис временно недоступен")
