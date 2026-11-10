from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Index, Integer, Numeric, String, and_
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "accounts"

    customer_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    balance: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, default=Decimal("0")
    )


class Operation(Base):
    __tablename__ = "operations"

    operation_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    type: Mapped[str] = mapped_column(String(20), nullable=False)
    customer_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    store_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    check_number: Mapped[str] = mapped_column(String(100), nullable=False)
    amount: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    points_delta: Mapped[int] = mapped_column(Integer, nullable=False)
    balance_after: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    reject_reason: Mapped[str | None] = mapped_column(String(50), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


# Уникальность номера чека проверяется только среди принятых покупок.
Index(
    "uq_purchase_check",
    Operation.check_number,
    unique=True,
    postgresql_where=and_(
        Operation.type == "purchase",
        Operation.status == "applied",
    ),
)
