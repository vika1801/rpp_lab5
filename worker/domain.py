from decimal import Decimal, ROUND_DOWN


def calculate_operation(operation, balance, purchase=None, already_returned=False):
    balance = Decimal(balance)
    points_delta = 0
    reject_reason = None

    if operation["type"] == "purchase":
        if operation.get("duplicate_purchase"):
            reject_reason = "duplicate_purchase"
        else:
            amount = Decimal(str(operation["amount"]))
            earned = (amount * Decimal("0.05")).to_integral_value(
                rounding=ROUND_DOWN
            )
            points_delta = int(earned)

    elif operation["type"] == "return":
        if purchase is None:
            reject_reason = "purchase_not_found"
        elif already_returned:
            reject_reason = "already_returned"
        else:
            points_delta = -int(purchase.points_delta)

    elif operation["type"] == "redeem":
        amount = Decimal(str(operation["amount"]))
        points = int(operation["points"])

        if Decimal(points) > amount * Decimal("0.30"):
            reject_reason = "limit_exceeded"
        elif Decimal(points) > balance:
            reject_reason = "insufficient_balance"
        else:
            points_delta = -points

    if reject_reason is not None:
        return {
            "status": "rejected",
            "points_delta": 0,
            "balance_after": balance,
            "reject_reason": reject_reason,
        }

    return {
        "status": "applied",
        "points_delta": points_delta,
        "balance_after": balance + Decimal(points_delta),
        "reject_reason": None,
    }
