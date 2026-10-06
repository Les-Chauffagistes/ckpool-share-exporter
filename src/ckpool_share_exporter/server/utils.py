from decimal import Decimal


def _json_default(value: object) -> float:
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
