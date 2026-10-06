from datetime import datetime
from decimal import Decimal


def _json_default(value: object):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
