import json
from decimal import Decimal

from aiohttp import web

from ckpool_share_exporter.dao import ShareWeightDAO
from ckpool_share_exporter.server.v1 import SHARE_WEIGHT_DAO
from ckpool_share_exporter.settings import settings


def _json_default(value: object) -> float:
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


async def distribution(request: web.Request) -> web.Response:
    username = request.match_info["username"]
    raw_window_days = request.query.get("window_days")
    if raw_window_days is None:
        window_days = settings.distribution_window_days
    else:
        try:
            window_days = int(raw_window_days)
        except ValueError:
            raise web.HTTPBadRequest(text="window_days must be a positive integer") from None
        if window_days < 1:
            raise web.HTTPBadRequest(text="window_days must be a positive integer")

    dao: ShareWeightDAO = request.app[SHARE_WEIGHT_DAO]
    rows = await dao.cluster_distribution(
        username,
        window_days=window_days,
    )
    return web.json_response(
        [dict(row) for row in rows],
        dumps=lambda value: json.dumps(value, default=_json_default),
    )
