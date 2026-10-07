import json

from aiohttp import web

from ckpool_share_exporter.dao.PoolStat import PoolStatDAO
from ckpool_share_exporter.server.utils import _json_default
from ckpool_share_exporter.server.v1 import POOL_STAT_DAO


async def stats(request: web.Request):
    cluster = request.query.get("cluster")
    dao: PoolStatDAO = request.app[POOL_STAT_DAO]

    if cluster is None:
        rows = await dao.get_cluster_stat()

    else:
        rows = await dao.get_instance_stat(cluster)

    return web.json_response(
        [dict(row) for row in rows],
        dumps = lambda value: json.dumps(value, default = _json_default),
    )
