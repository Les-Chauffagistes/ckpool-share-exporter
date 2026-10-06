from aiohttp import web
from chauff_cmn.logging.aiohttp import request_logging_middleware

from ckpool_share_exporter.dao import PoolStatDAO, ShareWeightDAO
from ckpool_share_exporter.server.v1.app import create_v1_app


async def health(_request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


def create_app(dao: ShareWeightDAO, pool_stat_dao: PoolStatDAO) -> web.Application:
    app = web.Application(middlewares=[request_logging_middleware])
    app.router.add_get("/health", health)
    app.add_subapp("/v1", create_v1_app(dao, pool_stat_dao))
    return app
