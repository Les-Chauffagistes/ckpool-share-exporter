from aiohttp import web

from ckpool_share_exporter.dao import ShareWeightDAO
from ckpool_share_exporter.server.v1 import SHARE_WEIGHT_DAO
from ckpool_share_exporter.server.v1.repartition import distribution


def create_v1_app(dao: ShareWeightDAO) -> web.Application:
    app = web.Application()
    app[SHARE_WEIGHT_DAO] = dao
    app.router.add_get("/distribution/{username}", distribution)
    return app
