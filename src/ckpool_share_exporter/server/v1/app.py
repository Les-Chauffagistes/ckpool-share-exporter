from aiohttp import web

from ckpool_share_exporter.dao import ShareWeightDAO
from ckpool_share_exporter.dao.PoolStat import PoolStatDAO
from ckpool_share_exporter.server.v1 import POOL_STAT_DAO, SHARE_WEIGHT_DAO
from ckpool_share_exporter.server.v1.repartition import distribution
from ckpool_share_exporter.server.v1.stats import stats


def create_v1_app(dao: ShareWeightDAO, pool_stat_dao: PoolStatDAO) -> web.Application:
    app = web.Application()
    app[SHARE_WEIGHT_DAO] = dao
    app[POOL_STAT_DAO] = pool_stat_dao
    app.router.add_get("/distribution/{username}", distribution)
    app.router.add_get("/stats", stats)
    return app
