from aiohttp import web

from ckpool_share_exporter.dao import ShareWeightDAO
from ckpool_share_exporter.dao.PoolStat import PoolStatDAO

SHARE_WEIGHT_DAO = web.AppKey("share_weight_dao", ShareWeightDAO)
POOL_STAT_DAO = web.AppKey("pool_stat_dao", PoolStatDAO)