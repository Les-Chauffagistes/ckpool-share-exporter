from aiohttp import web

from ckpool_share_exporter.dao import ShareWeightDAO

SHARE_WEIGHT_DAO = web.AppKey("share_weight_dao", ShareWeightDAO)
