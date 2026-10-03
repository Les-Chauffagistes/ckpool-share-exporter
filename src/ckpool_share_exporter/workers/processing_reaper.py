import asyncpg

from chauff_cmn.logging import logger as log
from ckpool_share_exporter.dao import FileDAO
from ckpool_share_exporter.settings import settings


async def release_processing_sharelogs(pg: asyncpg.Pool):
    released = await FileDAO(pg).release_expired(
        settings.pool_instance_name, settings.processing_timeout_seconds)
    if released:
        log.warning(
            f"{released} sharelogs released from PROCESSING after "
            f"{settings.processing_timeout_seconds}s"
        )
