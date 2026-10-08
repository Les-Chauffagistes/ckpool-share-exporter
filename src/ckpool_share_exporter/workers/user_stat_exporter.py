from ckpool_share_exporter.models import UserStat
from os import listdir

import asyncpg

from ckpool_share_exporter.dao.Users import UsersDAO
from ckpool_share_exporter.settings import settings
from aiofiles import open
from json import loads
from chauff_cmn.logging import logger as log


async def export_user_stat(pg: asyncpg.Pool):
    users_dao = UsersDAO(pg)
    base_path = f"{settings.base_log_dir}/users"
    users = listdir(base_path)
    for user in users:
        try:
            user_stat_file = f"{base_path}/{user}"
            async with open(user_stat_file, "r") as f:
                user_stat = UserStat(**loads(await f.read()))
                await users_dao.commit_user(user, user_stat.to_database_row(user))
        except:
            log.exception("Failed to export user stat")
            continue
