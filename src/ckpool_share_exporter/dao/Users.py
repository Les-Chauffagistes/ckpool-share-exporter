import asyncpg

from ckpool_share_exporter.models import UserStat


class UsersDAO:

    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def commit_user(self, instance_name: str, address: str, data: UserStat.DBModel):
        await self.pg.execute(
            """
            INSERT INTO users (address, pool_instance, hashrate1m, hashrate5m, hashrate1hr, hashrate1d, hashrate7d, lastshare, workers,
                               shares, bestshare, authorized)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (address, pool_instance) DO UPDATE SET (hashrate1m, hashrate5m, hashrate1hr, hashrate1d, hashrate7d, lastshare,
                                                 workers, shares, bestshare,
                                                 authorized) = ($3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            address, instance_name, data.hashrate1m, data.hashrate5m, data.hashrate1hr, data.hashrate1d, data.hashrate7d, data.lastshare, data.workers, data.shares, data.bestshare, data.authorized
        )
