from datetime import datetime, UTC

import asyncpg

from ckpool_share_exporter.models import PoolStat

# Remplacement: pool_stat ne garde que le dernier instantane de chaque instance,
# sans historique.
_UPSERT_POOL_STAT = """
                    INSERT INTO pool_stat
                    (pool_instance, updated_at, runtime_s, users, workers, idle, disconnected,
                     hashrate_1m, hashrate_5m, hashrate_15m, hashrate_1h, hashrate_6h, hashrate_1d, hashrate_7d,
                     diff, accepted, rejected, bestshare, sps_1m, sps_5m, sps_15m, sps_1h)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                            $15, $16, $17, $18, $19, $20, $21, $22)
                    ON CONFLICT (pool_instance) DO UPDATE
                        SET updated_at   = excluded.updated_at,
                            runtime_s    = excluded.runtime_s,
                            users        = excluded.users,
                            workers      = excluded.workers,
                            idle         = excluded.idle,
                            disconnected = excluded.disconnected,
                            hashrate_1m  = excluded.hashrate_1m,
                            hashrate_5m  = excluded.hashrate_5m,
                            hashrate_15m = excluded.hashrate_15m,
                            hashrate_1h  = excluded.hashrate_1h,
                            hashrate_6h  = excluded.hashrate_6h,
                            hashrate_1d  = excluded.hashrate_1d,
                            hashrate_7d  = excluded.hashrate_7d,
                            diff         = excluded.diff,
                            accepted     = excluded.accepted,
                            rejected     = excluded.rejected,
                            bestshare    = excluded.bestshare,
                            sps_1m       = excluded.sps_1m,
                            sps_5m       = excluded.sps_5m,
                            sps_15m      = excluded.sps_15m,
                            sps_1h       = excluded.sps_1h \
                    """


class PoolStatDAO:
    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def upsert(self, stat: PoolStat, pool_instance: str) -> None:
        """Remplace l'instantane de l'instance par celui-ci."""
        await self.pg.execute(
            _UPSERT_POOL_STAT,
            pool_instance, datetime.fromtimestamp(stat.lastupdate, UTC),
            stat.runtime, stat.users, stat.workers, stat.idle, stat.disconnected,
            stat.hashrate1m, stat.hashrate5m, stat.hashrate15m, stat.hashrate1hr,
            stat.hashrate6hr, stat.hashrate1d, stat.hashrate7d,
            stat.diff, stat.accepted, stat.rejected, stat.bestshare,
            stat.SPS1m, stat.SPS5m, stat.SPS15m, stat.SPS1h,
        )

    async def get_instance_stat(self, pool_instance: str):
        return await self.pg.fetch(
            """SELECT *
               FROM pool_stat
               WHERE pool_instance = $1""",
            pool_instance,
        )

    async def get_cluster_stat(self):
        return await self.pg.fetch(
            """SELECT *
               FROM pool_stat""",
        )
