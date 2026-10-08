import asyncpg

from ckpool_share_exporter.models import UserStat, WorkerStat

# IS DISTINCT FROM: un worker inactif a des stats identiques d'un tick a l'autre,
# or un UPDATE sans condition reecrit la ligne (tuple mort, WAL) meme si rien
# n'a change.
_UPSERT_WORKERS = """
                  INSERT INTO user_workers (workername, pool_instance, address, hashrate1m, hashrate5m, hashrate1hr,
                                            hashrate1d, hashrate7d, lastshare, shares, bestshare)
                  SELECT workername, $1, $2, hashrate1m, hashrate5m, hashrate1hr, hashrate1d, hashrate7d, lastshare,
                         shares, bestshare
                  FROM unnest($3::text[], $4::bigint[], $5::bigint[], $6::bigint[], $7::bigint[], $8::bigint[],
                              $9::timestamptz[], $10::bigint[], $11::bigint[])
                      AS t(workername, hashrate1m, hashrate5m, hashrate1hr, hashrate1d, hashrate7d, lastshare, shares,
                           bestshare)
                  ON CONFLICT (workername, pool_instance) DO UPDATE SET (address, hashrate1m, hashrate5m, hashrate1hr,
                                                                         hashrate1d, hashrate7d, lastshare, shares,
                                                                         bestshare)
                      = (excluded.address, excluded.hashrate1m, excluded.hashrate5m, excluded.hashrate1hr,
                         excluded.hashrate1d, excluded.hashrate7d, excluded.lastshare, excluded.shares,
                         excluded.bestshare)
                  WHERE (user_workers.address, user_workers.hashrate1m, user_workers.hashrate5m,
                         user_workers.hashrate1hr, user_workers.hashrate1d, user_workers.hashrate7d,
                         user_workers.lastshare, user_workers.shares, user_workers.bestshare)
                      IS DISTINCT FROM
                        (excluded.address, excluded.hashrate1m, excluded.hashrate5m, excluded.hashrate1hr,
                         excluded.hashrate1d, excluded.hashrate7d, excluded.lastshare, excluded.shares,
                         excluded.bestshare) \
                  """


class UsersDAO:

    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def commit_user(self, instance_name: str, address: str, data: UserStat.DBModel,
                          workers: list[WorkerStat.DBModel] | None = None):
        """Ecrit la ligne de l'adresse et celles de ses workers dans une seule transaction,
        pour que users.workers ne diverge pas de user_workers."""
        async with self.pg.acquire() as connection, connection.transaction():
            await connection.execute(
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
            if workers:
                # Un workername en double dans un meme INSERT ... ON CONFLICT leve
                # "cannot affect row a second time": le dernier l'emporte.
                unique = list({worker.workername: worker for worker in workers}.values())
                await connection.execute(
                    _UPSERT_WORKERS, instance_name, address,
                    [w.workername for w in unique],
                    [w.hashrate1m for w in unique], [w.hashrate5m for w in unique],
                    [w.hashrate1hr for w in unique], [w.hashrate1d for w in unique],
                    [w.hashrate7d for w in unique], [w.lastshare for w in unique],
                    [w.shares for w in unique], [w.bestshare for w in unique],
                )
