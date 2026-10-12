from datetime import datetime
from typing import NamedTuple, Sequence

import asyncpg

from ckpool_share_exporter.models import ShareRow


class CursorConflict(Exception):
    """shipped_offset a change en base depuis la lecture: un autre shipper a livre ces lignes.

    Levee pour annuler toute la transaction. Le perdant recharge l'offset et continue.
    """


class ShareCursor(NamedTuple):
    path: str
    block: str
    shipped_offset: int
    closed: bool
    blocked: bool


_INSERT_SHARE_LOG = """
                    INSERT INTO share_log
                    (epoch, seq, created_at, pool_instance, path, byte_offset, workinfoid, username,
                     workername, diff, sdiff, result, parse_error, raw)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) \
                    """

_UPSERT_WATERMARK = """
                    INSERT INTO instance_watermark (pool_instance, shipped_until, heartbeat_at)
                    VALUES ($1, $2, now())
                    ON CONFLICT (pool_instance) DO UPDATE
                        SET shipped_until = greatest(instance_watermark.shipped_until, excluded.shipped_until),
                            heartbeat_at  = excluded.heartbeat_at \
                    """

_COLUMNS = "path, block, shipped_offset, closed, blocked"


class ShareCursorDAO:
    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def has_any(self, pool_instance: str) -> bool:
        return bool(await self.pg.fetchval(
            "SELECT EXISTS (SELECT 1 FROM share_cursor WHERE pool_instance = $1)", pool_instance))

    async def get_open(self, pool_instance: str) -> list[ShareCursor]:
        rows = await self.pg.fetch(
            f"SELECT {_COLUMNS} FROM share_cursor WHERE pool_instance = $1 AND NOT closed ORDER BY path",
            pool_instance)
        return [ShareCursor(*row.values()) for row in rows]

    async def get(self, pool_instance: str, path: str) -> ShareCursor | None:
        row = await self.pg.fetchrow(
            f"SELECT {_COLUMNS} FROM share_cursor WHERE pool_instance = $1 AND path = $2",
            pool_instance, path)
        return ShareCursor(*row.values()) if row else None

    async def ensure(self, pool_instance: str, path: str, block: str, offset: int) -> ShareCursor:
        """Cree le curseur a `offset` s'il n'existe pas; sinon retourne l'existant, inchange."""
        await self.pg.execute(
            """INSERT INTO share_cursor (pool_instance, path, block, shipped_offset)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (pool_instance, path) DO NOTHING""",
            pool_instance, path, block, offset)
        cursor = await self.get(pool_instance, path)
        assert cursor is not None
        return cursor

    async def close(self, pool_instance: str, path: str) -> None:
        await self.pg.execute(
            """UPDATE share_cursor SET closed = true, updated_at = now()
               WHERE pool_instance = $1 AND path = $2""",
            pool_instance, path)

    async def block(self, pool_instance: str, path: str) -> None:
        await self.pg.execute(
            """UPDATE share_cursor SET blocked = true, updated_at = now()
               WHERE pool_instance = $1 AND path = $2""",
            pool_instance, path)


class ShareLogDAO:
    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def ship(
            self, pool_instance: str, path: str, old_offset: int, new_offset: int,
            rows: Sequence[ShareRow], watermark: datetime | None,
    ) -> tuple[int, int]:
        """Livre un batch de shares et avance le curseur, atomiquement.

        1. UPDATE optimiste du curseur (WHERE shipped_offset = old): 0 ligne =>
           CursorConflict et rollback. Fait AVANT de prendre le verrou global, pour
           qu'un perdant ne le tienne pas inutilement; le perdant attend sur le
           verrou du curseur, puis revoit l'offset du gagnant.
        2. UPDATE share_seq ... RETURNING: le verrou serialise les shippers, donc
           l'ordre de seq est l'ordre de commit. Annule avec la transaction, donc gapless.
        3. INSERT simple, sans ON CONFLICT: un conflit serait un bug et laisserait
           un trou dans le seq, la transaction doit echouer.
        4. watermark, jamais en recul; 5. NOTIFY, delivre au commit.

        Retourne (epoch, dernier seq), ou (epoch, seq courant) si le batch est vide.
        """
        async with self.pg.acquire() as connection, connection.transaction():
            updated = await connection.execute(
                """UPDATE share_cursor
                   SET shipped_offset = $4, updated_at = now()
                   WHERE pool_instance = $1 AND path = $2 AND shipped_offset = $3
                     AND NOT closed AND NOT blocked""",
                pool_instance, path, old_offset, new_offset)
            if updated == "UPDATE 0":
                raise CursorConflict(path)

            count = len(rows)
            if count:
                epoch, last = await connection.fetchrow(
                    "UPDATE share_seq SET v = v + $1 WHERE id = 1 RETURNING epoch, v", count)
                first = last - count + 1
                await connection.executemany(_INSERT_SHARE_LOG, [
                    (epoch, first + index, row.created_at, pool_instance, path, row.byte_offset,
                     row.workinfoid, row.username, row.workername, row.diff, row.sdiff, row.result,
                     row.parse_error, row.raw)
                    for index, row in enumerate(rows)
                ])
                await connection.execute("SELECT pg_notify('shares', $1)", f"{epoch}:{last}")
            else:
                epoch, last = await connection.fetchrow("SELECT epoch, v FROM share_seq WHERE id = 1")

            if watermark is not None:
                await connection.execute(_UPSERT_WATERMARK, pool_instance, watermark)
            return epoch, last

    async def heartbeat(self, pool_instance: str, watermark: datetime) -> None:
        await self.pg.execute(_UPSERT_WATERMARK, pool_instance, watermark)
