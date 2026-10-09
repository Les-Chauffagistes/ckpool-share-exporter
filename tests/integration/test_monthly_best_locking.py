"""Les replicas ecrivent les memes lignes de monthly_bests (dao.MonthlyBest)."""

import asyncio
from datetime import UTC, date, datetime

import asyncpg

from ckpool_share_exporter.dao import FileDAO, ShareWeightDAO
from ckpool_share_exporter.models import MonthlyBest

USERS = [f"addr{i}" for i in range(8)]
MONTH = date(2026, 10, 1)


async def commit_best_diffs(weights, path, order, pool_instance):
    files = FileDAO(weights.pg)
    await files.register([path], pool_instance)
    await files.claim(path, pool_instance)
    diffs = {(user, MONTH): MonthlyBest(workername = f"{user}.rig", best_diff = 1.0) for user in order}
    await weights.commit_sharelog(
        {}, path, pool_instance, datetime(2026, 1, 1, tzinfo = UTC), 1, diffs)


async def test_replicas_writing_the_same_users_in_opposite_orders_never_deadlock(pg):
    weights = ShareWeightDAO(pg)
    for i in range(30):
        results = await asyncio.gather(
            commit_best_diffs(weights, f"/logs/a{i}", USERS, "ckpool01"),
            commit_best_diffs(weights, f"/logs/b{i}", USERS[::-1], "debian"),
            return_exceptions = True)
        assert not [r for r in results if isinstance(r, asyncpg.DeadlockDetectedError)]
