"""UPSERT_MONTHLY_BESTS: seul un record strictement battu reecrit la ligne."""

from datetime import date

from ckpool_share_exporter.dao.MonthlyBest import UPSERT_MONTHLY_BESTS

MONTH = date(2026, 10, 1)
USER = "addr0"


async def upsert(pg, best_diff, workername):
    await pg.execute(UPSERT_MONTHLY_BESTS, MONTH, USER, best_diff, workername)


async def current(pg):
    return await pg.fetchrow(
        'SELECT best_diff, workername, updated_at FROM monthly_bests WHERE month = $1 AND "user" = $2',
        MONTH, USER)


async def test_first_insert_stores_diff_and_workername(pg):
    await upsert(pg, 10.0, "addr0.first")

    row = await current(pg)
    assert (row["best_diff"], row["workername"]) == (10.0, "addr0.first")


async def test_higher_diff_replaces_diff_and_workername_together(pg):
    await upsert(pg, 10.0, "addr0.first")
    before = await current(pg)

    await upsert(pg, 20.0, "addr0.winner")

    row = await current(pg)
    assert (row["best_diff"], row["workername"]) == (20.0, "addr0.winner")
    assert row["updated_at"] > before["updated_at"]


async def test_lower_diff_changes_nothing_including_updated_at(pg):
    await upsert(pg, 20.0, "addr0.winner")
    before = await current(pg)

    await upsert(pg, 5.0, "addr0.later")

    assert await current(pg) == before


async def test_equal_diff_keeps_the_first_workername_and_updated_at(pg):
    await upsert(pg, 20.0, "addr0.winner")
    before = await current(pg)

    await upsert(pg, 20.0, "addr0.tie")

    assert await current(pg) == before
