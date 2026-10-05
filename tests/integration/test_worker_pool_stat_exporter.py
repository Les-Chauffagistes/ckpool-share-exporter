"""Export de pool.status de bout en bout (workers.pool_stat_exporter.export_pool_stat)."""

import pytest

from ckpool_share_exporter.workers.pool_stat_exporter import export_pool_stat
from tests.unit.test_pool_stat import LINES


@pytest.fixture
def status_file(log_dir):
    directory = log_dir / "pool"
    directory.mkdir()
    return directory / "pool.status"


async def rows_of(pg, pool_instance):
    return await pg.fetch(
        "SELECT * FROM pool_stat WHERE pool_instance = $1", pool_instance)


async def test_a_snapshot_is_stored_under_its_pool_instance(pg, status_file, pool_instance):
    status_file.write_text("\n".join(LINES) + "\n", encoding = "utf-8")

    await export_pool_stat(pg)

    (row,) = await rows_of(pg, pool_instance)
    assert row["updated_at"].timestamp() == 1791189501
    assert (row["users"], row["workers"], row["accepted"]) == (49, 885, 5784732259)
    assert row["hashrate_1m"] == pytest.approx(1.48e12)


async def test_a_new_snapshot_replaces_the_previous_one(pg, status_file, pool_instance):
    status_file.write_text("\n".join(LINES) + "\n", encoding = "utf-8")
    await export_pool_stat(pg)

    newer = [LINES[0].replace("1791189501", "1791189511").replace('"Workers": 885', '"Workers": 900'),
             *LINES[1:]]
    status_file.write_text("\n".join(newer) + "\n", encoding = "utf-8")
    await export_pool_stat(pg)

    (row,) = await rows_of(pg, pool_instance)
    assert row["updated_at"].timestamp() == 1791189511
    assert row["workers"] == 900


async def test_a_half_written_file_inserts_nothing_and_recovers(pg, status_file, pool_instance):
    status_file.write_text("\n".join(LINES[:1]) + "\n", encoding = "utf-8")
    await export_pool_stat(pg)
    assert await rows_of(pg, pool_instance) == []

    status_file.write_text("\n".join(LINES) + "\n", encoding = "utf-8")
    await export_pool_stat(pg)

    assert len(await rows_of(pg, pool_instance)) == 1


async def test_a_missing_file_is_not_an_error(pg, log_dir, pool_instance):
    await export_pool_stat(pg)

    assert await rows_of(pg, pool_instance) == []
