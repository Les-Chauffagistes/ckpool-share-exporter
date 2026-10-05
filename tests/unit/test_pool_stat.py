"""Lecture de pool.status (workers.pool_stat_exporter.read_pool_stat) et models.PoolStat."""

import pytest

from ckpool_share_exporter.workers.pool_stat_exporter import read_pool_stat

LINES = [
    '{"runtime": 1078028, "lastupdate": 1791189501, "Users": 49, "Workers": 885, "Idle": 884, "Disconnected": 0}',
    '{"hashrate1m": "1.48T", "hashrate5m": "1.56T", "hashrate15m": "1.47T", "hashrate1hr": "1.41T", '
    '"hashrate6hr": "1.37T", "hashrate1d": "1.37T", "hashrate7d": "9.98T"}',
    '{"diff": 0.0, "accepted": 5784732259, "rejected": 345210059, "bestshare": 15211409842, '
    '"SPS1m": 0.236, "SPS5m": 0.248, "SPS15m": 0.234, "SPS1h": 0.225}',
]


def write(tmp_path, lines, *, final_newline = True):
    path = tmp_path / "pool.status"
    path.write_text("\n".join(lines) + ("\n" if final_newline else ""), encoding = "utf-8")
    return path


async def test_the_three_lines_are_merged_into_one_snapshot(tmp_path):
    stat = await read_pool_stat(write(tmp_path, LINES))

    assert stat is not None
    assert (stat.lastupdate, stat.users, stat.workers, stat.idle) == (1791189501, 49, 885, 884)
    assert stat.accepted == 5784732259
    assert stat.SPS1m == 0.236


async def test_hashrates_are_converted_to_hashes_per_second(tmp_path):
    stat = await read_pool_stat(write(tmp_path, LINES))

    assert stat.hashrate1m == pytest.approx(1.48e12)
    assert stat.hashrate7d == pytest.approx(9.98e12)


@pytest.mark.parametrize("lines, final_newline", [
    (LINES[:1], True),               # reecriture au debut: une seule ligne
    (LINES[:2], True),
    (LINES, False),                  # derniere ligne encore en cours d'ecriture
    ([LINES[0], LINES[1], LINES[2][:40] + "\n"], True),   # ligne coupee puis saut de ligne
    ([], True),                      # fichier tout juste tronque
])
async def test_a_partial_rewrite_yields_no_snapshot(tmp_path, lines, final_newline):
    assert await read_pool_stat(write(tmp_path, lines, final_newline = final_newline)) is None


async def test_an_unparsable_hashrate_discards_the_snapshot(tmp_path):
    broken = LINES[1].replace("1.48T", "1.48Q")

    assert await read_pool_stat(write(tmp_path, [LINES[0], broken, LINES[2]])) is None


async def test_a_missing_file_raises_so_the_worker_can_report_it(tmp_path):
    with pytest.raises(FileNotFoundError):
        await read_pool_stat(tmp_path / "absent")
