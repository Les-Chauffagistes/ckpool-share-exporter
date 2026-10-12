"""Shipping des sharelogs vers share_log de bout en bout (workers.share_shipper).

Un ShareShipper est construit par test et on appelle tick() a la main, comme le
fait run_forever en production.
"""

import asyncio
import os
import time
from datetime import UTC, datetime

import pytest

import ckpool_share_exporter.workers.share_shipper as shipper_module
from ckpool_share_exporter import settings as settings_module
from ckpool_share_exporter.dao import ShareLogDAO
from ckpool_share_exporter.workers.file_explorer import register_new_sharelogs
from ckpool_share_exporter.workers.share_ingestor import ingest_sharelogs
from ckpool_share_exporter.workers.share_shipper import ShareShipper, ship_shares
from tests.helpers import ADDRESS_A, ADDRESS_B, append_sharelog, line, set_mtime, write_sharelog

_MTIME = float(int(time.time()))


@pytest.fixture(autouse = True)
def backfill(monkeypatch):
    """Les tests partent d'un curseur a 0, sauf ceux du premier deploiement."""
    monkeypatch.setattr(settings_module.settings, "share_shipper_backfill", True)
    monkeypatch.setattr(shipper_module, "_shipper", None)


def make_block(log_dir, name, mtime = None):
    directory = log_dir / name
    directory.mkdir(exist_ok = True)
    if mtime is not None:
        os.utime(directory, (mtime, mtime))
    return directory


async def shipped(pg, pool_instance):
    return await pg.fetch(
        "SELECT * FROM share_log WHERE pool_instance = $1 ORDER BY epoch, seq", pool_instance)


async def cursor_of(pg, pool_instance, path):
    return await pg.fetchrow(
        "SELECT * FROM share_cursor WHERE pool_instance = $1 AND path = $2", pool_instance, path)


async def watermark_of(pg, pool_instance):
    return await pg.fetchval(
        "SELECT shipped_until FROM instance_watermark WHERE pool_instance = $1", pool_instance)


# --- nominal -----------------------------------------------------------------

async def test_shares_are_shipped_with_their_fields_and_the_cursor_advances(pg, round_dir, pool_instance):
    path = write_sharelog(round_dir / "10.sharelog", [
        line(workinfoid = 10, rig = "rig1", diff = 1000.0, sdiff = 5000.0, createdate = "1700000000,123456789"),
        line(workinfoid = 10, rig = "rig2", diff = 500.0, result = False, createdate = "1700000001,5000"),
    ])
    shipper = ShareShipper(pg)

    await shipper.tick()

    rows = await shipped(pg, pool_instance)
    assert [(r["seq"], r["epoch"], r["workinfoid"], r["diff"], r["sdiff"], r["result"]) for r in rows] == [
        (1, 1, 10, 1000.0, 5000.0, True),
        (2, 1, 10, 500.0, 8750.0, False),
    ]
    assert rows[0]["created_at"] == datetime(2023, 11, 14, 22, 13, 20, 123456, tzinfo = UTC)
    assert rows[1]["created_at"] == datetime(2023, 11, 14, 22, 13, 21, 5, tzinfo = UTC)
    assert rows[0]["username"] == ADDRESS_A and rows[0]["workername"].endswith(".rig1")
    assert (rows[0]["path"], rows[0]["byte_offset"]) == ("0000000f/10.sharelog", 0)
    assert rows[1]["byte_offset"] == len(line(workinfoid = 10, rig = "rig1", diff = 1000.0, sdiff = 5000.0,
                                              createdate = "1700000000,123456789")) + 1
    assert not any(r["parse_error"] for r in rows)

    cursor = await cursor_of(pg, pool_instance, "0000000f/10.sharelog")
    assert cursor["shipped_offset"] == path.stat().st_size
    assert not cursor["closed"] and not cursor["blocked"]


async def test_a_second_tick_without_new_lines_ships_nothing(pg, round_dir, pool_instance):
    write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10)])
    shipper = ShareShipper(pg)

    await shipper.tick()
    await shipper.tick()

    assert len(await shipped(pg, pool_instance)) == 1


async def test_appended_lines_are_shipped_on_the_next_tick(pg, round_dir, pool_instance):
    path = write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10)])
    shipper = ShareShipper(pg)
    await shipper.tick()

    append_sharelog(path, [line(workinfoid = 10), line(workinfoid = 10)])
    await shipper.tick()

    assert [r["seq"] for r in await shipped(pg, pool_instance)] == [1, 2, 3]


async def test_a_file_created_after_startup_is_picked_up_from_zero(pg, round_dir, pool_instance):
    shipper = ShareShipper(pg)
    await shipper.tick()

    write_sharelog(round_dir / "11.sharelog", [line(workinfoid = 11)])
    await shipper.tick()

    assert [r["workinfoid"] for r in await shipped(pg, pool_instance)] == [11]


async def test_more_than_one_batch_is_shipped_in_a_single_tick(pg, round_dir, pool_instance, monkeypatch):
    monkeypatch.setattr(shipper_module, "BATCH_SIZE", 3)
    path = write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10) for _ in range(8)])

    await ShareShipper(pg).tick()

    assert [r["seq"] for r in await shipped(pg, pool_instance)] == list(range(1, 9))
    assert (await cursor_of(pg, pool_instance, "0000000f/10.sharelog"))["shipped_offset"] == path.stat().st_size


# --- ligne partielle, reprise ------------------------------------------------

async def test_a_partial_line_is_re_read_once_complete(pg, round_dir, pool_instance):
    complete = line(workinfoid = 10, createdate = 1_700_000_000)
    last = line(workinfoid = 11, createdate = 1_700_000_001)
    path = write_sharelog(round_dir / "10.sharelog", [complete, last[:40]], final_newline = False)
    shipper = ShareShipper(pg)

    await shipper.tick()
    assert [r["workinfoid"] for r in await shipped(pg, pool_instance)] == [10]
    assert (await cursor_of(pg, pool_instance, "0000000f/10.sharelog"))["shipped_offset"] == len(complete) + 1
    await shipper.tick()
    assert [r["workinfoid"] for r in await shipped(pg, pool_instance)] == [10]

    with path.open("a") as handle:
        handle.write(last[40:] + "\n")
    await shipper.tick()

    rows = await shipped(pg, pool_instance)
    assert [(r["workinfoid"], r["parse_error"]) for r in rows] == [(10, False), (11, False)]
    assert rows[1]["byte_offset"] == len(complete) + 1


async def test_a_restart_mid_file_resumes_at_the_stored_offset(pg, round_dir, pool_instance):
    path = write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10) for _ in range(3)])
    await ShareShipper(pg).tick()

    append_sharelog(path, [line(workinfoid = 10) for _ in range(2)])
    await ShareShipper(pg).tick()

    rows = await shipped(pg, pool_instance)
    assert [r["seq"] for r in rows] == [1, 2, 3, 4, 5]
    assert len({r["byte_offset"] for r in rows}) == 5


async def test_a_file_created_while_the_exporter_was_down_is_shipped_after_restart(pg, round_dir, pool_instance):
    write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10)])
    await ShareShipper(pg).tick()

    write_sharelog(round_dir / "11.sharelog", [line(workinfoid = 11)])
    await ShareShipper(pg).tick()

    assert [r["workinfoid"] for r in await shipped(pg, pool_instance)] == [10, 11]


# --- premier deploiement -----------------------------------------------------

async def test_first_deployment_starts_at_the_current_end_of_files(pg, round_dir, pool_instance, monkeypatch):
    monkeypatch.setattr(settings_module.settings, "share_shipper_backfill", False)
    old = line(workinfoid = 10)
    # La derniere ligne est en cours d'ecriture: le curseur doit s'arreter avant elle.
    path = round_dir / "10.sharelog"
    path.write_text(old + "\n" + old[:30])
    shipper = ShareShipper(pg)

    await shipper.tick()
    assert await shipped(pg, pool_instance) == []
    assert (await cursor_of(pg, pool_instance, "0000000f/10.sharelog"))["shipped_offset"] == len(old) + 1

    with path.open("a") as handle:
        handle.write(old[30:] + "\n")
    write_sharelog(round_dir / "11.sharelog", [line(workinfoid = 11)])
    await shipper.tick()

    rows = await shipped(pg, pool_instance)
    assert [(r["workinfoid"], r["parse_error"]) for r in rows] == [(10, False), (11, False)]


async def test_a_second_start_does_not_re_initialise_existing_cursors(pg, round_dir, pool_instance, monkeypatch):
    monkeypatch.setattr(settings_module.settings, "share_shipper_backfill", False)
    path = write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10)])
    await ShareShipper(pg).tick()

    append_sharelog(path, [line(workinfoid = 10)])
    await ShareShipper(pg).tick()

    assert len(await shipped(pg, pool_instance)) == 1


# --- changement de bloc ------------------------------------------------------

async def test_a_finished_block_is_read_to_the_end_then_closed(pg, log_dir, pool_instance):
    now = time.time()
    old = make_block(log_dir, "00000001", now - 300)
    mid = make_block(log_dir, "00000002", now - 200)
    write_sharelog(old / "1.sharelog", [line(workinfoid = 1)])
    write_sharelog(mid / "2.sharelog", [line(workinfoid = 2)])
    make_block(log_dir, "00000001", now - 300)
    make_block(log_dir, "00000002", now - 200)
    shipper = ShareShipper(pg)
    await shipper.tick()
    assert shipper.active == ["00000002", "00000001"]

    append_sharelog(old / "1.sharelog", [line(workinfoid = 1)])
    new = make_block(log_dir, "00000003", now)
    write_sharelog(new / "3.sharelog", [line(workinfoid = 3)])
    await shipper.tick()

    assert shipper.active == ["00000003", "00000002"]
    assert sorted(r["workinfoid"] for r in await shipped(pg, pool_instance)) == [1, 1, 2, 3]
    assert (await cursor_of(pg, pool_instance, "00000001/1.sharelog"))["closed"]
    assert not (await cursor_of(pg, pool_instance, "00000002/2.sharelog"))["closed"]
    assert "00000001/1.sharelog" not in shipper.files


async def test_active_blocks_are_ranked_by_mtime_and_several_can_appear_in_one_tick(pg, log_dir, pool_instance):
    now = time.time()
    # Ordre lexicographique contraire a l'ordre des mtime.
    for name, age in (("00000020", 500), ("0000000a", 100), ("00000005", 300), ("0000000f", 200)):
        directory = make_block(log_dir, name)
        write_sharelog(directory / "1.sharelog", [line(workinfoid = int(name, 16))])
        os.utime(directory, (now - age, now - age))
    shipper = ShareShipper(pg)

    await shipper.tick()

    assert shipper.active == ["0000000a", "0000000f"]
    assert [r["workinfoid"] for r in await shipped(pg, pool_instance)] == [10, 15]


async def test_a_block_finished_during_downtime_is_caught_up_on_restart(pg, log_dir, pool_instance):
    now = time.time()
    old = make_block(log_dir, "00000001", now - 300)
    path = write_sharelog(old / "1.sharelog", [line(workinfoid = 1)])
    os.utime(old, (now - 300, now - 300))
    await ShareShipper(pg).tick()

    append_sharelog(path, [line(workinfoid = 1)])
    for name, age in (("00000002", 200), ("00000003", 100)):
        directory = make_block(log_dir, name)
        write_sharelog(directory / "9.sharelog", [line(workinfoid = int(name, 16))])
        os.utime(directory, (now - age, now - age))
    os.utime(old, (now - 300, now - 300))
    await ShareShipper(pg).tick()

    assert sorted(r["workinfoid"] for r in await shipped(pg, pool_instance)) == [1, 1, 2, 3]
    assert (await cursor_of(pg, pool_instance, "00000001/1.sharelog"))["closed"]


async def test_the_unterminated_last_line_of_a_finished_block_is_shipped(pg, log_dir, pool_instance):
    now = time.time()
    old = make_block(log_dir, "00000001", now - 300)
    mid = make_block(log_dir, "00000002", now - 200)
    write_sharelog(old / "1.sharelog", [line(workinfoid = 1)], final_newline = False)
    write_sharelog(mid / "2.sharelog", [line(workinfoid = 2)])
    os.utime(old, (now - 300, now - 300))
    os.utime(mid, (now - 200, now - 200))
    shipper = ShareShipper(pg)
    await shipper.tick()
    assert [r["workinfoid"] for r in await shipped(pg, pool_instance)] == [2]

    new = make_block(log_dir, "00000003", now)
    write_sharelog(new / "3.sharelog", [line(workinfoid = 3)])
    await shipper.tick()

    assert sorted(r["workinfoid"] for r in await shipped(pg, pool_instance)) == [1, 2, 3]
    assert (await cursor_of(pg, pool_instance, "00000001/1.sharelog"))["closed"]


# --- double shipper ----------------------------------------------------------

async def test_a_stale_second_shipper_loses_the_optimistic_guard_and_duplicates_nothing(pg, round_dir, pool_instance):
    first, second = ShareShipper(pg), ShareShipper(pg)
    await first.tick()
    await second.tick()
    path = write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10) for _ in range(4)])

    await first.tick()
    await second.tick()

    assert [r["seq"] for r in await shipped(pg, pool_instance)] == [1, 2, 3, 4]
    assert second.files["0000000f/10.sharelog"].offset == path.stat().st_size

    append_sharelog(path, [line(workinfoid = 10)])
    await second.tick()
    assert [r["seq"] for r in await shipped(pg, pool_instance)] == [1, 2, 3, 4, 5]


async def test_two_shippers_ticking_concurrently_ship_each_share_once(pg, round_dir, pool_instance):
    first, second = ShareShipper(pg), ShareShipper(pg)
    path = write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10) for _ in range(50)])

    await asyncio.gather(first.tick(), second.tick())
    append_sharelog(path, [line(workinfoid = 10) for _ in range(50)])
    await asyncio.gather(first.tick(), second.tick())

    rows = await shipped(pg, pool_instance)
    assert [r["seq"] for r in rows] == list(range(1, 101))
    assert len({r["byte_offset"] for r in rows}) == 100
    assert (await cursor_of(pg, pool_instance, "0000000f/10.sharelog"))["shipped_offset"] == path.stat().st_size


async def test_a_conflict_rolls_back_the_whole_batch(pg, pool_instance):
    from ckpool_share_exporter.dao import CursorConflict, ShareCursorDAO
    from ckpool_share_exporter.models import ShareRow

    await ShareCursorDAO(pg).ensure(pool_instance, "0000000f/1.sharelog", "0000000f", 100)
    row = ShareRow(datetime.now(UTC), 0, workinfoid = 1)

    with pytest.raises(CursorConflict):
        await ShareLogDAO(pg).ship(pool_instance, "0000000f/1.sharelog", 0, 50, [row], datetime.now(UTC))

    assert await shipped(pg, pool_instance) == []
    assert await pg.fetchval("SELECT v FROM share_seq") == 0
    assert await watermark_of(pg, pool_instance) is None


# --- fichier retreci, ligne illisible ----------------------------------------

async def test_a_shrunk_file_is_blocked_and_never_shipped_again(pg, round_dir, pool_instance):
    path = write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10) for _ in range(3)])
    shipper = ShareShipper(pg)
    await shipper.tick()

    write_sharelog(path, [line(workinfoid = 99)])
    await shipper.tick()
    append_sharelog(path, [line(workinfoid = 99) for _ in range(5)])
    await shipper.tick()
    await ShareShipper(pg).tick()

    assert [r["workinfoid"] for r in await shipped(pg, pool_instance)] == [10, 10, 10]
    assert (await cursor_of(pg, pool_instance, "0000000f/10.sharelog"))["blocked"]


async def test_a_blocked_file_does_not_stop_the_other_files_nor_the_watermark(pg, round_dir, pool_instance):
    bad = write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10) for _ in range(3)])
    shipper = ShareShipper(pg)
    await shipper.tick()
    write_sharelog(bad, [line(workinfoid = 99)])
    write_sharelog(round_dir / "11.sharelog", [line(workinfoid = 11)])

    await shipper.tick()

    assert [r["workinfoid"] for r in await shipped(pg, pool_instance)] == [10, 10, 10, 11]
    assert (time.time() - (await watermark_of(pg, pool_instance)).timestamp()) < 10


async def test_an_unreadable_line_is_shipped_raw_and_the_seq_stays_gapless(pg, round_dir, pool_instance):
    garbage = '{"workinfoid": 1, "truncated'
    write_sharelog(round_dir / "10.sharelog", [
        line(workinfoid = 10, createdate = "1700000000,1000"), garbage, "not json at all",
        line(workinfoid = 10, createdate = "1700000002,0"),
    ])

    await ShareShipper(pg).tick()

    rows = await shipped(pg, pool_instance)
    assert [r["seq"] for r in rows] == [1, 2, 3, 4]
    assert [r["parse_error"] for r in rows] == [False, True, True, False]
    assert [r["raw"] for r in rows] == [None, garbage, "not json at all", None]
    assert rows[1]["workinfoid"] is None and rows[1]["diff"] is None
    assert rows[1]["created_at"] == rows[0]["created_at"]
    assert rows[1]["byte_offset"] > rows[0]["byte_offset"]


# --- seq, epoch, notify ------------------------------------------------------

async def test_seq_is_gapless_across_files_ticks_and_instances(pg, log_dir, round_dir, pool_instance, monkeypatch):
    first_path = write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10) for _ in range(7)])
    second_path = write_sharelog(round_dir / "11.sharelog", [line(workinfoid = 11) for _ in range(5)])
    await ShareShipper(pg).tick()

    other = pool_instance + "-b"
    monkeypatch.setattr(settings_module.settings, "pool_instance_name", other)
    other_shipper = ShareShipper(pg)
    await other_shipper.tick()

    append_sharelog(first_path, [line(workinfoid = 10) for _ in range(3)])
    await other_shipper.tick()
    monkeypatch.setattr(settings_module.settings, "pool_instance_name", pool_instance)
    append_sharelog(second_path, [line(workinfoid = 11)])
    await ShareShipper(pg).tick()

    seqs = [r["seq"] for r in await pg.fetch("SELECT seq FROM share_log ORDER BY seq")]
    assert seqs == list(range(1, 12 + 12 + 3 + 3 + 1 + 1))
    assert await pg.fetchval("SELECT v FROM share_seq") == len(seqs)


async def test_a_bumped_epoch_is_stamped_on_the_following_batches(pg, round_dir, pool_instance):
    path = write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10)])
    shipper = ShareShipper(pg)
    await shipper.tick()

    await pg.execute("UPDATE share_seq SET epoch = epoch + 1")
    append_sharelog(path, [line(workinfoid = 10)])
    await shipper.tick()

    assert [(r["epoch"], r["seq"]) for r in await shipped(pg, pool_instance)] == [(1, 1), (2, 2)]


async def test_a_batch_notifies_listeners_after_commit(pg, round_dir):
    received: list[str] = []
    async with pg.acquire() as listener:
        def on_notify(*args):
            received.append(args[3])

        await listener.add_listener("shares", on_notify)
        write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10) for _ in range(3)])

        await ShareShipper(pg).tick()
        await asyncio.sleep(0.2)
        await listener.remove_listener("shares", on_notify)

    assert received == ["1:3"]


# --- watermark ---------------------------------------------------------------

async def test_the_watermark_at_rest_is_now_minus_the_margin(pg, round_dir, pool_instance, monkeypatch):
    monkeypatch.setattr(settings_module.settings, "share_shipper_margin_seconds", 2.0)
    write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10, createdate = 1_700_000_000)])
    before = time.time()

    await ShareShipper(pg).tick()

    watermark = (await watermark_of(pg, pool_instance)).timestamp()
    assert before - 2.0 - 0.5 <= watermark <= time.time() - 2.0 + 0.5


async def test_an_incomplete_file_holds_the_watermark_at_its_last_shipped_share(pg, round_dir, pool_instance):
    complete = line(workinfoid = 10, createdate = "1700000000,250000000")
    last = line(workinfoid = 10, createdate = "1700000001,0")
    path = write_sharelog(round_dir / "10.sharelog", [complete, last[:30]], final_newline = False)
    shipper = ShareShipper(pg)

    await shipper.tick()
    assert (await watermark_of(pg, pool_instance)) == datetime(2023, 11, 14, 22, 13, 20, 250000, tzinfo = UTC)

    with path.open("a") as handle:
        handle.write(last[30:] + "\n")
    await shipper.tick()
    assert (await watermark_of(pg, pool_instance)).timestamp() > time.time() - 10


async def test_the_watermark_never_goes_backwards(pg, pool_instance):
    dao = ShareLogDAO(pg)
    later, earlier = datetime(2030, 1, 1, tzinfo = UTC), datetime(2029, 1, 1, tzinfo = UTC)

    await dao.heartbeat(pool_instance, later)
    await dao.heartbeat(pool_instance, earlier)

    assert await watermark_of(pg, pool_instance) == later


async def test_an_idle_tick_refreshes_the_heartbeat(pg, round_dir, pool_instance):
    shipper = ShareShipper(pg)
    await shipper.tick()
    first = await pg.fetchval("SELECT heartbeat_at FROM instance_watermark WHERE pool_instance = $1", pool_instance)

    await shipper.tick()

    second = await pg.fetchval("SELECT heartbeat_at FROM instance_watermark WHERE pool_instance = $1", pool_instance)
    assert second > first


# --- worker ------------------------------------------------------------------

async def test_the_in_memory_state_is_rebuilt_after_an_exception(pg, round_dir, pool_instance, monkeypatch):
    write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10)])

    async def boom(*args, **kwargs):
        raise ConnectionError("db down")

    with monkeypatch.context() as patched:
        patched.setattr(ShareLogDAO, "ship", boom)
        with pytest.raises(ConnectionError):
            await ship_shares(pg)
    assert shipper_module._shipper is None

    await ship_shares(pg)

    assert [r["workinfoid"] for r in await shipped(pg, pool_instance)] == [10]
    assert shipper_module._shipper is not None


async def test_a_failed_commit_does_not_advance_the_in_memory_offset(pg, round_dir, pool_instance, monkeypatch):
    path = write_sharelog(round_dir / "10.sharelog", [line(workinfoid = 10)])
    shipper = ShareShipper(pg)
    real = ShareLogDAO.ship
    calls = {"n": 0}

    async def flaky(self, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("db down")
        return await real(self, *args, **kwargs)

    monkeypatch.setattr(ShareLogDAO, "ship", flaky)
    with pytest.raises(ConnectionError):
        await shipper.tick()
    await shipper.tick()

    assert [r["seq"] for r in await shipped(pg, pool_instance)] == [1]
    assert (await cursor_of(pg, pool_instance, "0000000f/10.sharelog"))["shipped_offset"] == path.stat().st_size


# --- controle de coherence avec l'ingestor -----------------------------------

async def test_share_log_sums_match_share_weights_fed_by_the_ingestor(pg, log_dir, pool_instance):
    now = time.time()
    block_a = make_block(log_dir, "00000001", now - 100)
    block_b = make_block(log_dir, "00000002", now - 50)
    for directory, base in ((block_a, 100), (block_b, 200)):
        for job in range(base, base + 3):
            write_sharelog(directory / f"{job}.sharelog", [
                line(workinfoid = job, address = address, rig = rig, diff = diff, result = ok,
                     createdate = f"{1_700_000_000 + job},{job * 7}")
                for address, rig, diff, ok in (
                    (ADDRESS_A, "rig1", 1000.0, True), (ADDRESS_A, "rig1", 1000.0, True),
                    (ADDRESS_A, "rig2", 250.5, True), (ADDRESS_A, "rig2", 999.0, False),
                    (ADDRESS_B, "rig1", 64.0, True), (ADDRESS_B, "rig3", 128.0, False),
                )
            ])
            set_mtime(directory / f"{job}.sharelog", float(int(now)))
    os.utime(block_a, (now - 100, now - 100))
    os.utime(block_b, (now - 50, now - 50))

    await register_new_sharelogs(pg)
    await ingest_sharelogs(pg)
    await ShareShipper(pg).tick()
    await ShareShipper(pg).tick()

    weights = await pg.fetch(
        """SELECT workinfoid, workername, diff_sum, shares_ok, shares_ko
           FROM share_weights WHERE pool_instance = $1 ORDER BY workinfoid, workername""", pool_instance)
    logged = await pg.fetch(
        """SELECT workinfoid, workername,
                  coalesce(sum(diff) FILTER (WHERE result), 0) AS diff_sum,
                  count(*) FILTER (WHERE result)     AS shares_ok,
                  count(*) FILTER (WHERE NOT result) AS shares_ko
           FROM share_log WHERE pool_instance = $1 GROUP BY workinfoid, workername
           ORDER BY workinfoid, workername""", pool_instance)

    assert len(weights) == 6 * 4
    assert [tuple(r) for r in logged] == [tuple(r) for r in weights]
