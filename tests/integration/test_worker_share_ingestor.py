"""Ingestion des sharelogs de bout en bout (workers.share_ingestor.ingest_sharelogs).

Chaque test passe par register_new_sharelogs pour enregistrer les fichiers, comme en
production.
"""

import time
from datetime import UTC, date, datetime

import pytest

import ckpool_share_exporter.workers.share_ingestor as ingestor
from ckpool_share_exporter.dao import ShareWeightDAO
from ckpool_share_exporter.workers.file_explorer import register_new_sharelogs
from ckpool_share_exporter.workers.share_ingestor import ingest_sharelogs
from tests.helpers import (
    ADDRESS_A, ADDRESS_B, append_sharelog, line, set_mtime, worker_name, write_sharelog,
)

# mtime entier: la comparaison (mtime, size) de ingest_sharelogs passe par
# datetime.fromtimestamp, et un mtime entier ecarte toute question d'arrondi.
_MTIME = float(int(time.time()))


def sharelog(directory, lines, *, name = "0.sharelog", final_newline = True, mtime = _MTIME):
    path = write_sharelog(directory / name, lines, final_newline = final_newline)
    set_mtime(path, mtime)
    return path


async def ingest(pg):
    await register_new_sharelogs(pg)
    await ingest_sharelogs(pg)


async def weights_of(pg, pool_instance):
    return await pg.fetch(
        """SELECT bucket_at, workinfoid, username, workername, diff_sum, shares_ok, shares_ko
           FROM share_weights WHERE pool_instance = $1 ORDER BY workername, bucket_at""",
        pool_instance)


async def file_row(pg, path, pool_instance):
    return await pg.fetchrow(
        """SELECT status::text AS status, ingested_mtime, ingested_size, retry_count
           FROM file WHERE path = $1 AND pool_instance = $2""",
        str(path), pool_instance)


# --- ingestion nominale ------------------------------------------------------

async def test_a_sharelog_is_aggregated_into_share_weights(pg, round_dir, pool_instance):
    path = sharelog(round_dir, [
        line(workinfoid = 10, rig = "rig1", diff = 1000.0, sdiff = 999_999.0, createdate = 1_700_000_000),
        line(workinfoid = 10, rig = "rig1", diff = 1000.0, sdiff = 12.0, createdate = 1_700_000_005),
        line(workinfoid = 10, rig = "rig2", diff = 500.0, createdate = 1_700_000_007),
    ])

    await ingest(pg)

    rows = await weights_of(pg, pool_instance)
    assert [(row["workername"], row["diff_sum"], row["shares_ok"]) for row in rows] == [
        (worker_name(ADDRESS_A, "rig1"), 2000.0, 2),
        (worker_name(ADDRESS_A, "rig2"), 500.0, 1),
    ]
    assert rows[0]["bucket_at"] == datetime.fromtimestamp(1_700_000_000, UTC)
    assert rows[0]["username"] == ADDRESS_A

    settled = await file_row(pg, path, pool_instance)
    assert settled["status"] == "DONE"
    assert settled["ingested_size"] == path.stat().st_size
    assert settled["ingested_mtime"] == datetime.fromtimestamp(_MTIME, UTC)


async def test_rejected_shares_never_weigh(pg, round_dir, pool_instance):
    sharelog(round_dir, [
        line(workinfoid = 10, diff = 100.0, result = True),
        line(workinfoid = 10, diff = 9_999_999.0, result = False),
        line(workinfoid = 10, diff = 9_999_999.0, result = False),
    ])

    await ingest(pg)

    (row,) = await weights_of(pg, pool_instance)
    assert (row["diff_sum"], row["shares_ok"], row["shares_ko"]) == (100.0, 1, 2)


async def test_two_payout_addresses_stay_separate(pg, round_dir, pool_instance):
    sharelog(round_dir, [
        line(workinfoid = 10, address = ADDRESS_A, rig = "bitaxe01", diff = 100.0),
        line(workinfoid = 10, address = ADDRESS_B, rig = "bitaxe01", diff = 700.0),
    ])

    await ingest(pg)

    rows = await weights_of(pg, pool_instance)
    assert {(row["username"], row["diff_sum"]) for row in rows} == {
        (ADDRESS_A, 100.0), (ADDRESS_B, 700.0),
    }


async def test_the_full_pipeline_feeds_the_distribution(pg, round_dir, pool_instance):
    now = int(time.time())
    sharelog(round_dir, [
        line(workinfoid = 10, rig = "rig1", diff = 300.0, createdate = now),
        line(workinfoid = 10, rig = "rig2", diff = 100.0, createdate = now),
    ])

    await ingest(pg)

    rows = await ShareWeightDAO(pg).distribution(ADDRESS_A, pool_instance, window_days = 1)
    assert [(row["workername"], row["part"]) for row in rows] == [
        (worker_name(ADDRESS_A, "rig1"), 0.75),
        (worker_name(ADDRESS_A, "rig2"), 0.25),
    ]
    assert sum(row["part"] for row in rows) == pytest.approx(1.0)


# --- relecture conditionnelle ------------------------------------------------

@pytest.fixture
def aggregate_calls(monkeypatch):
    """Compte les appels a aggregate_sharelog.

    Verifier que les compteurs sont identiques ne prouve rien: l'upsert par
    remplacement rendrait une relecture invisible. Il faut compter les lectures.
    """
    calls: list[str] = []
    original = ingestor.aggregate_sharelog

    async def counting(path):
        calls.append(str(path))
        return await original(path)

    monkeypatch.setattr(ingestor, "aggregate_sharelog", counting)
    return calls


async def test_an_unchanged_file_is_not_read_again(pg, round_dir, pool_instance, aggregate_calls):
    sharelog(round_dir, [line(workinfoid = 10, diff = 100.0)])

    await ingest(pg)
    assert len(aggregate_calls) == 1

    await ingest_sharelogs(pg)
    await ingest_sharelogs(pg)

    assert len(aggregate_calls) == 1


async def test_a_touched_file_is_read_again_without_doubling_the_counters(
        pg, round_dir, pool_instance, aggregate_calls):
    """Idempotence de la reingestion: meme contenu relu, memes valeurs."""
    path = sharelog(round_dir, [
        line(workinfoid = 10, diff = 100.0),
        line(workinfoid = 10, diff = 100.0),
    ])
    await ingest(pg)

    set_mtime(path, _MTIME + 10)
    await ingest_sharelogs(pg)
    set_mtime(path, _MTIME + 20)
    await ingest_sharelogs(pg)

    assert len(aggregate_calls) == 3
    rows = await weights_of(pg, pool_instance)
    assert len(rows) == 1
    assert (rows[0]["diff_sum"], rows[0]["shares_ok"]) == (200.0, 2)


async def test_an_appended_file_is_read_again(pg, round_dir, pool_instance, aggregate_calls):
    path = sharelog(round_dir, [line(workinfoid = 10, diff = 100.0)])
    await ingest(pg)

    append_sharelog(path, [line(workinfoid = 10, diff = 100.0)])
    set_mtime(path, _MTIME + 10)
    await ingest_sharelogs(pg)

    assert len(aggregate_calls) == 2
    (row,) = await weights_of(pg, pool_instance)
    assert (row["diff_sum"], row["shares_ok"]) == (200.0, 2)


# --- stabilite de bucket_at --------------------------------------------------

async def test_appending_a_later_share_keeps_one_row(pg, round_dir, pool_instance):
    path = sharelog(round_dir, [line(workinfoid = 10, diff = 100.0, createdate = 1_700_000_000)])
    await ingest(pg)

    append_sharelog(path, [line(workinfoid = 10, diff = 100.0, createdate = 1_700_000_900)])
    set_mtime(path, _MTIME + 10)
    await ingest_sharelogs(pg)

    rows = await weights_of(pg, pool_instance)
    assert len(rows) == 1
    assert rows[0]["bucket_at"] == datetime.fromtimestamp(1_700_000_000, UTC)
    assert rows[0]["shares_ok"] == 2


async def test_appending_an_earlier_share_keeps_one_row(pg, round_dir, pool_instance):
    """Recul d'horloge: la share appendue precede celles deja ecrites. Avec
    min(createdate), bucket_at se deplacait, l'upsert creait une seconde ligne
    et les shares etaient comptees deux fois."""
    path = sharelog(round_dir, [line(workinfoid = 10, diff = 100.0, createdate = 1_700_000_000)])
    await ingest(pg)

    append_sharelog(path, [line(workinfoid = 10, diff = 100.0, createdate = 1_699_999_100)])
    set_mtime(path, _MTIME + 10)
    await ingest_sharelogs(pg)

    rows = await weights_of(pg, pool_instance)
    assert len(rows) == 1
    assert rows[0]["bucket_at"] == datetime.fromtimestamp(1_700_000_000, UTC)
    assert rows[0]["shares_ok"] == 2


# --- garde-fou sur la troncature ---------------------------------------------

async def test_a_shrunk_file_is_quarantined_without_being_written(pg, round_dir, pool_instance):
    """Un .sharelog ne fait que grossir. S'il a retreci, il a ete reecrit: les
    premieres lignes d'un job ont pu disparaitre et bucket_at bougerait."""
    path = sharelog(round_dir, [line(workinfoid = 10, diff = 100.0) for _ in range(5)])
    await ingest(pg)
    before = [tuple(row) for row in await weights_of(pg, pool_instance)]

    write_sharelog(path, [line(workinfoid = 11, diff = 900.0)])
    set_mtime(path, _MTIME + 10)
    await ingest_sharelogs(pg)

    settled = await file_row(pg, path, pool_instance)
    assert settled["status"] == "QUARANTINED"
    assert [tuple(row) for row in await weights_of(pg, pool_instance)] == before


async def test_a_quarantined_file_is_never_picked_up_again(
        pg, round_dir, pool_instance, aggregate_calls):
    path = sharelog(round_dir, [line(workinfoid = 10, diff = 100.0) for _ in range(5)])
    await ingest(pg)
    assert len(aggregate_calls) == 1

    write_sharelog(path, [line(workinfoid = 11, diff = 900.0)])
    set_mtime(path, _MTIME + 10)
    await ingest_sharelogs(pg)

    # Le fichier regrossit au-dela de sa taille ingeree: il reste ecarte.
    append_sharelog(path, [line(workinfoid = 11, diff = 900.0) for _ in range(20)])
    set_mtime(path, _MTIME + 20)
    await register_new_sharelogs(pg)
    await ingest_sharelogs(pg)

    assert len(aggregate_calls) == 1
    assert (await file_row(pg, path, pool_instance))["status"] == "QUARANTINED"


# --- lignes invalides --------------------------------------------------------

async def test_a_truncated_last_line_does_not_quarantine_a_live_file(pg, round_dir, pool_instance):
    path = sharelog(
        round_dir,
        [line(workinfoid = 10, diff = 100.0), line(workinfoid = 10, diff = 100.0),
         '{"workinfoid": 10, "workern'],
        final_newline = False)

    await ingest(pg)

    assert (await file_row(pg, path, pool_instance))["status"] == "DONE"
    (row,) = await weights_of(pg, pool_instance)
    assert (row["diff_sum"], row["shares_ok"]) == (200.0, 2)


async def test_the_pending_line_is_counted_once_completed(pg, round_dir, pool_instance):
    complete = line(workinfoid = 10, diff = 300.0)
    cut = len(complete) // 2
    path = sharelog(
        round_dir, [line(workinfoid = 10, diff = 100.0), complete[:cut]], final_newline = False)
    await ingest(pg)

    append_sharelog(path, [complete[cut:]])
    set_mtime(path, _MTIME + 10)
    await ingest_sharelogs(pg)

    (row,) = await weights_of(pg, pool_instance)
    assert (row["diff_sum"], row["shares_ok"]) == (400.0, 2)


async def test_too_many_unparsable_lines_quarantine_the_file(pg, round_dir, pool_instance):
    """MAX_REJECTED_RATIO = 1%: au-dela, le fichier n'est pas un sharelog
    exploitable et rien n'est insere."""
    lines = [line(workinfoid = 10, diff = 100.0) for _ in range(100)]
    lines += ["not json", "not json either", "{}"]
    path = sharelog(round_dir, lines)

    await ingest(pg)

    assert (await file_row(pg, path, pool_instance))["status"] == "QUARANTINED"
    assert await weights_of(pg, pool_instance) == []


async def test_a_few_unparsable_lines_are_tolerated(pg, round_dir, pool_instance):
    lines = [line(workinfoid = 10, diff = 100.0) for _ in range(200)]
    lines.insert(50, "not json")
    path = sharelog(round_dir, lines)

    await ingest(pg)

    assert (await file_row(pg, path, pool_instance))["status"] == "DONE"
    (row,) = await weights_of(pg, pool_instance)
    assert row["shares_ok"] == 200


# --- erreurs de lecture ------------------------------------------------------

def _write_invalid_utf8(path):
    path.write_bytes(b'{"workinfoid": 10}\n\xff\xfe\n')


async def test_an_unreadable_file_goes_back_to_pending(pg, round_dir, pool_instance):
    """UnicodeDecodeError avec retry_count < 3: le fichier est relache et
    retente au tick suivant."""
    path = round_dir / "0.sharelog"
    _write_invalid_utf8(path)
    set_mtime(path, _MTIME)

    await ingest(pg)

    settled = await file_row(pg, path, pool_instance)
    assert settled["status"] == "PENDING"
    assert settled["retry_count"] == 1


async def test_an_unreadable_file_is_quarantined_past_the_retry_budget(pg, round_dir, pool_instance):
    """Des octets invalides le resteront: au-dela de 3 tentatives, le fichier
    est ecarte au lieu d'etre relu integralement a chaque tick."""
    path = round_dir / "0.sharelog"
    _write_invalid_utf8(path)
    set_mtime(path, _MTIME)
    await register_new_sharelogs(pg)
    await pg.execute(
        "UPDATE file SET retry_count = 3 WHERE path = $1 AND pool_instance = $2",
        str(path), pool_instance)

    await ingest_sharelogs(pg)

    assert (await file_row(pg, path, pool_instance))["status"] == "QUARANTINED"


async def test_a_vanished_file_is_skipped_without_locking(pg, round_dir, pool_instance):
    path = round_dir / "0.sharelog"
    sharelog(round_dir, [line(workinfoid = 10, diff = 100.0)])
    await register_new_sharelogs(pg)
    path.unlink()

    await ingest_sharelogs(pg)

    assert (await file_row(pg, path, pool_instance))["status"] == "PENDING"


async def test_a_lease_lost_during_the_read_writes_nothing(pg, round_dir, pool_instance,
                                                           monkeypatch):
    """Une lecture plus longue que processing_timeout_seconds se fait reprendre son
    verrou par release_processing_sharelogs. Ce tick ne doit alors ni ecrire de poids, ni toucher au
    statut: celui qui detient le verrou finira le travail."""
    path = sharelog(round_dir, [line(workinfoid = 10, diff = 100.0)])
    await register_new_sharelogs(pg)
    original = ingestor.aggregate_sharelog

    async def reaped_while_reading(target):
        await pg.execute(
            "UPDATE file SET status = 'PENDING' WHERE path = $1 AND pool_instance = $2",
            str(target), pool_instance)
        return await original(target)

    monkeypatch.setattr(ingestor, "aggregate_sharelog", reaped_while_reading)

    await ingest_sharelogs(pg)

    assert await pg.fetchval(
        "SELECT count(*) FROM share_weights WHERE pool_instance = $1", pool_instance) == 0
    settled = await file_row(pg, path, pool_instance)
    assert settled["status"] == "PENDING"
    assert settled["ingested_mtime"] is None


async def test_an_unexpected_error_quarantines_past_the_retry_budget(
        pg, round_dir, pool_instance, monkeypatch):
    """Une erreur imprevue interrompt le tick ENTIER: sans ce plafond, un seul
    fichier pathologique empeche indefiniment l'ingestion de tous ceux decouverts
    apres lui."""
    path = sharelog(round_dir, [line(workinfoid = 10, diff = 100.0)])
    await register_new_sharelogs(pg)
    await pg.execute(
        "UPDATE file SET retry_count = $1 WHERE path = $2 AND pool_instance = $3",
        ingestor.MAX_READ_ATTEMPTS, str(path), pool_instance)

    async def boom(_path):
        raise RuntimeError("boom")

    monkeypatch.setattr(ingestor, "aggregate_sharelog", boom)

    with pytest.raises(RuntimeError):
        await ingest_sharelogs(pg)

    assert (await file_row(pg, path, pool_instance))["status"] == "QUARANTINED"


async def test_an_unexpected_error_releases_the_lock(pg, round_dir, pool_instance, monkeypatch):
    """Sans le release, claim() ne reprenant pas un PROCESSING, le fichier ne
    serait relu qu'au prochain demarrage."""
    path = sharelog(round_dir, [line(workinfoid = 10, diff = 100.0)])
    await register_new_sharelogs(pg)

    async def boom(_path):
        raise RuntimeError("boom")

    monkeypatch.setattr(ingestor, "aggregate_sharelog", boom)

    with pytest.raises(RuntimeError):
        await ingest_sharelogs(pg)

    settled = await file_row(pg, path, pool_instance)
    assert settled["status"] == "PENDING"
    assert settled["retry_count"] == 1


# --- best diff mensuel -------------------------------------------------------

async def best_of(pg):
    return await pg.fetch('SELECT month, "user", best_diff FROM monthly_bests ORDER BY month, "user"')


async def test_best_diff_keeps_the_highest_value_across_files_of_the_same_month(
        pg, round_dir, pool_instance):
    sharelog(round_dir, [line(workinfoid = 10, sdiff = 900.5, createdate = 1_700_000_000)], name = "a.sharelog")
    await ingest(pg)
    sharelog(round_dir, [line(workinfoid = 11, sdiff = 40.0, createdate = 1_700_000_100)], name = "b.sharelog")
    await ingest(pg)

    (row,) = await best_of(pg)
    assert (row["month"], row["user"], row["best_diff"]) == (date(2023, 11, 1), ADDRESS_A, 900.5)


async def test_best_diff_is_raised_by_a_later_better_share(pg, round_dir, pool_instance):
    sharelog(round_dir, [line(workinfoid = 10, sdiff = 40.0, createdate = 1_700_000_000)], name = "a.sharelog")
    await ingest(pg)
    sharelog(round_dir, [line(workinfoid = 11, sdiff = 70.0, createdate = 1_700_000_100)], name = "b.sharelog")
    await ingest(pg)

    (row,) = await best_of(pg)
    assert row["best_diff"] == 70.0
