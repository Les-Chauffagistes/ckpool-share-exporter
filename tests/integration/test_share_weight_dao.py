"""Ecriture des poids et lecture de la repartition (dao.ShareWeightDAO)."""

import time
from datetime import UTC, datetime

import pytest

from ckpool_share_exporter.dao import FileDAO, LeaseLost, ShareWeightDAO
from ckpool_share_exporter.models import SharelogAggregate
from tests.helpers import ADDRESS_A, ADDRESS_B, worker_name

NOW = int(time.time())


@pytest.fixture
def weights(pg):
    return ShareWeightDAO(pg)


@pytest.fixture
def files(pg):
    return FileDAO(pg)


def aggregate(bucket_at = NOW, username = ADDRESS_A, diff_sum = 0.0, shares_ok = 0, shares_ko = 0):
    return SharelogAggregate(
        bucket_at = bucket_at, username = username,
        diff_sum = diff_sum, shares_ok = shares_ok, shares_ko = shares_ko)


async def rows_of(pg, pool_instance):
    return await pg.fetch(
        """SELECT bucket_at, workinfoid, username, workername, diff_sum, shares_ok, shares_ko, shares
           FROM share_weights WHERE pool_instance = $1 ORDER BY workinfoid, workername""",
        pool_instance)


async def commit(weights, aggregates, pool_instance, path = "/logs/0000000f/a.sharelog",
                 mtime = None, size = 100, *, lease = True):
    """Solde un sharelog.

    lease = True pose d'abord le verrou que commit_sharelog exige, comme ingest_sharelogs
    le fait en production (register puis claim).
    """
    if lease:
        files = FileDAO(weights.pg)
        await files.register([path], pool_instance)
        await files.claim(path, pool_instance)
    await weights.commit_sharelog(
        aggregates, path, pool_instance,
        mtime or datetime(2026, 1, 1, tzinfo = UTC), size)


# --- verrou exige au solde ---------------------------------------------------

async def test_commit_sharelog_refuses_to_settle_without_the_lease(weights, files, pg,
                                                                   pool_instance):
    path = "/logs/0000000f/a.sharelog"
    await files.register([path], pool_instance)  # PENDING: le verrou n'est pas tenu

    with pytest.raises(LeaseLost):
        await commit(weights, {(10, worker_name(ADDRESS_A, "rig1")): aggregate(diff_sum = 1.0)},
                     pool_instance, path = path, lease = False)


async def test_commit_sharelog_writes_no_weight_when_the_lease_was_lost(weights, files, pg,
                                                                        pool_instance):
    """Le point critique: les poids sont ecrits AVANT le solde, dans la meme
    transaction. Sans rollback, un agregat calcule sur un prefixe plus court
    resterait en base et, l'upsert remplacant, ecraserait celui du detenteur
    legitime du verrou."""
    path = "/logs/0000000f/a.sharelog"
    await files.register([path], pool_instance)

    with pytest.raises(LeaseLost):
        await commit(weights, {(10, worker_name(ADDRESS_A, "rig1")): aggregate(diff_sum = 500.0,
                                                                               shares_ok = 5)},
                     pool_instance, path = path, lease = False)

    assert await rows_of(pg, pool_instance) == []
    assert await pg.fetchval(
        """SELECT status::text FROM file WHERE path = $1 AND pool_instance = $2""",
        path, pool_instance) == "PENDING"


# --- cloisonnement entre instances du cluster --------------------------------

async def test_commit_sharelog_never_touches_another_pool_instance(weights, files, pg,
                                                                   pool_instance):
    """Chaque node du Swarm ecrit dans la meme base, et le meme chemin existe sur
    chacun: ni les poids ni le solde du fichier ne doivent franchir la frontiere.
    """
    path = "/logs/0000000f/a.sharelog"
    neighbour = "another-node"
    key = (10, worker_name(ADDRESS_A, "rig1"))
    for owner in (neighbour, pool_instance):
        await files.register([path], owner)
        await files.claim(path, owner)
    await commit(weights, {key: aggregate(diff_sum = 500.0, shares_ok = 5)}, neighbour,
                 path = path, size = 999)

    await commit(weights, {key: aggregate(diff_sum = 1.0, shares_ok = 1)}, pool_instance,
                 path = path, size = 1)

    (untouched,) = await rows_of(pg, neighbour)
    assert (untouched["diff_sum"], untouched["shares_ok"]) == (500.0, 5)
    assert await pg.fetchval(
        """SELECT ingested_size FROM file WHERE path = $1 AND pool_instance = $2""",
        path, neighbour) == 999


# --- ecriture ----------------------------------------------------------------

async def test_commit_writes_one_row_per_aggregate(weights, pg, pool_instance):
    await commit(weights, {
        (1, worker_name(ADDRESS_A, "rig1")): aggregate(diff_sum = 100.0, shares_ok = 2),
        (1, worker_name(ADDRESS_A, "rig2")): aggregate(diff_sum = 250.0, shares_ok = 5, shares_ko = 1),
    }, pool_instance)

    rows = await rows_of(pg, pool_instance)

    assert [(row["workername"], row["diff_sum"], row["shares_ok"], row["shares_ko"]) for row in rows] == [
        (worker_name(ADDRESS_A, "rig1"), 100.0, 2, 0),
        (worker_name(ADDRESS_A, "rig2"), 250.0, 5, 1),
    ]


async def test_shares_is_computed_by_the_database(weights, pg, pool_instance):
    await commit(weights, {
        (1, worker_name(ADDRESS_A, "rig1")): aggregate(shares_ok = 7, shares_ko = 3),
    }, pool_instance)

    (row,) = await rows_of(pg, pool_instance)

    assert row["shares"] == 10


async def test_bucket_at_is_the_job_start_in_utc(weights, pg, pool_instance):
    await commit(weights, {
        (1, worker_name(ADDRESS_A, "rig1")): aggregate(bucket_at = 1790265931),
    }, pool_instance)

    (row,) = await rows_of(pg, pool_instance)

    assert row["bucket_at"] == datetime.fromtimestamp(1790265931, UTC)


async def test_commit_marks_the_file_done_with_the_observed_stat(weights, files, pg, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)
    mtime = datetime(2026, 3, 4, 5, 6, 7, tzinfo = UTC)

    await commit(weights, {}, pool_instance, mtime = mtime, size = 987)

    row = await pg.fetchrow(
        """SELECT status::text AS status, ingested_mtime, ingested_size
           FROM file WHERE pool_instance = $1""", pool_instance)
    assert (row["status"], row["ingested_mtime"], row["ingested_size"]) == ("DONE", mtime, 987)


async def test_an_empty_aggregate_set_still_settles_the_file(weights, files, pg, pool_instance):
    await files.register(["/logs/0000000f/a.sharelog"], pool_instance)

    await commit(weights, {}, pool_instance)

    assert await pg.fetchval(
        "SELECT status::text FROM file WHERE pool_instance = $1", pool_instance) == "DONE"
    assert await rows_of(pg, pool_instance) == []


# --- semantique de remplacement ----------------------------------------------

async def test_committing_the_same_aggregates_twice_does_not_double_the_counters(
        weights, pg, pool_instance):
    """L'upsert remplace, il n'additionne pas: relire un sharelog est idempotent."""
    aggregates = {
        (1, worker_name(ADDRESS_A, "rig1")): aggregate(diff_sum = 100.0, shares_ok = 2, shares_ko = 1),
    }

    await commit(weights, aggregates, pool_instance)
    await commit(weights, aggregates, pool_instance)
    await commit(weights, aggregates, pool_instance)

    (row,) = await rows_of(pg, pool_instance)
    assert (row["diff_sum"], row["shares_ok"], row["shares_ko"]) == (100.0, 2, 1)


async def test_a_grown_file_replaces_the_previous_value(weights, pg, pool_instance):
    key = (1, worker_name(ADDRESS_A, "rig1"))

    await commit(weights, {key: aggregate(diff_sum = 100.0, shares_ok = 1)}, pool_instance)
    await commit(weights, {key: aggregate(diff_sum = 300.0, shares_ok = 3)}, pool_instance)

    (row,) = await rows_of(pg, pool_instance)
    assert (row["diff_sum"], row["shares_ok"]) == (300.0, 3)


async def test_two_pool_instances_never_collide(weights, pg, pool_instance):
    key = (1, worker_name(ADDRESS_A, "rig1"))

    await commit(weights, {key: aggregate(diff_sum = 100.0)}, pool_instance)
    await commit(weights, {key: aggregate(diff_sum = 999.0)}, "another-pool")

    (mine,) = await rows_of(pg, pool_instance)
    (other,) = await rows_of(pg, "another-pool")
    assert (mine["diff_sum"], other["diff_sum"]) == (100.0, 999.0)


async def test_a_moved_bucket_at_would_create_a_second_row(weights, pg, pool_instance):
    """Demonstration de ce que la stabilite de bucket_at protege: bucket_at fait
    partie de la PK, donc un bucket qui se deplace n'ecrase rien, il ajoute --
    et les shares sont comptees deux fois."""
    key = (1, worker_name(ADDRESS_A, "rig1"))

    await commit(weights, {key: aggregate(bucket_at = NOW, diff_sum = 100.0, shares_ok = 1)}, pool_instance)
    await commit(weights, {key: aggregate(bucket_at = NOW - 60, diff_sum = 100.0, shares_ok = 1)}, pool_instance)

    rows = await rows_of(pg, pool_instance)
    assert len(rows) == 2
    assert sum(row["shares_ok"] for row in rows) == 2


# --- repartition -------------------------------------------------------------

async def test_distribution_parts_sum_to_one(weights, pool_instance):
    await commit(weights, {
        (1, worker_name(ADDRESS_A, "rig1")): aggregate(diff_sum = 300.0, shares_ok = 3),
        (1, worker_name(ADDRESS_A, "rig2")): aggregate(diff_sum = 100.0, shares_ok = 1),
    }, pool_instance)

    rows = await weights.distribution(ADDRESS_A, pool_instance, window_days = 1)

    assert [row["workername"] for row in rows] == [
        worker_name(ADDRESS_A, "rig1"), worker_name(ADDRESS_A, "rig2"),
    ]
    assert [row["part"] for row in rows] == [0.75, 0.25]
    assert sum(row["part"] for row in rows) == pytest.approx(1.0)


async def test_distribution_is_visible_without_refreshing_the_aggregate(weights, pool_instance):
    """materialized_only = false: la part reflete les shares a la seconde pres,
    sans attendre le rafraichissement de l'agregat continu."""
    await commit(weights, {
        (1, worker_name(ADDRESS_A, "rig1")): aggregate(diff_sum = 42.0, shares_ok = 1),
    }, pool_instance)

    rows = await weights.distribution(ADDRESS_A, pool_instance, window_days = 1)

    assert [(row["diff_sum"], row["part"]) for row in rows] == [(42.0, 1.0)]


async def test_the_daily_archive_also_sees_fresh_shares(weights, pg, pool_instance):
    """Un CAgg empile ne voit que la partie MATERIALISEE de son parent; sans
    materialized_only = false sur les deux, le jour en cours serait sous-compte."""
    await commit(weights, {
        (1, worker_name(ADDRESS_A, "rig1")): aggregate(diff_sum = 42.0, shares_ok = 1),
    }, pool_instance)

    total = await pg.fetchval(
        "SELECT sum(diff_sum) FROM share_weights_daily WHERE pool_instance = $1", pool_instance)

    assert total == 42.0


async def test_distribution_sums_several_jobs_of_the_same_rig(weights, pool_instance):
    await commit(weights, {
        (1, worker_name(ADDRESS_A, "rig1")): aggregate(diff_sum = 100.0, shares_ok = 1),
        (2, worker_name(ADDRESS_A, "rig1")): aggregate(diff_sum = 300.0, shares_ok = 3),
    }, pool_instance)

    rows = await weights.distribution(ADDRESS_A, pool_instance, window_days = 1)

    assert len(rows) == 1
    assert rows[0]["diff_sum"] == 400.0
    assert rows[0]["shares_ok"] == 4


async def test_distribution_is_scoped_to_one_payout_address(weights, pool_instance):
    await commit(weights, {
        (1, worker_name(ADDRESS_A, "rig1")): aggregate(username = ADDRESS_A, diff_sum = 100.0),
        (1, worker_name(ADDRESS_B, "rig1")): aggregate(username = ADDRESS_B, diff_sum = 900.0),
    }, pool_instance)

    rows = await weights.distribution(ADDRESS_A, pool_instance, window_days = 1)

    assert [(row["workername"], row["diff_sum"], row["part"]) for row in rows] == [
        (worker_name(ADDRESS_A, "rig1"), 100.0, 1.0),
    ]


async def test_distribution_ignores_shares_outside_the_window(weights, pool_instance):
    await commit(weights, {
        (1, worker_name(ADDRESS_A, "rig1")): aggregate(
            bucket_at = NOW - 20 * 24 * 3600, diff_sum = 100.0, shares_ok = 1),
    }, pool_instance)

    assert await weights.distribution(ADDRESS_A, pool_instance, window_days = 14) == []


async def test_distribution_defaults_to_the_configured_window(weights, pool_instance):
    await commit(weights, {
        (1, worker_name(ADDRESS_A, "rig1")): aggregate(diff_sum = 100.0, shares_ok = 1),
    }, pool_instance)

    rows = await weights.distribution(ADDRESS_A, pool_instance)

    assert [row["part"] for row in rows] == [1.0]


async def test_an_unknown_address_has_no_distribution(weights, pool_instance):
    assert await weights.distribution(ADDRESS_B, pool_instance, window_days = 14) == []
