"""Agregation d'un sharelog (workers.share_ingestor.aggregate_sharelog).

Pas de base: ces tests verrouillent les invariants de calcul purs.
"""

from datetime import UTC, date, datetime

import pytest

from ckpool_share_exporter.workers.share_ingestor import aggregate_sharelog
from tests.helpers import (
    ADDRESS_A, ADDRESS_B, append_sharelog, line, share, worker_name, write_sharelog,
)


@pytest.fixture
def sharelog(tmp_path):
    return tmp_path / "0000000f.sharelog"


async def test_diff_is_summed_not_sdiff(sharelog):
    """Invariant: le poids est la cible vardiff (`diff`), pas la difficulte
    reellement atteinte (`sdiff`). sdiff depasse diff d'un facteur aleatoire a
    queue lourde; le sommer fausserait tous les paiements."""
    write_sharelog(sharelog, [
        line(diff = 1000.0, sdiff = 987654.0),
        line(diff = 1000.0, sdiff = 12.5),
        line(diff = 1000.0, sdiff = 4_000_000.0),
    ])

    aggregates, read, rejected, _ = await aggregate_sharelog(sharelog)

    (aggregate,) = aggregates.values()
    assert aggregate.diff_sum == 3000.0
    assert (read, rejected) == (3, 0)


async def test_only_accepted_shares_feed_diff_sum(sharelog):
    """Invariant: une share rejetee ne represente aucun travail utile; elle
    n'incremente que shares_ko."""
    write_sharelog(sharelog, [
        line(diff = 100.0, result = True),
        line(diff = 100.0, result = True),
        line(diff = 9_999_999.0, result = False),
        line(diff = 9_999_999.0, result = False),
        line(diff = 9_999_999.0, result = False),
    ])

    aggregates, _, _, _ = await aggregate_sharelog(sharelog)

    (aggregate,) = aggregates.values()
    assert aggregate.diff_sum == 200.0
    assert aggregate.shares_ok == 2
    assert aggregate.shares_ko == 3


async def test_a_file_of_rejected_shares_only_weighs_nothing(sharelog):
    write_sharelog(sharelog, [line(diff = 500.0, result = False) for _ in range(4)])

    aggregates, _, _, _ = await aggregate_sharelog(sharelog)

    (aggregate,) = aggregates.values()
    assert aggregate.diff_sum == 0.0
    assert (aggregate.shares_ok, aggregate.shares_ko) == (0, 4)


# --- bucket_at ---------------------------------------------------------------

async def test_bucket_at_is_the_first_line_seen_not_the_minimum(sharelog):
    """Invariant: bucket_at = createdate de la PREMIERE ligne du job dans le
    fichier. Un min() se deplacerait si une share plus ancienne arrivait apres
    coup (recul d'horloge), creant une seconde ligne dans share_weights."""
    write_sharelog(sharelog, [
        line(workinfoid = 42, createdate = 2_000),
        line(workinfoid = 42, createdate = 1_000),
        line(workinfoid = 42, createdate = 3_000),
    ])

    aggregates, _, _, _ = await aggregate_sharelog(sharelog)

    (aggregate,) = aggregates.values()
    assert aggregate.bucket_at == 2_000


async def test_bucket_at_is_deterministic_across_repeated_reads(sharelog):
    write_sharelog(sharelog, [
        line(workinfoid = 42, createdate = 2_000),
        line(workinfoid = 42, createdate = 1_000),
    ])

    first, _, _, _ = await aggregate_sharelog(sharelog)
    second, _, _, _ = await aggregate_sharelog(sharelog)
    third, _, _, _ = await aggregate_sharelog(sharelog)

    buckets = {key: aggregate.bucket_at for key, aggregate in first.items()}
    assert {key: aggregate.bucket_at for key, aggregate in second.items()} == buckets
    assert {key: aggregate.bucket_at for key, aggregate in third.items()} == buckets


async def test_bucket_at_survives_an_append_of_a_later_share(sharelog):
    write_sharelog(sharelog, [line(workinfoid = 42, createdate = 2_000)])
    before, _, _, _ = await aggregate_sharelog(sharelog)

    append_sharelog(sharelog, [line(workinfoid = 42, createdate = 5_000)])
    after, _, _, _ = await aggregate_sharelog(sharelog)

    assert set(after) == set(before)
    assert next(iter(after.values())).bucket_at == 2_000


async def test_bucket_at_survives_an_append_of_an_earlier_share(sharelog):
    """Recul d'horloge NTP pendant le job: la share appendue est anterieure a
    celles deja ecrites. Avec min(createdate) la cle changeait -> ligne fantome
    et double comptage. La premiere ligne rencontree, elle, ne bouge pas."""
    write_sharelog(sharelog, [line(workinfoid = 42, createdate = 2_000)])
    before, _, _, _ = await aggregate_sharelog(sharelog)

    append_sharelog(sharelog, [line(workinfoid = 42, createdate = 1_000)])
    after, _, _, _ = await aggregate_sharelog(sharelog)

    assert set(after) == set(before)
    assert next(iter(after.values())).bucket_at == 2_000


async def test_bucket_at_is_per_job(sharelog):
    write_sharelog(sharelog, [
        line(workinfoid = 1, createdate = 1_000),
        line(workinfoid = 2, createdate = 2_000),
        line(workinfoid = 1, createdate = 3_000),
    ])

    aggregates, _, _, _ = await aggregate_sharelog(sharelog)

    workername = worker_name(ADDRESS_A, "GeeBeeCryptos")
    assert aggregates[(1, workername)].bucket_at == 1_000
    assert aggregates[(2, workername)].bucket_at == 2_000


# --- cle d'agregation --------------------------------------------------------

async def test_two_rigs_of_the_same_address_are_distinct(sharelog):
    write_sharelog(sharelog, [
        line(workinfoid = 7, rig = "bitaxe01", diff = 100.0),
        line(workinfoid = 7, rig = "bitaxe02", diff = 250.0),
    ])

    aggregates, _, _, _ = await aggregate_sharelog(sharelog)

    assert aggregates[(7, worker_name(ADDRESS_A, "bitaxe01"))].diff_sum == 100.0
    assert aggregates[(7, worker_name(ADDRESS_A, "bitaxe02"))].diff_sum == 250.0


async def test_same_rig_name_under_two_payout_addresses_stays_distinct(sharelog):
    """Invariant: l'adresse de paiement est `username`. ckpool la prefixe dans
    workername, donc deux machines homonymes sous deux adresses differentes
    restent deux cles, et chacune porte son propre username."""
    write_sharelog(sharelog, [
        line(workinfoid = 7, address = ADDRESS_A, rig = "bitaxe01", diff = 100.0),
        line(workinfoid = 7, address = ADDRESS_B, rig = "bitaxe01", diff = 250.0),
    ])

    aggregates, _, _, _ = await aggregate_sharelog(sharelog)

    assert len(aggregates) == 2
    first = aggregates[(7, worker_name(ADDRESS_A, "bitaxe01"))]
    second = aggregates[(7, worker_name(ADDRESS_B, "bitaxe01"))]
    assert (first.username, first.diff_sum) == (ADDRESS_A, 100.0)
    assert (second.username, second.diff_sum) == (ADDRESS_B, 250.0)


async def test_client_ip_is_never_part_of_the_key(sharelog):
    """`address` est l'IP du client: une machine qui change d'IP reste un seul
    worker."""
    write_sharelog(sharelog, [
        line(workinfoid = 7, rig = "bitaxe01", diff = 100.0, client_ip = "10.0.0.1"),
        line(workinfoid = 7, rig = "bitaxe01", diff = 100.0, client_ip = "192.168.1.9"),
    ])

    aggregates, _, _, _ = await aggregate_sharelog(sharelog)

    assert len(aggregates) == 1
    (aggregate,) = aggregates.values()
    assert aggregate.shares_ok == 2
    assert aggregate.diff_sum == 200.0


async def test_the_same_rig_on_two_jobs_is_two_buckets(sharelog):
    write_sharelog(sharelog, [
        line(workinfoid = 1, diff = 100.0),
        line(workinfoid = 2, diff = 100.0),
    ])

    aggregates, _, _, _ = await aggregate_sharelog(sharelog)

    assert len(aggregates) == 2


# --- robustesse de lecture ---------------------------------------------------

async def test_a_truncated_last_line_is_ignored_without_being_rejected(sharelog):
    """Une derniere ligne sans saut de ligne final est une ecriture en cours:
    elle n'est ni comptee ni traitee comme invalide, sinon le fichier vivant
    partirait en quarantaine."""
    write_sharelog(
        sharelog,
        [line(workinfoid = 1, diff = 100.0), line(workinfoid = 1, diff = 100.0), '{"workinfoid": 1, "workern'],
        final_newline = False,
    )

    aggregates, read, rejected, _ = await aggregate_sharelog(sharelog)

    assert (read, rejected) == (2, 0)
    (aggregate,) = aggregates.values()
    assert aggregate.diff_sum == 200.0


async def test_the_truncated_last_line_is_picked_up_once_completed(sharelog):
    partial = line(workinfoid = 1, diff = 300.0)
    cut = len(partial) // 2
    write_sharelog(sharelog, [line(workinfoid = 1, diff = 100.0), partial[:cut]], final_newline = False)

    append_sharelog(sharelog, [partial[cut:]])
    aggregates, read, rejected, _ = await aggregate_sharelog(sharelog)

    assert (read, rejected) == (2, 0)
    (aggregate,) = aggregates.values()
    assert aggregate.diff_sum == 400.0


async def test_unparsable_lines_are_counted_as_rejected(sharelog):
    write_sharelog(sharelog, [
        line(workinfoid = 1, diff = 100.0),
        "not json at all",
        '{"workinfoid": 1}',
        line(workinfoid = 1, diff = 100.0),
    ])

    aggregates, read, rejected, _ = await aggregate_sharelog(sharelog)

    assert (read, rejected) == (4, 2)
    (aggregate,) = aggregates.values()
    assert aggregate.diff_sum == 200.0


async def test_blank_lines_are_skipped_entirely(sharelog):
    write_sharelog(sharelog, [line(workinfoid = 1, diff = 100.0), "", "", line(workinfoid = 1, diff = 100.0)])

    _, read, rejected, _ = await aggregate_sharelog(sharelog)

    assert (read, rejected) == (2, 0)


async def test_an_empty_file_aggregates_to_nothing(sharelog):
    write_sharelog(sharelog, [])

    aggregates, read, rejected, _ = await aggregate_sharelog(sharelog)

    assert (aggregates, read, rejected) == ({}, 0, 0)


async def test_lines_spanning_several_chunks_are_reassembled(tmp_path):
    """read_lines lit par blocs: une ligne a cheval sur deux blocs ne doit ni se
    perdre ni etre coupee en deux lignes invalides."""
    sharelog = tmp_path / "chunked.sharelog"
    write_sharelog(sharelog, [line(workinfoid = 1, diff = 1.0) for _ in range(500)])

    from ckpool_share_exporter.utils import read_lines

    count = 0
    async for _ in read_lines(sharelog, chunk_size = 64):
        count += 1

    assert count == 500
    aggregates, read, rejected, _ = await aggregate_sharelog(sharelog)
    assert (read, rejected) == (500, 0)
    assert next(iter(aggregates.values())).diff_sum == 500.0


# --- best diff mensuel -------------------------------------------------------

async def test_best_diff_is_the_highest_accepted_sdiff_per_user_and_month(sharelog):
    oct_2026 = 1_790_000_000  # 2026-09-21 UTC
    write_sharelog(sharelog, [
        line(sdiff = 10.0, createdate = oct_2026),
        line(sdiff = 500.5, createdate = oct_2026 + 5),
        line(sdiff = 20.0, createdate = oct_2026 + 9),
        line(address = ADDRESS_B, sdiff = 7.0, createdate = oct_2026),
    ])

    *_, bests = await aggregate_sharelog(sharelog)

    assert bests == {
        (ADDRESS_A, date(2026, 9, 1)): 500.5,
        (ADDRESS_B, date(2026, 9, 1)): 7.0,
    }


async def test_best_diff_ignores_rejected_shares(sharelog):
    write_sharelog(sharelog, [
        line(sdiff = 10.0, result = True),
        line(sdiff = 9_999_999.0, result = False),
    ])

    *_, bests = await aggregate_sharelog(sharelog)

    assert list(bests.values()) == [10.0]


async def test_best_diff_splits_on_the_utc_month_boundary_and_keeps_the_year(sharelog):
    last_second = int(datetime(2026, 9, 30, 23, 59, 59, tzinfo = UTC).timestamp())
    write_sharelog(sharelog, [
        line(sdiff = 1.0, createdate = last_second),
        line(sdiff = 2.0, createdate = last_second + 1),
        line(sdiff = 3.0, createdate = last_second + 1 + 365 * 86400),
    ])

    *_, bests = await aggregate_sharelog(sharelog)

    assert bests == {
        (ADDRESS_A, date(2026, 9, 1)): 1.0,
        (ADDRESS_A, date(2026, 10, 1)): 2.0,
        (ADDRESS_A, date(2027, 10, 1)): 3.0,
    }
