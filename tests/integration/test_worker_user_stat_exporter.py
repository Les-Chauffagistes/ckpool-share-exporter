"""Export des stats ckpool par utilisateur."""

import json
from uuid import uuid4

from ckpool_share_exporter.workers.user_stat_exporter import export_user_stat


def user_stat(*, hashrate1m = "1.48T", workers = 1, shares = 2_896_444_789):
    return {
        "hashrate1m": hashrate1m,
        "hashrate5m": "1.38T",
        "hashrate1hr": "1.31T",
        "hashrate1d": "1.35T",
        "hashrate7d": "3.81T",
        "lastshare": 1791448721,
        "workers": workers,
        "shares": shares,
        "bestshare": 595286781.2753154,
        "bestever": 595286781,
        "authorised": 1790111749,
        "worker": [{
            "workername": "bc1qqp9zq4an6nyzhcspz2xfmkcf8rj0p6w94a5gyeu2a7rghxjhnqqsvymz5m.Hugo",
            "hashrate1m": "1.48T",
            "hashrate5m": "1.38T",
            "hashrate1hr": "1.31T",
            "hashrate1d": "1.35T",
            "hashrate7d": "986G",
            "lastshare": 1791448721,
            "shares": 259738674,
            "bestshare": 151169683.2161437,
            "bestever": 151169683,
        }],
    }


async def rows_of(pg, address, pool_instance):
    return await pg.fetch(
        "SELECT * FROM users WHERE address = $1 AND pool_instance = $2",
        address, pool_instance)


async def test_user_stat_is_stored_for_its_address(pg, log_dir, pool_instance):
    address = f"test-{uuid4().hex}"
    users_dir = log_dir / "users"
    users_dir.mkdir()
    (users_dir / address).write_text(json.dumps(user_stat()), encoding = "utf-8")

    await export_user_stat(pg)

    (row,) = await rows_of(pg, address, pool_instance)
    assert row["pool_instance"] == pool_instance
    assert row["hashrate1m"] == 1_480_000_000_000
    assert row["hashrate5m"] == 1_380_000_000_000
    assert row["workers"] == 1
    assert row["shares"] == 2_896_444_789
    assert row["bestshare"] == 595_286_781
    assert row["lastshare"].timestamp() == 1791448721
    assert row["authorized"].timestamp() == 1790111749


async def test_new_user_stat_replaces_the_previous_values(pg, log_dir, pool_instance):
    address = f"test-{uuid4().hex}"
    users_dir = log_dir / "users"
    users_dir.mkdir()
    stat_file = users_dir / address
    stat_file.write_text(json.dumps(user_stat()), encoding = "utf-8")
    await export_user_stat(pg)

    stat_file.write_text(
        json.dumps(user_stat(hashrate1m = "2T", workers = 3, shares = 150)),
        encoding = "utf-8",
    )
    await export_user_stat(pg)

    (row,) = await rows_of(pg, address, pool_instance)
    assert row["pool_instance"] == pool_instance
    assert row["hashrate1m"] == 2_000_000_000_000
    assert row["workers"] == 3
    assert row["shares"] == 150


async def test_a_bad_user_file_does_not_prevent_other_users_from_exporting(
        pg, log_dir, pool_instance):
    users_dir = log_dir / "users"
    users_dir.mkdir()
    address = f"test-{uuid4().hex}"
    (users_dir / "broken").write_text("{invalid json", encoding = "utf-8")
    (users_dir / address).write_text(json.dumps(user_stat()), encoding = "utf-8")

    await export_user_stat(pg)

    (row,) = await rows_of(pg, address, pool_instance)
    assert row["pool_instance"] == pool_instance
    assert row["workers"] == 1


def worker(name, **overrides):
    return {**user_stat()["worker"][0], "workername": name, **overrides}


async def workers_of(pg, address, pool_instance):
    return await pg.fetch(
        "SELECT * FROM user_workers WHERE address = $1 AND pool_instance = $2 ORDER BY workername",
        address, pool_instance)


async def test_workers_are_stored_under_their_address(pg, log_dir, pool_instance):
    address = f"test-{uuid4().hex}"
    users_dir = log_dir / "users"
    users_dir.mkdir()
    stat = user_stat(workers = 2)
    stat["worker"] = [worker(f"{address}.a"), worker(f"{address}.b", hashrate7d = "2T")]
    (users_dir / address).write_text(json.dumps(stat), encoding = "utf-8")

    await export_user_stat(pg)

    first, second = await workers_of(pg, address, pool_instance)
    assert first["workername"] == f"{address}.a"
    assert first["hashrate7d"] == 986_000_000_000
    assert first["shares"] == 259_738_674
    assert first["bestshare"] == 151_169_683
    assert first["lastshare"].timestamp() == 1791448721
    assert second["hashrate7d"] == 2_000_000_000_000


async def test_a_new_file_replaces_the_workers_values(pg, log_dir, pool_instance):
    address = f"test-{uuid4().hex}"
    users_dir = log_dir / "users"
    users_dir.mkdir()
    stat = user_stat()
    stat["worker"] = [worker(f"{address}.a")]
    (users_dir / address).write_text(json.dumps(stat), encoding = "utf-8")
    await export_user_stat(pg)

    stat["worker"] = [worker(f"{address}.a", shares = 999)]
    (users_dir / address).write_text(json.dumps(stat), encoding = "utf-8")
    await export_user_stat(pg)

    (row,) = await workers_of(pg, address, pool_instance)
    assert row["shares"] == 999


async def test_unchanged_workers_are_not_rewritten(pg, log_dir, pool_instance):
    address = f"test-{uuid4().hex}"
    users_dir = log_dir / "users"
    users_dir.mkdir()
    stat = user_stat()
    stat["worker"] = [worker(f"{address}.a")]
    (users_dir / address).write_text(json.dumps(stat), encoding = "utf-8")
    await export_user_stat(pg)
    xmin = await pg.fetchval(
        "SELECT xmin::text FROM user_workers WHERE address = $1", address)

    await export_user_stat(pg)

    assert xmin == await pg.fetchval(
        "SELECT xmin::text FROM user_workers WHERE address = $1", address)


async def test_a_duplicated_workername_does_not_fail_the_export(pg, log_dir, pool_instance):
    address = f"test-{uuid4().hex}"
    users_dir = log_dir / "users"
    users_dir.mkdir()
    stat = user_stat()
    stat["worker"] = [worker(f"{address}.a", shares = 1), worker(f"{address}.a", shares = 2)]
    (users_dir / address).write_text(json.dumps(stat), encoding = "utf-8")

    await export_user_stat(pg)

    (row,) = await workers_of(pg, address, pool_instance)
    assert row["shares"] == 2
    assert len(await rows_of(pg, address, pool_instance)) == 1
