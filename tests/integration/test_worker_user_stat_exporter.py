"""Export des stats ckpool par utilisateur."""

import json
from uuid import uuid4

from ckpool_share_exporter.workers.user_stat_exporter import export_user_stat


def user_stat(*, hashrate1m = "1.5T", workers = 2, shares = 120):
    return {
        "hashrate1m": hashrate1m,
        "hashrate5m": "1.4T",
        "hashrate1hr": "1.3T",
        "hashrate1d": "1.2T",
        "hashrate7d": "1.1T",
        "lastshare": 1791189501,
        "workers": workers,
        "shares": shares,
        "bestshare": 42,
        "authorized": 1791189400,
        "worker": [],
    }


async def rows_of(pg, address):
    return await pg.fetch("SELECT * FROM users WHERE address = $1", address)


async def test_user_stat_is_stored_for_its_address(pg, log_dir):
    address = f"test-{uuid4().hex}"
    users_dir = log_dir / "users"
    users_dir.mkdir()
    (users_dir / address).write_text(json.dumps(user_stat()), encoding = "utf-8")

    await export_user_stat(pg)

    (row,) = await rows_of(pg, address)
    assert row["hashrate1m"] == 1_500_000_000_000
    assert row["hashrate5m"] == 1_400_000_000_000
    assert row["workers"] == 2
    assert row["shares"] == 120
    assert row["bestshare"] == 42
    assert row["lastshare"].timestamp() == 1791189501
    assert row["authorized"].timestamp() == 1791189400


async def test_new_user_stat_replaces_the_previous_values(pg, log_dir):
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

    (row,) = await rows_of(pg, address)
    assert row["hashrate1m"] == 2_000_000_000_000
    assert row["workers"] == 3
    assert row["shares"] == 150


async def test_a_bad_user_file_does_not_prevent_other_users_from_exporting(pg, log_dir):
    users_dir = log_dir / "users"
    users_dir.mkdir()
    address = f"test-{uuid4().hex}"
    (users_dir / "broken").write_text("{invalid json", encoding = "utf-8")
    (users_dir / address).write_text(json.dumps(user_stat()), encoding = "utf-8")

    await export_user_stat(pg)

    (row,) = await rows_of(pg, address)
    assert row["workers"] == 2
