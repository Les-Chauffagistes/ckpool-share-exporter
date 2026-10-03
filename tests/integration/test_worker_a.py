"""Decouverte et enregistrement des sharelogs (workers.file_explorer.worker_A)."""

import time

from ckpool_share_exporter.workers import file_explorer
from ckpool_share_exporter.workers.file_explorer import worker_A
from tests.helpers import line, set_mtime, write_sharelog

OLD = time.time() - 40 * 24 * 3600


async def registered(pg, pool_instance):
    return {
        row["path"]
        for row in await pg.fetch(
            "SELECT path FROM file WHERE pool_instance = $1", pool_instance)
    }


async def test_worker_a_registers_the_sharelogs_of_valid_rounds(pg, log_dir, pool_instance):
    write_sharelog(log_dir / "0000000f" / "0.sharelog", [line()])
    write_sharelog(log_dir / "0000000e" / "0.sharelog", [line()])

    await worker_A(pg)

    assert await registered(pg, pool_instance) == {
        str(log_dir / "0000000f" / "0.sharelog"),
        str(log_dir / "0000000e" / "0.sharelog"),
    }


async def test_worker_a_ignores_foreign_directories(pg, log_dir, pool_instance):
    write_sharelog(log_dir / "pool" / "0.sharelog", [line()])
    write_sharelog(log_dir / "user" / "0.sharelog", [line()])
    # Nom en majuscules distinct de 0000000f: sur un systeme de fichiers
    # insensible a la casse (APFS), "0000000F" designerait le meme repertoire.
    write_sharelog(log_dir / "ABCDEF01" / "0.sharelog", [line()])
    write_sharelog(log_dir / "0000000f" / "0.sharelog", [line()])

    await worker_A(pg)

    assert await registered(pg, pool_instance) == {str(log_dir / "0000000f" / "0.sharelog")}


async def test_worker_a_ignores_an_old_sharelog_inside_a_fresh_block(pg, log_dir, pool_instance):
    """Un bloc peut mettre des heures a tomber: le mtime du repertoire ne dit rien
    de l'age de ses sharelogs. Ce filtre est dans worker_A et non dans
    list_sharelogs, parce qu'il coute un stat et ne vaut que pour un chemin
    encore inconnu."""
    write_sharelog(log_dir / "0000000f" / "old.sharelog", [line()], mtime = OLD)
    write_sharelog(log_dir / "0000000f" / "new.sharelog", [line()])

    await worker_A(pg)

    assert await registered(pg, pool_instance) == {str(log_dir / "0000000f" / "new.sharelog")}


async def test_worker_a_records_the_block_directory(pg, log_dir, pool_instance):
    """Le bloc est ce qui permet a get_monitored de ne surveiller que les
    sharelogs encore susceptibles de grossir."""
    write_sharelog(log_dir / "0000000f" / "0.sharelog", [line()])

    await worker_A(pg)

    assert await pg.fetchval(
        "SELECT block FROM file WHERE pool_instance = $1", pool_instance) == "0000000f"


async def test_worker_a_does_not_restat_an_already_known_sharelog(pg, log_dir, pool_instance,
                                                                  monkeypatch):
    """Raison d'etre du cache: en regime normal un tick ne stat que les nouveaux
    chemins. Sans lui, chaque tick stat les milliers de sharelogs de la fenetre
    pour n'en retenir aucun."""
    known = write_sharelog(log_dir / "0000000f" / "0.sharelog", [line()])
    await worker_A(pg)

    statted: list[str] = []
    original = file_explorer.os.path.getmtime

    async def counting(path):
        statted.append(str(path))
        return await original(path)

    monkeypatch.setattr(file_explorer.os.path, "getmtime", counting)
    fresh = write_sharelog(log_dir / "0000000f" / "1.sharelog", [line()])
    await worker_A(pg)

    assert str(fresh) in statted
    assert str(known) not in statted


async def test_worker_a_ignores_rounds_outside_the_monitor_window(pg, log_dir, pool_instance):
    write_sharelog(log_dir / "0000000e" / "0.sharelog", [line()])
    set_mtime(log_dir / "0000000e", OLD)
    write_sharelog(log_dir / "0000000f" / "0.sharelog", [line()])

    await worker_A(pg)

    assert await registered(pg, pool_instance) == {str(log_dir / "0000000f" / "0.sharelog")}


async def test_worker_a_is_idempotent_across_ticks(pg, log_dir, pool_instance):
    write_sharelog(log_dir / "0000000f" / "0.sharelog", [line()])

    await worker_A(pg)
    await worker_A(pg)
    await worker_A(pg)

    assert await pg.fetchval(
        "SELECT count(*) FROM file WHERE pool_instance = $1", pool_instance) == 1


async def test_worker_a_does_not_resurrect_a_quarantined_file(pg, log_dir, pool_instance):
    path = write_sharelog(log_dir / "0000000f" / "0.sharelog", [line()])
    await worker_A(pg)
    await pg.execute(
        "UPDATE file SET status = 'QUARANTINED' WHERE path = $1 AND pool_instance = $2",
        str(path), pool_instance)

    await worker_A(pg)

    assert await pg.fetchval(
        "SELECT status::text FROM file WHERE path = $1 AND pool_instance = $2",
        str(path), pool_instance) == "QUARANTINED"


async def test_worker_a_registers_a_new_file_appearing_later(pg, log_dir, pool_instance):
    write_sharelog(log_dir / "0000000f" / "0.sharelog", [line()])
    await worker_A(pg)

    write_sharelog(log_dir / "0000000f" / "1.sharelog", [line()])
    await worker_A(pg)

    assert len(await registered(pg, pool_instance)) == 2
